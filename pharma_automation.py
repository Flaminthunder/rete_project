import json
import time
from collections import deque
import csv
import re # For parsing rule labels
import os # For path manipulation

# --- Action Implementations ---
ACTION_IMPLEMENTATIONS = {
    "DISCARD": lambda data_row, val: None,
}

SPECIAL_NON_ENTRY_RULES = {
     "Defect Confirmed": lambda input_boolean: bool(input_boolean)
}

class WorkflowEngine:
    def __init__(self, workflow_json_path):
        self.nodes = {}
        self.connections_from_source = {}
        self.connections_to_target = {}
        self._load_workflow(workflow_json_path)
        self.node_order = self._get_topological_order()

    def _load_workflow(self, filepath):
        try:
            with open(filepath, 'r') as f:
                workflow = json.load(f)
        except FileNotFoundError:
            print(f"Error: Workflow file '{filepath}' not found.")
            self.nodes = {}
            return
        except json.JSONDecodeError as e:
            print(f"Error: Could not decode JSON from workflow file '{filepath}'. Details: {e}")
            self.nodes = {}
            return

        for node_data in workflow.get("nodes", []):
            node_id = node_data["id"]
            self.nodes[node_id] = {
                "id": node_id, "type": node_data["type"], "label": node_data.get("label", node_data["type"]),
                "num_inputs": node_data.get("numInputs", 0), "num_outputs": node_data.get("numOutputs", 0),
                "data": node_data
            }
            self.connections_from_source[node_id] = []
            self.connections_to_target[node_id] = []

        for conn_data in workflow.get("connections", []):
            source_id, target_id = conn_data["sourceNodeId"], conn_data["targetNodeId"]
            if source_id not in self.nodes or target_id not in self.nodes: continue
            conn_info = {
                "source_id": source_id, "source_output_key": conn_data["sourceOutputKey"],
                "target_id": target_id, "target_input_key": conn_data["targetInputKey"], "id": conn_data["id"]
            }
            self.connections_from_source[source_id].append(conn_info)
            self.connections_to_target[target_id].append(conn_info)

    def _get_topological_order(self):
        if not self.nodes: return []
        in_degree = {node_id: len(self.connections_to_target.get(node_id, [])) for node_id in self.nodes}
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        sorted_order, visited_in_sort = [], set()
        while queue:
            u_id = queue.popleft()
            if u_id in visited_in_sort: continue
            visited_in_sort.add(u_id); sorted_order.append(u_id)
            for conn_info in self.connections_from_source.get(u_id, []):
                v_id = conn_info["target_id"]
                if v_id in in_degree:
                    in_degree[v_id] -= 1
                    if in_degree[v_id] == 0 and v_id not in visited_in_sort: queue.append(v_id)
        if len(sorted_order) != len(self.nodes):
            for node_id in self.nodes:
                if node_id not in sorted_order: sorted_order.append(node_id)
        return sorted_order

    def parse_rule_label(self, rule_label):
        match = re.match(
            r"^\s*([a-zA-Z_][\w_]*)\s*([!=<>]=?|[\<\>])\s*(True|False|[\d\.]+(?:[eE][+-]?\d+)?|'(?:[^']*)'|\"(?:[^\"]*)\")\s*$",
            rule_label
        )
        if not match: return None, None, None, f"Invalid rule format: {rule_label}"
        column_name, operator, value_str = match.group(1), match.group(2), match.group(3)
        
        parsed_value = None
        if value_str.lower() == 'true': parsed_value = True
        elif value_str.lower() == 'false': parsed_value = False
        elif (value_str.startswith("'") and value_str.endswith("'")) or \
             (value_str.startswith('"') and value_str.endswith('"')):
            parsed_value = value_str[1:-1] # Store as string
        else:
            try: # Attempt to parse as float first
                parsed_value = float(value_str)
            except ValueError:
                # If not float, and not bool/quoted string, it's an unquoted string or malformed number
                # This case should ideally not happen if rule values are always bool, number, or quoted string
                return None, None, None, f"Unrecognized rule value format: {value_str} in {rule_label}"
        return column_name, operator, parsed_value, None


    def evaluate_condition(self, actual_value, operator, rule_value, rule_label_for_error=""):
        # Try to make types compatible for comparison
        # print(f"DEBUG: Pre-coercion: actual='{actual_value}' ({type(actual_value)}), op='{operator}', rule_val='{rule_value}' ({type(rule_value)})")
        
        a_val, r_val = actual_value, rule_value

        if type(a_val) != type(r_val):
            # Attempt coercion, preferring the type of the rule_value if it's not string,
            # or if actual_value is string and rule_value is specific (bool/float).
            if isinstance(r_val, bool) and isinstance(a_val, str):
                if a_val.lower() == 'true': a_val = True
                elif a_val.lower() == 'false': a_val = False
                else: return False # String actual_value cannot be coerced to bool for comparison
            elif isinstance(r_val, (int, float)) and isinstance(a_val, str):
                try: a_val = float(a_val)
                except (ValueError, TypeError): return False # String actual_value cannot be coerced to float
            elif isinstance(a_val, (int, float)) and isinstance(r_val, str): # if rule value is a string number like '0.8'
                try: r_val = float(r_val)
                except (ValueError, TypeError): pass # keep r_val as string if not convertible, comparison will likely fail
            elif isinstance(a_val, bool) and isinstance(r_val, str): # if rule value is 'True' or 'False'
                if r_val.lower() == 'true': r_val = True
                elif r_val.lower() == 'false': r_val = False
                else: pass # keep r_val as string if not convertible

        # print(f"DEBUG: Post-coercion: actual='{a_val}' ({type(a_val)}), op='{operator}', rule_val='{r_val}' ({type(r_val)})")

        try:
            if isinstance(a_val, bool) and isinstance(r_val, bool):
                if operator == "==": return a_val == r_val
                if operator == "!=": return a_val != r_val
            elif isinstance(a_val, str) and isinstance(r_val, str):
                # Case-insensitive comparison for strings
                if operator == "==": return a_val.lower() == r_val.lower()
                if operator == "!=": return a_val.lower() != r_val.lower()
                # Other operators like >, < are not standard for strings in this context
            elif isinstance(a_val, (int, float)) and isinstance(r_val, (int, float)):
                if operator == "==": return a_val == r_val
                if operator == "!=": return a_val != r_val
                if operator == ">":  return a_val > r_val
                if operator == "<":  return a_val < r_val
                if operator == ">=": return a_val >= r_val
                if operator == "<=": return a_val <= r_val
            
            # If types are still different after attempted coercion, it's a mismatch
            # print(f"Warning: Type mismatch or unhandled operator for rule '{rule_label_for_error}'. Actual: {type(a_val)}, Rule: {type(r_val)}, Op: {operator}")
            return False
        except TypeError as e:
            # print(f"Warning: TypeError during condition evaluation for rule '{rule_label_for_error}': {e}.")
            return False

    def process_event(self, row_data, row_num_for_log=""):
        evaluated_outputs = {node_id: {} for node_id in self.nodes}
        discard_action_triggered = False

        if not self.node_order: return "ERROR_WORKFLOW_ORDER"

        for node_id in self.node_order:
            node = self.nodes.get(node_id)
            if not node: continue
            node_type, node_label = node["type"], node["label"]
            current_node_inputs_values = {}
            incoming_connections = self.connections_to_target.get(node_id, [])
            for conn in incoming_connections:
                s_id, s_key, t_key = conn["source_id"], conn["source_output_key"], conn["target_input_key"]
                current_node_inputs_values[t_key] = evaluated_outputs.get(s_id, {}).get(s_key, False)

            result = False
            if node_type == "Rule":
                is_entry_rule = not incoming_connections or node.get("num_inputs", 0) == 0
                if is_entry_rule:
                    column_name, operator, rule_val, parse_error = self.parse_rule_label(node_label)
                    if parse_error:
                        error_msg = f"ERROR_RULE_PARSE (Row {row_num_for_log}, Rule: '{node_label}', Reason: {parse_error})"
                        # print(error_msg) # Optional: print to console
                        return error_msg # Stop processing this row if rule is fundamentally unparsable
                    
                    if column_name not in row_data:
                        error_msg = f"ERROR_MISSING_COLUMN (Row {row_num_for_log}, Rule: '{node_label}', Column: '{column_name}')"
                        # print(error_msg)
                        return error_msg # Stop processing this row
                    
                    actual_val = row_data.get(column_name)
                    result = self.evaluate_condition(actual_val, operator, rule_val, node_label)

                elif node_label in SPECIAL_NON_ENTRY_RULES:
                    input_val = current_node_inputs_values.get('input0', False)
                    result = SPECIAL_NON_ENTRY_RULES[node_label](input_val)
                
                for i in range(node.get("num_outputs", 0)): evaluated_outputs[node_id][f"output{i}"] = result

            elif node_type == "AND":
                num_inputs = node.get("num_inputs", 0)
                if num_inputs == 0: result = True
                else:
                    result = True
                    for i in range(num_inputs):
                        if not current_node_inputs_values.get(f"input{i}", False): result = False; break
                evaluated_outputs[node_id]["output"] = result
            
            elif node_type == "OR":
                num_inputs = node.get("num_inputs", 0)
                if num_inputs == 0: result = False
                else:
                    result = False
                    for i in range(num_inputs):
                        if current_node_inputs_values.get(f"input{i}", False): result = True; break
                evaluated_outputs[node_id]["output"] = result

            elif node_type == "Action":
                action_func = ACTION_IMPLEMENTATIONS.get(node_label)
                if action_func:
                    trigger_value = current_node_inputs_values.get("input0", False)
                    if trigger_value:
                        action_func(row_data, trigger_value)
                        if node_label == "DISCARD": discard_action_triggered = True
        
        return "DISCARD" if discard_action_triggered else "ACCEPT"

def load_csv_data(filepath):
    data_list = []
    expected_headers = None # To check consistency
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8-sig') as csvfile: # utf-8-sig handles BOM
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames:
                print(f"Warning: CSV file '{filepath}' is empty or has no header row.")
                return []
            expected_headers = [h.strip() for h in reader.fieldnames] # Store cleaned headers

            for i, row_dict in enumerate(reader):
                processed_row = {}
                # Ensure all expected headers are present, even if value is empty
                for header in expected_headers:
                    raw_value = row_dict.get(header) # Get value using original (potentially unstripped) header from DictReader
                    
                    # Intelligent type conversion for the value
                    if raw_value is None or raw_value.strip() == "": # Handle None or empty strings
                        processed_row[header] = None # Represent empty/missing as None
                        continue

                    value = raw_value.strip() # Use stripped value for conversions

                    if value.lower() == 'true': processed_row[header] = True
                    elif value.lower() == 'false': processed_row[header] = False
                    else:
                        try: # Attempt float conversion
                            processed_row[header] = float(value)
                        except ValueError: # If not float, keep as string
                            processed_row[header] = value 
                data_list.append(processed_row)
    except FileNotFoundError:
        print(f"Error: CSV data file '{filepath}' not found.")
    except Exception as e:
        print(f"Error reading CSV file '{filepath}': {e}")
    return data_list

if __name__ == "__main__":
    workflow_file = "p4.json" 
    input_csv_file = "pill_data.csv"      
    output_csv_file = f"processed_{os.path.basename(input_csv_file)}_4"

    engine = WorkflowEngine(workflow_file)
    if not engine.nodes:
        print(f"Primary workflow '{workflow_file}' failed to load. Check workflow file and console errors.")
        # Attempt DUMMY workflow if primary fails (optional, remove if not desired)
        dummy_workflow_file = "pharma_workflow_DUMMY.json"
        print(f"Attempting to create and use a DUMMY workflow: {dummy_workflow_file}")
        dummy_workflow_content = { # ... (dummy content from previous example) ... 
            "nodes": [
                {"id": "r1", "label": "is_cracked == True", "type": "Rule", "numOutputs": 1},
                {"id": "r2", "label": "weight > 0.9", "type": "Rule", "numOutputs": 1},
                {"id": "r3", "label": "color == 'red'", "type": "Rule", "numOutputs": 1},
                {"id": "r4", "label": "missing_column == True", "type": "Rule", "numOutputs": 1}, # To test error
                {"id": "lg1", "label": "OR", "type": "OR", "numInputs": 3},
                {"id": "act1", "label": "DISCARD", "type": "Action", "numInputs": 1}
            ],
            "connections": [
                {"id": "c1", "sourceNodeId": "r1", "sourceOutputKey": "output0", "targetNodeId": "lg1", "targetInputKey": "input0"},
                {"id": "c2", "sourceNodeId": "r2", "sourceOutputKey": "output0", "targetNodeId": "lg1", "targetInputKey": "input1"},
                {"id": "c3", "sourceNodeId": "r3", "sourceOutputKey": "output0", "targetNodeId": "lg1", "targetInputKey": "input2"},
                # Rule r4 (missing_column) is not connected to demonstrate separate error handling if needed
                {"id": "c4", "sourceNodeId": "lg1", "sourceOutputKey": "output", "targetNodeId": "act1", "targetInputKey": "input0"}
            ]
        }
        try:
            with open(dummy_workflow_file, 'w') as f: json.dump(dummy_workflow_content, f, indent=2)
            engine = WorkflowEngine(dummy_workflow_file) # Re-initialize with dummy
            if not engine.nodes:
                 print(f"FATAL: Dummy workflow '{dummy_workflow_file}' also failed to load. Exiting.")
                 exit()
            print(f"Successfully loaded DUMMY workflow: {dummy_workflow_file}")
            workflow_file = dummy_workflow_file # Update workflow_file to reflect dummy is used
        except Exception as e_dummy:
            print(f"FATAL: Could not create/load DUMMY workflow. Error: {e_dummy}. Exiting.")
            exit()


    all_input_data = load_csv_data(input_csv_file)
    if not all_input_data:
        print(f"No data loaded from '{input_csv_file}'. Cannot proceed.")
        exit()

    processed_data_with_output = []
    print(f"\n=== STARTING BATCH PROCESSING FROM {input_csv_file} USING WORKFLOW {workflow_file} ===")
    start_time = time.time()
    error_count = 0

    for i, data_row in enumerate(all_input_data):
        row_log_num = i + 2 # CSV row number (1-based header + 1-based data)
        decision = engine.process_event(data_row.copy(), row_log_num) 
        
        output_row = data_row.copy()
        output_row['output'] = decision 
        processed_data_with_output.append(output_row)
        
        if "ERROR_" in decision:
            error_count += 1
            print(f"  Pill {data_row.get('pill_id', 'N/A')} (Row {row_log_num}): {decision}") # Print errors immediately

    end_time = time.time()
    print(f"=== BATCH PROCESSING COMPLETE: {len(all_input_data)} pills processed in {end_time - start_time:.2f} seconds. Errors: {error_count} ===")

    if processed_data_with_output:
        if all_input_data:
            # Use headers from the first row of loaded (and potentially type-converted) data
            # This ensures headers match the keys used in processed_data_with_output
            base_fieldnames = list(all_input_data[0].keys())
            fieldnames = [fn for fn in base_fieldnames if fn != 'output'] + ['output'] # ensure 'output' is last
        else: # Should not happen
            fieldnames = ['output']

        try:
            with open(output_csv_file, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(processed_data_with_output)
            print(f"\nProcessed data saved to: {output_csv_file}")
        except Exception as e:
            print(f"Error writing output CSV '{output_csv_file}': {e}")
    else:
        print("No data was processed to write to output CSV.")