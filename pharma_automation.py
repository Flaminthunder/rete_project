import json
import time
from collections import deque
import csv
import re
import os
# import math # Uncomment if you add 'math' to restricted_globals for eval

# --- Action Implementations ---
ACTION_IMPLEMENTATIONS = {
    "DISCARD": lambda data_row, val: None,
    # Example: "ALERT": lambda data_row, val: print(f"ALERT! Row: {data_row}")
}

class WorkflowEngine:
    def __init__(self, workflow_json_path):
        self.nodes = {}
        self.connections_from_source = {}
        self.connections_to_target = {}
        self._load_workflow(workflow_json_path)
        self.node_order = self._get_topological_order()
        if not self.node_order and self.nodes:
            print("Warning: Topological sort failed. Processing order may be arbitrary.")
            self.node_order = list(self.nodes.keys())

    def _load_workflow(self, filepath):
        try:
            with open(filepath, 'r') as f:
                workflow = json.load(f)
        except FileNotFoundError:
            print(f"Error: Workflow file '{filepath}' not found.")
            self.nodes = {}; return
        except json.JSONDecodeError as e:
            print(f"Error: Could not decode JSON from '{filepath}'. Details: {e}")
            self.nodes = {}; return

        for node_data in workflow.get("nodes", []):
            node_id = node_data["id"]
            self.nodes[node_id] = {
                "id": node_id, "type": node_data["type"],
                "label": node_data.get("label", node_data["type"]),
                "num_inputs": node_data.get("numInputs", 0),
                "num_outputs": node_data.get("numOutputs", 0),
                "data": node_data
            }
            self.connections_from_source[node_id] = []
            self.connections_to_target[node_id] = []

        for conn_data in workflow.get("connections", []):
            source_id, target_id = conn_data["sourceNodeId"], conn_data["targetNodeId"]
            if source_id not in self.nodes or target_id not in self.nodes:
                print(f"Warning: Conn '{conn_data.get('id')}' refs non-existent node(s). Skipping.")
                continue
            conn_info = {
                "source_id": source_id, "source_output_key": conn_data["sourceOutputKey"],
                "target_id": target_id, "target_input_key": conn_data["targetInputKey"],
                "id": conn_data["id"]
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
            # print(f"Warning: Topological sort issue. Processed: {len(sorted_order)}/{len(self.nodes)}")
            for node_id in self.nodes:
                if node_id not in sorted_order: sorted_order.append(node_id)
        return sorted_order

    def _cast_value(self, value, target_type_str, column_name_for_error="<unknown column>"):
        if value is None:
            if target_type_str in ["float", "int", "bool"]: return None
            return None
        target_type_str = target_type_str.lower()
        original_value_for_error_msg = value
        try:
            if target_type_str == "float": return float(value)
            elif target_type_str == "int": return int(float(value))
            elif target_type_str == "str": return str(value)
            elif target_type_str == "bool":
                if isinstance(value, str):
                    val_lower = value.lower()
                    if val_lower == 'true': return True
                    if val_lower == 'false': return False
                    try: return bool(float(value))
                    except ValueError: return None
                return bool(value)
            else: return value
        except (ValueError, TypeError): return None

    def _get_variables_from_code_line(self, code_line):
        # This regex finds valid Python identifiers.
        # It will pick up column names, function names, True, False, etc.
        return set(re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', code_line))

    def process_event(self, row_data, row_num_for_log=""):
        evaluated_outputs = {node_id: {} for node_id in self.nodes}
        if not self.node_order: return "ERROR_WORKFLOW_ORDER"

        for node_id in self.node_order:
            node = self.nodes.get(node_id)
            if not node: continue

            node_specific_data = node["data"]
            node_type = node_specific_data["type"]
            node_display_label = node_specific_data.get("label", node_type)
            current_node_inputs_values = {}
            # Populate current_node_inputs_values from predecessors
            incoming_connections = self.connections_to_target.get(node_id, [])
            for conn in incoming_connections:
                s_id, s_key, t_key = conn["source_id"], conn["source_output_key"], conn["target_input_key"]
                input_value = evaluated_outputs.get(s_id, {}).get(s_key, False)
                current_node_inputs_values[t_key] = input_value

            result = False
            if node_type == "Rule":
                rule_variable_type = node_specific_data.get("variableType", "str").lower()
                code_line_to_eval = node_specific_data.get("codeLine", "False").strip()

                # --- Specific Debugging for a Rule Node (e.g., your color rule) ---
                # if node_id == "d0af0da32b999c94": # Replace with actual ID of the rule to debug
                #     print(f"\n--- DEBUGGING Rule '{node_display_label}' (ID: {node_id}) for Row {row_num_for_log} ---")
                #     print(f"    Raw row_data: {row_data}")
                #     print(f"    codeLine to eval: '{code_line_to_eval}'")
                #     print(f"    Declared variableType for rule: '{rule_variable_type}'")
                # --- End Debug ---

                eval_scope = {}
                # Populate scope with values from row_data that are mentioned in code_line
                # The `rule_variable_type` applies to these CSV-derived values.
                identifiers_in_code = self._get_variables_from_code_line(code_line_to_eval)

                # if node_id == "d0af0da32b999c94": print(f"    Identifiers from code: {identifiers_in_code}")

                for var_name in identifiers_in_code:
                    if var_name in row_data: # If identifier is a column in CSV
                        raw_csv_value = row_data[var_name]
                        casted_value = self._cast_value(raw_csv_value, rule_variable_type, var_name)
                        eval_scope[var_name] = casted_value
                        # if node_id == "d0af0da32b999c94": print(f"        Added to scope: {var_name} = {casted_value} (type: {type(casted_value)}) from CSV")
                    # else:
                        # If not in row_data, it might be a literal like 'blue', True, or a number.
                        # `eval` will handle these if they are valid Python.
                        # We don't need to add them to `eval_scope` explicitly from here.
                        # if node_id == "d0af0da32b999c94": print(f"        Identifier '{var_name}' not in CSV, assumed literal or builtin for eval.")
                        pass


                # if node_id == "d0af0da32b999c94": print(f"    Final eval_scope: {eval_scope}")

                if not code_line_to_eval: # Handle empty codeLine
                    result = False
                else:
                    try:
                        # Globals for eval: allow specific safe builtins.
                        # Python literals like 'blue', True, False, 0.8 are part of the expression string itself.
                        restricted_globals = {
                            "__builtins__": {
                                "True": True, "False": False, "abs": abs, "min": min, "max": max,
                                "round": round, "len": len, "str": str, "int": int, "float": float, "bool": bool,
                                # "math": math # If you import math and want to allow math functions
                            }
                        }
                        eval_result = bool(eval(code_line_to_eval, restricted_globals, eval_scope))
                        result = eval_result
                        # if node_id == "d0af0da32b999c94": print(f"    Eval result: {result}")
                    except NameError as ne:
                        print(f"    ERROR_RULE_EVAL (NameError) (Row {row_num_for_log}, Rule: '{node_display_label}', Code: '{code_line_to_eval}'). Variable not found in CSV or not a literal. Details: {ne}. Scope keys: {list(eval_scope.keys())}")
                        result = False
                    except TypeError as te:
                        print(f"    ERROR_RULE_EVAL (TypeError) (Row {row_num_for_log}, Rule: '{node_display_label}', Code: '{code_line_to_eval}'). Type mismatch. Scope: {eval_scope}. Details: {te}")
                        result = False
                    except Exception as e:
                        print(f"    ERROR_RULE_EVAL (Row {row_num_for_log}, Rule: '{node_display_label}', Code: '{code_line_to_eval}'). Scope: {eval_scope}. Details: {e}")
                        result = False
                
                for i in range(node_specific_data.get("numOutputs", 0)):
                    evaluated_outputs[node_id][f"output{i}"] = result
            
            elif node_type == "AND":
                num_gate_inputs = node_specific_data.get("numInputs", 0); result = True if num_gate_inputs == 0 else True
                if num_gate_inputs > 0:
                    for i in range(num_gate_inputs):
                        if not current_node_inputs_values.get(f"input{i}", False): result = False; break
                evaluated_outputs[node_id]["output"] = result
            
            elif node_type == "OR":
                num_gate_inputs = node_specific_data.get("numInputs", 0); result = False if num_gate_inputs == 0 else False
                if num_gate_inputs > 0:
                    for i in range(num_gate_inputs):
                        if current_node_inputs_values.get(f"input{i}", False): result = True; break
                evaluated_outputs[node_id]["output"] = result

            elif node_type == "Action":
                action_func = ACTION_IMPLEMENTATIONS.get(node_display_label) 
                if action_func:
                    if current_node_inputs_values.get("input0", False): 
                        action_func(row_data, True) 
                        if node_display_label == "DISCARD": return "DISCARD" 
                else: print(f"    Warning (Row {row_num_for_log}): No implementation for action '{node_display_label}'.")
            
        return "ACCEPT"

def load_csv_data(filepath):
    data_list = []
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile) # Assumes headers are valid Python identifiers or will be matched carefully
            if not reader.fieldnames:
                print(f"Warning: CSV file '{filepath}' is empty or has no header row.")
                return []
            
            fieldnames_from_csv = [h.strip() for h in reader.fieldnames] # Strip headers from CSV

            for i, row_dict_original_case in enumerate(reader):
                processed_row = {}
                # Ensure all headers are processed and keys are consistent (e.g. stripped)
                for header in fieldnames_from_csv:
                    # Find the key in row_dict_original_case that matches header (case-insensitively or after stripping)
                    # This step is crucial if CSV headers have inconsistent spacing/casing vs codeLine vars
                    actual_key_in_row = None
                    for r_key in row_dict_original_case.keys():
                        if r_key.strip() == header: # Match stripped header
                            actual_key_in_row = r_key
                            break
                    
                    raw_value = row_dict_original_case.get(actual_key_in_row) if actual_key_in_row else None
                    
                    if raw_value is None or raw_value.strip() == "":
                        processed_row[header] = None 
                        continue
                    
                    value = raw_value.strip()
                    if value.lower() == 'true': processed_row[header] = True
                    elif value.lower() == 'false': processed_row[header] = False
                    else:
                        try: processed_row[header] = float(value)
                        except ValueError: processed_row[header] = value 
                data_list.append(processed_row)
    except FileNotFoundError:
        print(f"Error: CSV data file '{filepath}' not found.")
    except Exception as e:
        print(f"Error reading CSV file '{filepath}': {e}")
    return data_list

if __name__ == "__main__":
    workflow_file = "part2.json" # YOUR WORKFLOW JSON
    input_csv_file = "pill_data.csv"      
    output_csv_file = f"processed_{os.path.basename(input_csv_file)}_v_final_check.csv"

    engine = WorkflowEngine(workflow_file)
    if not engine.nodes:
        print(f"Workflow '{workflow_file}' failed to load. Exiting.")
        exit()

    all_input_data = load_csv_data(input_csv_file)
    if not all_input_data:
        print(f"No data loaded from '{input_csv_file}'. Exiting.")
        exit()

    processed_data_with_output = []
    print(f"\n=== STARTING BATCH PROCESSING FROM {input_csv_file} USING WORKFLOW {workflow_file} ===")
    start_time = time.time()
    error_count = 0; accepted_count = 0; discarded_count = 0

    for i, data_row in enumerate(all_input_data):
        row_log_num = i + 2 
        # Ensure data_row keys match exactly what codeLine expects as variable names
        # If CSV headers are "Color Name" and codeLine uses "color_name", mapping is needed.
        # Current load_csv_data uses stripped headers as keys.
        decision = engine.process_event(data_row.copy(), row_log_num)
        
        output_row = data_row.copy()
        output_row['workflow_decision'] = decision 
        processed_data_with_output.append(output_row)
        
        if "ERROR_" in decision: error_count += 1
        elif decision == "DISCARD": discarded_count += 1
        elif decision == "ACCEPT": accepted_count +=1
    
    end_time = time.time()
    print(f"=== BATCH PROCESSING COMPLETE: {len(all_input_data)} rows processed in {end_time - start_time:.2f} seconds. ===")
    print(f"    Accepted: {accepted_count}\n    Discarded: {discarded_count}\n    Errors: {error_count}")

    if processed_data_with_output:
        if all_input_data: # Ensure all_input_data is not empty before accessing its first element
            # Use the keys from the first processed row for fieldnames, as they are standardized
            base_fieldnames = list(processed_data_with_output[0].keys()) if processed_data_with_output else []
            # Ensure 'workflow_decision' is last and not duplicated
            fieldnames = [fn for fn in base_fieldnames if fn != 'workflow_decision'] + ['workflow_decision']
            if not fieldnames or (len(fieldnames) == 1 and fieldnames[0] == 'workflow_decision' and not base_fieldnames) : # Handle empty base_fieldnames
                 # Fallback if base_fieldnames was empty (e.g., no data rows but an output column is desired)
                 # This case should ideally not be hit if all_input_data has items.
                 # If you want to write headers even for an empty processed list, define them explicitly.
                 print("Warning: Could not determine fieldnames for output CSV from processed data. Using minimal default.")
                 fieldnames = ['workflow_decision'] # Or a predefined list of expected original + new columns
        else:
            fieldnames = ['workflow_decision']


        try:
            with open(output_csv_file, mode='w', newline='', encoding='utf-8') as outfile:
                if not fieldnames: # Final check for fieldnames
                    print("Error: No fieldnames determined for output CSV. Cannot write header.")
                else:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(processed_data_with_output)
                    print(f"\nProcessed data saved to: {output_csv_file}")
        except Exception as e:
            print(f"Error writing output CSV '{output_csv_file}': {e}")
    else:
        print("No data was processed to write to output CSV.")