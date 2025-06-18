import json
import time
from collections import deque
import csv
import re
import os
from io import StringIO # To handle CSV data as a string if needed

# --- Action Implementations --- (Keep as is)
ACTION_IMPLEMENTATIONS = {
    "DISCARD": lambda data_row, val: None,
}

class WorkflowEngine:
    # ... (constructor, _load_workflow_from_data, _get_topological_order, _cast_value, _get_variables_from_code_line, process_event)
    # The __init__ will now take workflow_data (dict) instead of a path
    def __init__(self, workflow_data: dict): # Takes workflow data as dict
        self.nodes = {}
        self.connections_from_source = {}
        self.connections_to_target = {}
        self._load_workflow_from_data(workflow_data) # New method
        self.node_order = self._get_topological_order()
        if not self.node_order and self.nodes:
            print("Warning: Topological sort failed. Processing order may be arbitrary.")
            self.node_order = list(self.nodes.keys())

    def _load_workflow_from_data(self, workflow: dict): # Changed from filepath
        if not workflow:
            print("Error: Empty workflow data provided.")
            self.nodes = {}
            return

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

    # _get_topological_order, _cast_value, _get_variables_from_code_line, process_event
    # remain IDENTICAL to your last fully working Python script.
    # Ensure these are pasted correctly.
    def _get_topological_order(self):
        if not self.nodes: return []
        in_degree = {node_id: 0 for node_id in self.nodes}
        for node_id in self.nodes:
            in_degree[node_id] = len(self.connections_to_target.get(node_id, []))
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
                    if in_degree[v_id] == 0 and v_id not in visited_in_sort:
                        queue.append(v_id)
        if len(sorted_order) != len(self.nodes):
            for node_id in self.nodes:
                if node_id not in sorted_order: sorted_order.append(node_id)
        return sorted_order

    def _cast_value(self, value, target_type_str, column_name_for_error="<unknown column>"):
        if value is None:
            if target_type_str in ["float", "int", "bool"]: return None
            return None
        target_type_str = target_type_str.lower()
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
            incoming_connections = self.connections_to_target.get(node_id, [])
            for conn in incoming_connections:
                s_id, s_key, t_key = conn["source_id"], conn["source_output_key"], conn["target_input_key"]
                input_value = evaluated_outputs.get(s_id, {}).get(s_key, False)
                current_node_inputs_values[t_key] = input_value
            result = False
            if node_type == "Rule":
                rule_variable_type = node_specific_data.get("variableType", "str").lower()
                code_line_to_eval = node_specific_data.get("codeLine", "False").strip()
                identifiers_in_code = self._get_variables_from_code_line(code_line_to_eval)
                eval_scope = {}
                possible_to_evaluate_rule = True
                for var_name in identifiers_in_code:
                    if var_name in row_data:
                        raw_csv_value = row_data[var_name]
                        casted_value = self._cast_value(raw_csv_value, rule_variable_type, var_name)
                        if casted_value is None and raw_csv_value is not None:
                            possible_to_evaluate_rule = False; break
                        eval_scope[var_name] = casted_value
                    elif var_name.lower() == 'true': eval_scope[var_name] = True
                    elif var_name.lower() == 'false': eval_scope[var_name] = False
                    else:
                        is_literal_number = False
                        try: float(var_name); is_literal_number = True
                        except ValueError: pass
                        is_quoted_string_in_code = (var_name.startswith("'") and var_name.endswith("'")) or (var_name.startswith('"') and var_name.endswith('"'))
                        if not (is_literal_number or is_quoted_string_in_code):
                            possible_to_evaluate_rule = False; break
                if possible_to_evaluate_rule:
                    try:
                        restricted_globals = {"__builtins__": {"True": True, "False": False, "abs": abs, "min": min, "max": max, "round": round, "len": len, "str": str, "int": int, "float": float, "bool": bool}}
                        eval_result = bool(eval(code_line_to_eval, restricted_globals, eval_scope))
                        result = eval_result
                    except NameError as ne: result = False; print(f"NameError in rule '{node_display_label}': {ne}")
                    except TypeError as te: result = False; print(f"TypeError in rule '{node_display_label}': {te}")
                    except Exception as e: result = False; print(f"Exception in rule '{node_display_label}': {e}")
                else: result = False
                for i in range(node_specific_data.get("numOutputs", 0)): evaluated_outputs[node_id][f"output{i}"] = result
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
                else: print(f"Warning (Row {row_num_for_log}): No implementation for action '{node_display_label}'.")
        return "ACCEPT"

def load_csv_data_from_str(csv_string_data: str): # New: load from string
    data_list = []
    # Use StringIO to treat the string as a file
    csvfile = StringIO(csv_string_data)
    reader = csv.DictReader(csvfile)
    if not reader.fieldnames:
        print("Warning: CSV string data is empty or has no header row.")
        return []
    fieldnames_from_csv = [h.strip() for h in reader.fieldnames]
    for row_dict_original_case in reader:
        processed_row = {}
        for header in fieldnames_from_csv:
            actual_key_in_row = header # Assuming headers in string are already clean
            for r_key in row_dict_original_case.keys(): # Find matching key if casing/spacing differs
                if r_key.strip() == header:
                    actual_key_in_row = r_key
                    break
            raw_value = row_dict_original_case.get(actual_key_in_row)
            if raw_value is None or raw_value.strip() == "":
                processed_row[header] = None; continue
            value = raw_value.strip()
            if value.lower() == 'true': processed_row[header] = True
            elif value.lower() == 'false': processed_row[header] = False
            else:
                try: processed_row[header] = float(value)
                except ValueError: processed_row[header] = value
        data_list.append(processed_row)
    return data_list


def load_csv_data_from_file(filepath): # Renamed original
    # ... (Your existing load_csv_data_from_file logic remains the same) ...
    # This function should read from a filepath.
    data_list = []
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames: print(f"Warning: CSV file '{filepath}' is empty or has no header row."); return []
            fieldnames_from_csv = [h.strip() for h in reader.fieldnames]
            for row_dict_original_case in reader:
                processed_row = {}
                for header in fieldnames_from_csv:
                    actual_key_in_row = None
                    for r_key in row_dict_original_case.keys():
                        if r_key.strip() == header: actual_key_in_row = r_key; break
                    raw_value = row_dict_original_case.get(actual_key_in_row)
                    if raw_value is None or raw_value.strip() == "": processed_row[header] = None; continue
                    value = raw_value.strip()
                    if value.lower() == 'true': processed_row[header] = True
                    elif value.lower() == 'false': processed_row[header] = False
                    else:
                        try: processed_row[header] = float(value)
                        except ValueError: processed_row[header] = value 
                data_list.append(processed_row)
    except FileNotFoundError: print(f"Error: CSV data file '{filepath}' not found.")
    except Exception as e: print(f"Error reading CSV file '{filepath}': {e}")
    return data_list


# New main processing function to be called by Flask
def run_workflow_processing(workflow_data_dict: dict, input_csv_filepath: str, output_dir: str):
    engine = WorkflowEngine(workflow_data_dict) # Initialize with dict
    if not engine.nodes:
        return {"error": "Workflow failed to load from provided data.", "output_file": None, "stats": None}

    all_input_data = load_csv_data_from_file(input_csv_filepath) # Use the file loader
    if not all_input_data:
        return {"error": f"No data loaded from '{input_csv_filepath}'.", "output_file": None, "stats": None}

    processed_data_with_output = []
    print(f"\n=== PROCESSING CSV: {input_csv_filepath} ===")
    start_time = time.time()
    error_count = 0; accepted_count = 0; discarded_count = 0

    for i, data_row in enumerate(all_input_data):
        row_log_num = i + 2
        decision = engine.process_event(data_row.copy(), row_log_num)
        output_row = data_row.copy()
        output_row['workflow_decision'] = decision
        processed_data_with_output.append(output_row)
        if "ERROR_" in decision: error_count += 1
        elif decision == "DISCARD": discarded_count += 1
        elif decision == "ACCEPT": accepted_count += 1

    end_time = time.time()
    stats = {
        "total_processed": len(all_input_data),
        "time_taken": f"{end_time - start_time:.2f}s",
        "accepted": accepted_count,
        "discarded": discarded_count,
        "errors": error_count
    }
    print(f"=== PROCESSING COMPLETE: Stats: {stats} ===")

    if processed_data_with_output:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        # Create a unique output filename
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        base_csv_name = os.path.basename(input_csv_filepath)
        output_filename = f"processed_{timestamp}_{base_csv_name}"
        output_filepath = os.path.join(output_dir, output_filename)

        if all_input_data:
            base_fieldnames = list(processed_data_with_output[0].keys()) if processed_data_with_output else []
            fieldnames = [fn for fn in base_fieldnames if fn != 'workflow_decision'] + ['workflow_decision']
            if not fieldnames or (len(fieldnames) == 1 and fieldnames[0] == 'workflow_decision' and not base_fieldnames):
                 fieldnames = ['workflow_decision'] # Fallback
        else:
            fieldnames = ['workflow_decision']

        try:
            with open(output_filepath, mode='w', newline='', encoding='utf-8') as outfile:
                if not fieldnames:
                    return {"error": "No fieldnames for output CSV.", "output_file": None, "stats": stats}
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(processed_data_with_output)
            print(f"\nProcessed data saved to: {output_filepath}")
            return {"error": None, "output_file": output_filename, "stats": stats} # Return filename, not full path
        except Exception as e:
            return {"error": f"Error writing output CSV: {e}", "output_file": None, "stats": stats}
    else:
        return {"error": "No data processed to write.", "output_file": None, "stats": stats}

# Keep the __main__ block for standalone testing if you want
if __name__ == "__main__":
    default_workflow_path = "p4.json"
    default_csv_path = "pill_data.csv"
    default_output_dir = "processed_output" # From script's location

    # Load workflow from file for standalone test
    try:
        with open(default_workflow_path, 'r') as f:
            workflow_dict_for_test = json.load(f)
    except Exception as e:
        print(f"Error loading workflow {default_workflow_path} for standalone test: {e}")
        exit()

    results = run_workflow_processing(workflow_dict_for_test, default_csv_path, default_output_dir)
    if results["error"]:
        print(f"Standalone Test Error: {results['error']}")
    else:
        print(f"Standalone Test Success. Output: {results['output_file']}, Stats: {results['stats']}")