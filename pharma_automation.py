import json
import time
from collections import deque
import csv
import re
import os

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
            return
        except json.JSONDecodeError as e:
            print(f"Error: Could not decode JSON from '{filepath}'. Details: {e}")
            return

        for node_data in workflow.get("nodes", []):
            node_id = node_data["id"]
            self.nodes[node_id] = {
                "id": node_id, "type": node_data["type"],
                "label": node_data.get("label", node_data["type"]),
                "num_inputs": node_data.get("numInputs", 0),
                "num_outputs": node_data.get("numOutputs", 0),
                "source_file": node_data.get("source"),  # <-- NEW: Captures the source file
                "data": node_data
            }
            self.connections_from_source[node_id] = []
            self.connections_to_target[node_id] = []

        for conn_data in workflow.get("connections", []):
            source_id, target_id = conn_data["sourceNodeId"], conn_data["targetNodeId"]
            if source_id not in self.nodes or target_id not in self.nodes:
                continue
            conn_info = {
                "source_id": source_id, "source_output_key": conn_data["sourceOutputKey"],
                "target_id": target_id, "target_input_key": conn_data["targetInputKey"],
                "id": conn_data["id"]
            }
            self.connections_from_source[source_id].append(conn_info)
            self.connections_to_target[target_id].append(conn_info)

    def get_data_sources(self):
        """Finds all unique source filenames defined in the workflow."""
        sources = set()
        for node_id, node_data in self.nodes.items():
            if node_data.get('type') == 'Source' and node_data.get('source_file'):
                sources.add(node_data['source_file'])
        return list(sources)

    def _get_topological_order(self):
        if not self.nodes: return []
        in_degree = {node_id: len(self.connections_to_target.get(node_id, [])) for node_id in self.nodes}
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        sorted_order = []
        while queue:
            u_id = queue.popleft()
            sorted_order.append(u_id)
            for conn_info in self.connections_from_source.get(u_id, []):
                v_id = conn_info["target_id"]
                if v_id in in_degree:
                    in_degree[v_id] -= 1
                    if in_degree[v_id] == 0:
                        queue.append(v_id)
        if len(sorted_order) != len(self.nodes):
            print(f"Warning: Cycle detected or disconnected components. Processed {len(sorted_order)}/{len(self.nodes)} nodes.")
        return sorted_order

    def _cast_value(self, value, target_type_str):
        if value is None: return None
        target_type_str = target_type_str.lower()
        try:
            if target_type_str == "float": return float(value)
            if target_type_str == "int": return int(float(value))
            if target_type_str == "str": return str(value)
            if target_type_str == "bool":
                return str(value).lower() in ['true', '1', 't', 'y', 'yes']
            return value
        except (ValueError, TypeError):
            return None

    def _get_variables_from_code_line(self, code_line):
        return set(re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', code_line))

    def process_event(self, row_data, source_name_context, row_num_for_log=""):
        """Processes a single data row for a specific source context."""
        evaluated_outputs = {node_id: {} for node_id in self.nodes}
        if not self.node_order: return "ERROR_WORKFLOW_ORDER"

        for node_id in self.node_order:
            node = self.nodes.get(node_id)
            if not node: continue

            # *** NEW: Process only nodes relevant to the current data row's source ***
            if node.get('source_file') != source_name_context:
                continue

            node_specific_data = node["data"]
            node_type = node_specific_data["type"]
            node_display_label = node_specific_data.get("label", node_type)

            incoming_connections = self.connections_to_target.get(node_id, [])
            current_node_inputs_values = {
                conn["target_input_key"]: evaluated_outputs.get(conn["source_id"], {}).get(conn["source_output_key"], False)
                for conn in incoming_connections
            }
            
            # For Source nodes, the output is always True to kickstart the pipeline
            if node_type == "Source":
                evaluated_outputs[node_id]["output0"] = True
                continue

            result = False
            if node_type == "Rule":
                rule_variable_type = node_specific_data.get("variableType", "str").lower()
                code_line_to_eval = node_specific_data.get("codeLine", "False").strip()
                
                eval_scope = {}
                identifiers_in_code = self._get_variables_from_code_line(code_line_to_eval)
                for var_name in identifiers_in_code:
                    if var_name in row_data:
                        eval_scope[var_name] = self._cast_value(row_data[var_name], rule_variable_type)
                
                if not code_line_to_eval:
                    result = False
                else:
                    try:
                        restricted_globals = {"__builtins__": {"True": True, "False": False, "abs": abs, "min": min, "max": max, "round": round, "len": len}}
                        result = bool(eval(code_line_to_eval, restricted_globals, eval_scope))
                    except Exception as e:
                        print(f"    ERROR_RULE_EVAL (Row {row_num_for_log}, Rule: '{node_display_label}'): {e}")
                        result = False
                
                for i in range(node_specific_data.get("numOutputs", 0)):
                    evaluated_outputs[node_id][f"output{i}"] = result
            
            elif node_type == "AND":
                result = all(current_node_inputs_values.values()) if current_node_inputs_values else True
                evaluated_outputs[node_id]["output"] = result
            
            elif node_type == "OR":
                result = any(current_node_inputs_values.values()) if current_node_inputs_values else False
                evaluated_outputs[node_id]["output"] = result

            elif node_type == "Action":
                action_func = ACTION_IMPLEMENTATIONS.get(node_display_label)
                if action_func and current_node_inputs_values.get("input0", False):
                    action_func(row_data, True)
                    if node_display_label == "DISCARD": return "DISCARD"
                elif not action_func:
                     print(f"    Warning (Row {row_num_for_log}): No implementation for action '{node_display_label}'.")

        return "ACCEPT"

def load_csv_data(filepath):
    """Loads a CSV file into a list of dictionaries, handling simple type conversions."""
    if not os.path.exists(filepath):
        print(f"Error: CSV data file '{filepath}' not found.")
        return []
    data_list = []
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                processed_row = {}
                for key, value in row.items():
                    clean_key = key.strip()
                    if value is None or value.strip() == '':
                        processed_row[clean_key] = None
                        continue
                    clean_value = value.strip()
                    if clean_value.lower() in ['true', 'false']:
                        processed_row[clean_key] = clean_value.lower() == 'true'
                    else:
                        try:
                            processed_row[clean_key] = float(clean_value)
                        except ValueError:
                            processed_row[clean_key] = clean_value
                data_list.append(processed_row)
    except Exception as e:
        print(f"Error reading CSV file '{filepath}': {e}")
    return data_list

if __name__ == "__main__":
    workflow_file = "part2.json"
    output_csv_file = f"processed_output_combined.csv"

    engine = WorkflowEngine(workflow_file)
    if not engine.nodes:
        print(f"Workflow '{workflow_file}' failed to load or is empty. Exiting.")
        exit()

    # 1. Discover all data sources from the workflow
    source_files = engine.get_data_sources()
    if not source_files:
        print("No <Source> nodes found in the workflow. Nothing to process. Exiting.")
        exit()
    print(f"Found data sources in workflow: {source_files}")

    # 2. Load data from all discovered sources
    data_by_source = {
        source_file: load_csv_data(source_file)
        for source_file in source_files
    }

    # 3. Process each data row from each source
    all_processed_data = []
    print(f"\n=== STARTING BATCH PROCESSING USING WORKFLOW {workflow_file} ===")
    start_time = time.time()
    
    for source_file, data_rows in data_by_source.items():
        if not data_rows:
            print(f"--- Skipping '{source_file}' (no data loaded).")
            continue
        
        print(f"--- Processing {len(data_rows)} rows from '{source_file}'...")
        for i, data_row in enumerate(data_rows):
            decision = engine.process_event(data_row.copy(), source_file, row_num_for_log=i+2)
            
            output_row = data_row.copy()
            output_row['workflow_decision'] = decision
            output_row['original_source_file'] = source_file # For traceability
            all_processed_data.append(output_row)

    total_rows = len(all_processed_data)
    end_time = time.time()
    print(f"=== BATCH PROCESSING COMPLETE: {total_rows} rows processed in {end_time - start_time:.2f} seconds. ===")
    
    accepted_count = sum(1 for row in all_processed_data if row['workflow_decision'] == 'ACCEPT')
    discarded_count = sum(1 for row in all_processed_data if row['workflow_decision'] == 'DISCARD')
    error_count = total_rows - accepted_count - discarded_count
    print(f"    Accepted: {accepted_count}\n    Discarded: {discarded_count}\n    Errors: {error_count}")

    # 4. Write combined results to a single output CSV
    if all_processed_data:
        try:
            # Dynamically determine fieldnames from all processed rows
            all_fieldnames = set()
            for row in all_processed_data:
                all_fieldnames.update(row.keys())
            
            # Ensure consistent ordering
            fieldnames = sorted(list(all_fieldnames - {'workflow_decision', 'original_source_file'}))
            fieldnames += ['original_source_file', 'workflow_decision'] # Put these at the end

            with open(output_csv_file, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_processed_data)
                print(f"\nProcessed data saved to: {output_csv_file}")
        except Exception as e:
            print(f"\nError writing output CSV '{output_csv_file}': {e}")
    else:
        print("\nNo data was processed to write to output CSV.")