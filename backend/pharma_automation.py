import json
import time
from collections import deque
import csv
import re
import os
import logging

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Action Implementations ---
ACTION_IMPLEMENTATIONS = {
    "DISCARD": lambda data_row, val: None,
}

class WorkflowEngine:
    def __init__(self, workflow_data: dict):
        self.nodes = {}
        self.connections_from_source = {}
        self.connections_to_target = {}
        self.default_action = workflow_data.get('defaultAction', 'ACCEPT')
        self._load_workflow_from_data(workflow_data)
        try:
            self.node_order = self._get_topological_order()
        except ValueError as e: # Catch cycle error from topological sort
            logging.error(f"Error initializing WorkflowEngine: {e}")
            raise # Re-raise the error to be handled by the caller

    def _load_workflow_from_data(self, workflow: dict): # (No change needed here from your version)
        if not workflow or "nodes" not in workflow:
            logging.error("Empty or invalid workflow data provided.")
            self.nodes = {} # Ensure nodes is initialized even on error
            return

        for node_data in workflow.get("nodes", []):
            node_id = node_data["id"]
            self.nodes[node_id] = node_data # Store the whole node_data dictionary
            self.connections_from_source[node_id] = []
            self.connections_to_target[node_id] = []

        for conn_data in workflow.get("connections", []):
            source_id, target_id = conn_data["sourceNodeId"], conn_data["targetNodeId"]
            if source_id not in self.nodes or target_id not in self.nodes:
                logging.warning(f"Connection '{conn_data.get('id')}' refs non-existent node(s). Skipping.")
                continue
            self.connections_from_source[source_id].append(conn_data)
            self.connections_to_target[target_id].append(conn_data)

    def _get_topological_order(self): # (No change needed here from your version)
        if not self.nodes: return []
        in_degree = {node_id: len(self.connections_to_target.get(node_id, [])) for node_id in self.nodes}
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        sorted_order = []
        while queue:
            u_id = queue.popleft()
            sorted_order.append(u_id)
            for conn_info in self.connections_from_source.get(u_id, []):
                v_id = conn_info["targetNodeId"]
                in_degree[v_id] -= 1
                if in_degree[v_id] == 0: queue.append(v_id)
        if len(sorted_order) != len(self.nodes):
            cycle_nodes = set(self.nodes.keys()) - set(sorted_order)
            error_msg = f"Workflow has a cycle. Nodes involved/unreached: {cycle_nodes}"
            logging.error(error_msg)
            raise ValueError(error_msg)
        return sorted_order


    def _cast_value(self, value, target_type_str, column_name_for_error="<unknown column>"): # (No change needed)
        if value is None: return None
        target_type_str = target_type_str.lower()
        try:
            if target_type_str == "float": return float(value)
            if target_type_str == "int": return int(float(value))
            if target_type_str == "string": return str(value) # Changed from "str"
            if target_type_str == "bool":
                if isinstance(value, str): return value.lower() in ['true', '1', 't', 'y', 'yes']
                return bool(value)
            return value
        except (ValueError, TypeError): return None

    def _get_variables_from_code_line(self, code_line): # (No change needed)
        return set(re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', code_line))

    def process_event(self, row_data: dict, row_num_for_log="N/A"): # (No change needed in signature or core logic)
        evaluated_outputs = {node_id: {} for node_id in self.nodes}
        if not self.node_order: return "ERROR_WORKFLOW_ORDER"

        for node_id in self.node_order:
            node = self.nodes.get(node_id) # node here is the dictionary from self.nodes
            if not node: continue

            # node_specific_data is node itself in this structure
            node_type = node["type"]
            node_display_label = node.get("label", node_type)
            current_node_inputs = {}
            for conn in self.connections_to_target.get(node_id, []):
                source_id, source_key = conn["sourceNodeId"], conn["sourceOutputKey"]
                target_key = conn["targetInputKey"]
                input_value = evaluated_outputs.get(source_id, {}).get(source_key, False)
                current_node_inputs[target_key] = input_value

            if node_type == "Source": # Source nodes simply propagate a 'True' signal for now
                evaluated_outputs[node_id]['output0'] = True
            
            elif node_type == "Rule":
                code_line = node.get("codeLine", "False")
                var_type = node.get("variableType", "string") # Corrected: was "string"
                
                eval_scope = {}
                possible_to_eval = True
                for var_name in self._get_variables_from_code_line(code_line):
                    if var_name in row_data:
                        casted_val = self._cast_value(row_data[var_name], var_type)
                        if casted_val is None and row_data[var_name] is not None:
                            possible_to_eval = False; break
                        eval_scope[var_name] = casted_val
                
                result = False
                if possible_to_eval and code_line: # Check if code_line is not empty
                    try:
                        restricted_globals = {"__builtins__": {"True": True, "False": False, "len": len}}
                        result = bool(eval(code_line, restricted_globals, eval_scope))
                    except Exception as e:
                        logging.warning(f"Row {row_num_for_log}: Error in rule '{node_display_label}' (Code: '{code_line}'): {e}. Scope: {eval_scope}. Defaulting to False.")
                        result = False
                else:
                    result = False # If not possible to eval or empty code_line

                evaluated_outputs[node_id]['outputTrue'] = result
                evaluated_outputs[node_id]['outputFalse'] = not result

            elif node_type in ["AND", "OR"]:
                # Ensure current_node_inputs.values() are boolean
                input_values = [bool(v) for v in current_node_inputs.values()]
                if node_type == "AND":
                    result = all(input_values) if input_values else True # Empty AND is true
                else: # OR
                    result = any(input_values) if input_values else False # Empty OR is false
                evaluated_outputs[node_id]['output'] = result


            elif node_type == "Action":
                # An action is triggered if ANY of its inputs are true.
                # For single input action node, this simplifies to checking that one input.
                action_triggered = any(bool(v) for v in current_node_inputs.values())
                if action_triggered:
                    action_label = node.get("label", "UnknownAction") # Action is identified by its label
                    action_func = ACTION_IMPLEMENTATIONS.get(action_label)
                    if action_func:
                        action_func(row_data, True) # Pass True as val, can be ignored by lambda
                        if action_label == "DISCARD":
                            return "DISCARD"
                    else:
                        logging.warning(f"Row {row_num_for_log}: No implementation for action '{action_label}'.")
        
        return self.default_action


def load_csv_data_from_file(filepath): # (No major change, ensure it handles various empty/whitespace cases)
    data_list = []
    if not os.path.exists(filepath): # Add check for file existence
        logging.error(f"CSV data file '{filepath}' not found.")
        return None
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames:
                logging.warning(f"CSV file '{filepath}' is empty or has no header row.")
                return []
            
            stripped_fieldnames = [h.strip() for h in reader.fieldnames] # Store stripped fieldnames
            
            for row_dict_original_case in reader:
                processed_row = {}
                # Use original fieldnames for reading, then map to stripped fieldnames for consistency
                for i, original_header in enumerate(reader.fieldnames): # reader.fieldnames has original case
                    stripped_header = stripped_fieldnames[i]
                    raw_value = row_dict_original_case.get(original_header) # Use original key to get value
                    
                    if raw_value is None or raw_value.strip() == "":
                        processed_row[stripped_header] = None 
                        continue
                    
                    value = raw_value.strip()
                    if value.lower() == 'true': processed_row[stripped_header] = True
                    elif value.lower() == 'false': processed_row[stripped_header] = False
                    else:
                        try: processed_row[stripped_header] = float(value)
                        except ValueError: processed_row[stripped_header] = value 
                data_list.append(processed_row)
    except Exception as e:
        logging.error(f"Error reading CSV file '{filepath}': {e}")
        return None # Return None on error
    return data_list

# --- Main processing function to be called by Flask ---
def run_workflow_processing(workflow_data_dict: dict, base_data_dir: str, output_dir: str):
    # 1. Identify the source CSV file(s) from the workflow
    source_nodes_data = [node for node in workflow_data_dict.get("nodes", []) if node.get("type") == "Source"]

    if not source_nodes_data:
        return {"error": "No 'Source' node found in the workflow to specify input CSV.", "output_file": None, "stats": None}
    
    # For this version, assume one primary Source node or use the first one.
    # A more complex system could handle multiple sources or allow user to specify.
    primary_source_node = source_nodes_data[0]
    csv_filename_from_node = primary_source_node.get("source") # This is "pill_data.csv" or "other.csv"

    if not csv_filename_from_node:
        return {"error": f"Source node '{primary_source_node.get('label')}' does not specify a source file.", "output_file": None, "stats": None}

    # Construct the full path to the CSV file
    # IMPORTANT: Assume csv_filename_from_node is just the filename, not a full path.
    # And base_data_dir is where all data CSVs are stored (e.g., your backend/ folder)
    input_csv_filepath = os.path.join(base_data_dir, csv_filename_from_node)
    logging.info(f"Determined input CSV filepath: {input_csv_filepath}")


    # 2. Initialize WorkflowEngine (this can now raise ValueError if cycle)
    try:
        engine = WorkflowEngine(workflow_data_dict)
    except ValueError as e: # Cycle detected
        return {"error": str(e), "output_file": None, "stats": None}
    
    if not engine.nodes: # Should be caught by constructor but double check
        return {"error": "Workflow engine failed to load nodes from provided data.", "output_file": None, "stats": None}

    # 3. Load the specified CSV data
    all_input_data = load_csv_data_from_file(input_csv_filepath)
    if all_input_data is None: # load_csv_data_from_file returns None on file not found or read error
        return {"error": f"Failed to load data from '{input_csv_filepath}'. Check logs.", "output_file": None, "stats": None}
    if not all_input_data: # Empty list if CSV was empty but readable
        return {"error": f"No data rows found in '{input_csv_filepath}'.", "output_file": None, "stats": None}

    # 4. Process data (rest of the function is similar to before)
    processed_data = []
    logging.info(f"=== PROCESSING CSV: {input_csv_filepath} using workflow ===")
    start_time = time.time()
    decision_counts = {} # Using a dict for flexible decision categories

    for i, data_row in enumerate(all_input_data):
        decision = engine.process_event(data_row.copy(), row_num_for_log=i + 2)
        decision_counts[decision] = decision_counts.get(decision, 0) + 1
        
        output_row = data_row.copy()
        output_row['workflow_decision'] = decision
        processed_data.append(output_row)

    stats = {
        "total_processed": len(all_input_data),
        "time_taken": f"{time.time() - start_time:.2f}s",
        "decisions": decision_counts # This provides a breakdown
    }
    logging.info(f"=== PROCESSING COMPLETE: Stats: {stats} ===")

    # 5. Save output
    if processed_data:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        output_filename = f"processed_{timestamp}_{os.path.basename(csv_filename_from_node)}" # Use actual CSV name
        output_filepath = os.path.join(output_dir, output_filename)
        
        # Determine fieldnames from the first row of processed_data (which includes original + decision)
        if processed_data:
            output_fieldnames = list(processed_data[0].keys())
        else: # Should not happen if all_input_data had items
            output_fieldnames = ['workflow_decision']


        try:
            with open(output_filepath, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=output_fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(processed_data)
            logging.info(f"Processed data saved to: {output_filepath}")
            return {"error": None, "output_file": output_filename, "stats": stats}
        except Exception as e:
            error_msg = f"Error writing output CSV: {e}"
            logging.error(error_msg)
            return {"error": error_msg, "output_file": None, "stats": stats}
    else:
        return {"error": "No data was processed to write to output.", "output_file": None, "stats": stats}


# --- Main block for standalone testing ---
if __name__ == "__main__":
    script_dir = os.path.dirname(__file__)
    # Example: Load workflow from a file in the same directory as the script
    # In a real scenario, Flask will pass the workflow_data_dict directly.
    default_workflow_filename = "p4.json" # The JSON you provided for testing
    default_workflow_path = os.path.join(script_dir, default_workflow_filename)

    # Base directory where data CSVs are expected to be found (relative to script dir)
    # If SourceNode has "other_data.csv", it will look for "backend/other_data.csv"
    data_directory = script_dir 

    try:
        with open(default_workflow_path, 'r') as f:
            workflow_to_test = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Workflow file not found at '{default_workflow_path}'. Cannot run test.")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not parse JSON from '{default_workflow_path}'. Check syntax. Details: {e}")
        exit(1)
    
    # Output directory for standalone test
    default_output_dir = os.path.join(script_dir, "processed_output_standalone_test")

    print(f"--- Running Standalone Test ---")
    print(f"Workflow: '{default_workflow_path}'")
    # The actual input CSV will be determined by the Source node(s) in the workflow

    results = run_workflow_processing(workflow_to_test, data_directory, default_output_dir)

    print("-" * 30)
    if results.get("error"):
        print(f"[TEST FAILED]")
        print(f"Error: {results['error']}")
    else:
        print(f"[TEST SUCCESS]")
        print(f"Output file: {results.get('output_file')}")
        print(f"Stats: {results.get('stats')}")