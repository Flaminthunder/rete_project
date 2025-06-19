import json
import time
from collections import deque
import csv
import re
import os
import logging

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class WorkflowEngine:
    """
    Parses a JSON ruleset from the Rete.js frontend and executes the workflow
    against a given CSV file, row by row.
    """
    def __init__(self, workflow_data: dict):
        """
        Initializes the engine with the workflow definition dictionary.
        Raises ValueError if the workflow contains a cycle.
        """
        self.nodes = {}
        self.connections_from_source = {}
        self.connections_to_target = {}
        self.default_action = workflow_data.get('defaultAction', 'ACCEPT')
        self._load_workflow_from_data(workflow_data)
        # The following line will raise a ValueError if a cycle is detected.
        self.node_order = self._get_topological_order()

    def _load_workflow_from_data(self, workflow: dict):
        if not workflow or "nodes" not in workflow:
            logging.error("Empty or invalid workflow data provided.")
            return

        for node_data in workflow.get("nodes", []):
            node_id = node_data["id"]
            self.nodes[node_id] = node_data
            self.connections_from_source[node_id] = []
            self.connections_to_target[node_id] = []

        for conn_data in workflow.get("connections", []):
            source_id, target_id = conn_data["sourceNodeId"], conn_data["targetNodeId"]
            if source_id not in self.nodes or target_id not in self.nodes:
                logging.warning(f"Connection '{conn_data.get('id')}' refs non-existent node(s). Skipping.")
                continue
            
            self.connections_from_source[source_id].append(conn_data)
            self.connections_to_target[target_id].append(conn_data)

    def _get_topological_order(self):
        """
        Performs a topological sort.
        Raises a ValueError if a cycle is detected in the graph.
        """
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
                if in_degree[v_id] == 0:
                    queue.append(v_id)

        if len(sorted_order) != len(self.nodes):
            cycle_nodes = set(self.nodes.keys()) - set(sorted_order)
            error_msg = f"Workflow has a cycle and cannot be processed. Nodes involved: {cycle_nodes}"
            logging.error(error_msg)
            raise ValueError(error_msg)

        return sorted_order

    def _cast_value(self, value, target_type_str):
        if value is None: return None
        target_type_str = target_type_str.lower()
        try:
            if target_type_str == "float": return float(value)
            if target_type_str == "int": return int(float(value))
            if target_type_str == "string": return str(value)
            if target_type_str == "bool":
                if isinstance(value, str): return value.lower() in ['true', '1', 't', 'y', 'yes']
                return bool(value)
            return value
        except (ValueError, TypeError):
            return None

    def _get_variables_from_code_line(self, code_line):
        return set(re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', code_line))

    def process_event(self, row_data: dict, row_num_for_log="N/A"):
        """
        Processes a single data row through the entire workflow.
        Returns the decision ('ACCEPT', 'DISCARD', or other action label).
        """
        evaluated_outputs = {node_id: {} for node_id in self.nodes}

        for node_id in self.node_order:
            node = self.nodes.get(node_id)
            node_type = node["type"]
            node_label = node.get("label", node_type)

            current_node_inputs = {}
            for conn in self.connections_to_target.get(node_id, []):
                source_id, source_key = conn["sourceNodeId"], conn["sourceOutputKey"]
                target_key = conn["targetInputKey"]
                input_value = evaluated_outputs.get(source_id, {}).get(source_key, False)
                current_node_inputs[target_key] = input_value

            if node_type == "Source":
                evaluated_outputs[node_id]['output0'] = True
            
            elif node_type == "Rule":
                code_line = node.get("codeLine", "False")
                var_type = node.get("variableType", "string")
                
                eval_scope = {}
                possible_to_eval = True
                for var_name in self._get_variables_from_code_line(code_line):
                    if var_name in row_data:
                        casted_val = self._cast_value(row_data[var_name], var_type)
                        if casted_val is None and row_data[var_name] is not None:
                            possible_to_eval = False
                            break
                        eval_scope[var_name] = casted_val
                
                result = False
                if possible_to_eval:
                    try:
                        restricted_globals = {"__builtins__": {"True": True, "False": False, "len": len}}
                        result = bool(eval(code_line, restricted_globals, eval_scope))
                    except Exception as e:
                        logging.warning(f"Row {row_num_for_log}: Error in rule '{node_label}': {e}. Defaulting to False.")
                        result = False

                evaluated_outputs[node_id]['outputTrue'] = result
                evaluated_outputs[node_id]['outputFalse'] = not result

            elif node_type in ["AND", "OR"]:
                input_values = list(current_node_inputs.values())
                result = all(input_values) if node_type == "AND" else any(input_values)
                evaluated_outputs[node_id]['output'] = result

            elif node_type == "Action":
                if any(current_node_inputs.values()):
                    return node_label
        
        return self.default_action


def load_csv_data_from_file(filepath):
    data_list = []
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames:
                logging.warning(f"CSV file '{filepath}' is empty or has no header row.")
                return []
            
            fieldnames = [h.strip() for h in reader.fieldnames]
            reader.fieldnames = fieldnames
            for row in reader:
                processed_row = {k: (v.strip() if v else None) for k, v in row.items()}
                data_list.append(processed_row)
    except FileNotFoundError:
        logging.error(f"CSV data file '{filepath}' not found.")
        return None
    except Exception as e:
        logging.error(f"Error reading CSV file '{filepath}': {e}")
        return None
    return data_list


def run_workflow_processing(workflow_data_dict: dict, input_csv_filepath: str, output_dir: str):
    try:
        engine = WorkflowEngine(workflow_data_dict)
    except ValueError as e:
        # Catches the error from topological sort if a cycle is found
        return {"error": str(e), "output_file": None, "stats": None}

    all_input_data = load_csv_data_from_file(input_csv_filepath)
    if all_input_data is None:
        return {"error": f"Failed to load data from '{input_csv_filepath}'.", "output_file": None, "stats": None}
    if not all_input_data:
        return {"error": f"No data found in '{input_csv_filepath}'.", "output_file": None, "stats": None}

    processed_data = []
    logging.info(f"=== PROCESSING CSV: {input_csv_filepath} ===")
    start_time = time.time()
    decision_counts = {}

    for i, data_row in enumerate(all_input_data):
        decision = engine.process_event(data_row.copy(), row_num_for_log=i + 2)
        decision_counts[decision] = decision_counts.get(decision, 0) + 1
        output_row = data_row.copy()
        output_row['workflow_decision'] = decision
        processed_data.append(output_row)

    stats = {
        "total_processed": len(all_input_data),
        "time_taken": f"{time.time() - start_time:.2f}s",
        "decisions": decision_counts
    }
    logging.info(f"=== PROCESSING COMPLETE: Stats: {stats} ===")

    try:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        output_filename = f"processed_{timestamp}_{os.path.basename(input_csv_filepath)}"
        output_filepath = os.path.join(output_dir, output_filename)
        
        output_fieldnames = list(all_input_data[0].keys()) + ['workflow_decision']
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

# --- Main block for standalone testing ---
if __name__ == "__main__":
    # Get the absolute path to the directory where this script is located
    script_dir = os.path.dirname(__file__)

    # **THE FIX IS HERE**: Build absolute paths to the test files
    default_workflow_path = os.path.join(script_dir, "p4.json")
    default_csv_path = os.path.join(script_dir, "pill_data.csv")
    default_output_dir = os.path.join(script_dir, "processed_output")

    try:
        with open(default_workflow_path, 'r') as f:
            workflow_to_test = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Workflow file not found at '{default_workflow_path}'. Cannot run test.")
        exit(1)
    except json.JSONDecodeError:
        print(f"ERROR: Could not parse JSON from '{default_workflow_path}'. Check for syntax errors.")
        exit(1)
        
    print(f"--- Running Standalone Test ---")
    print(f"Workflow: '{default_workflow_path}'")
    print(f"Data: '{default_csv_path}'")

    results = run_workflow_processing(workflow_to_test, default_csv_path, default_output_dir)

    print("-" * 30)
    if results.get("error"):
        print(f"[TEST FAILED]")
        print(f"Error: {results['error']}")
    else:
        print(f"[TEST SUCCESS]")
        print(f"Output file: {results.get('output_file')}")
        print(f"Stats: {results.get('stats')}")