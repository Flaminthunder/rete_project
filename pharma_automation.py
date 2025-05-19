import json
import time
from collections import deque

# --- Configuration (Adjust these as per your actual process if needed) ---
TABLET_WEIGHT_NOMINAL_MG = 500.0
TABLET_WEIGHT_TOLERANCE_PERCENT = 2.0 # +/- 2%
COLOR_MATCH_THRESHOLD = 0.95 # Example, for "Tablet Color Mismatch" if you use it

# --- Rule Implementations ---
# Keys MUST match 'label' of RuleNodes in your Rete JSON.
RULE_IMPLEMENTATIONS = {
    # Rules from your pharma_workflow.json:
    "is_cracked == True": lambda data: data.get("camera_inspection", {}).get("is_cracked", False),
    "weight > 0.8": lambda data: data.get("weight_scale", {}).get("weight_mg", 0) > 0.8,
                       # !!! CRITICAL: Review this logic. Is 0.8 the correct threshold?
                       # Or should it be relative to nominal weight?
                       # e.g., data.get("weight_scale", {}).get("weight_mg", 0) > (TABLET_WEIGHT_NOMINAL_MG * 0.8 / 100) if 0.8 is a percentage
                       # e.g., abs(data.get("weight_scale", {}).get("weight_mg", TABLET_WEIGHT_NOMINAL_MG) - TABLET_WEIGHT_NOMINAL_MG) > 0.8 if 0.8 is a delta
    "color != 'blue'": lambda data: data.get("camera_inspection", {}).get("color_observed", "").lower() != "blue",

    # Keep these from the dummy example if you might switch workflows or for reference
    "Tablet Is Cracked": lambda data: data.get("camera_inspection", {}).get("is_cracked", False),
    "Tablet Color Mismatch": lambda data: data.get("camera_inspection", {}).get("color_match_score", 1.0) < COLOR_MATCH_THRESHOLD,
    "Tablet Weight Out of Spec": lambda data: not (
        TABLET_WEIGHT_NOMINAL_MG * (1 - TABLET_WEIGHT_TOLERANCE_PERCENT / 100)
        <= data.get("weight_scale", {}).get("weight_mg", TABLET_WEIGHT_NOMINAL_MG)
        <= TABLET_WEIGHT_NOMINAL_MG * (1 + TABLET_WEIGHT_TOLERANCE_PERCENT / 100)
    ),
    "Foreign Particle Detected": lambda data: data.get("camera_inspection", {}).get("foreign_particle_detected", False),
    "Defect Confirmed": lambda input_boolean: bool(input_boolean), # For rules processing other nodes' output
}

# --- Action Implementations ---
# Keys MUST match 'label' of ActionNodes in your Rete JSON.
ACTION_IMPLEMENTATIONS = {
    # Action from your pharma_workflow.json:
    "DISCARD": lambda data, val: print(f"  ACTION >> DISCARDING tablet {data.get('tablet_id')} (Trigger value from OR gate: {val})"),

    # Keep these from the dummy example if you might switch workflows or for reference
    "Divert Defective Tablet": lambda data, val: print(f"  ACTION >> Diverting tablet {data.get('tablet_id')} due to defect (Trigger: {val})"),
    "Raise Critical Alarm": lambda data, val: print(f"  ALARM  >> CRITICAL DEFECT on tablet {data.get('tablet_id')}! (Trigger: {val})"),
    "Log Good Tablet": lambda data, val: print(f"  INFO   >> Tablet {data.get('tablet_id')} passed inspection (Defect signal: {val}).") if not val else None, # Special handling for dummy
    "Log Minor Defect": lambda data, val: print(f"  WARN   >> Minor defect noted for tablet {data.get('tablet_id')}. (Trigger: {val})"),
    "HALT": lambda data, val: print(f"  ACTION >> System HALT triggered for tablet {data.get('tablet_id')}. (Trigger value: {val})")
}


class WorkflowEngine:
    def __init__(self, workflow_json_path):
        self.nodes = {}
        self.connections_from_source = {}
        self.connections_to_target = {}
        self._load_workflow(workflow_json_path)
        self.node_order = self._get_topological_order()
        if not self.node_order and self.nodes:
            print("Error: Could not determine node processing order. Check for cycles or ensure graph is connected.")
            print("Proceeding with loaded node order, but execution might be unpredictable for complex graphs.")
            self.node_order = list(self.nodes.keys())

    def _load_workflow(self, filepath):
        try:
            with open(filepath, 'r') as f:
                workflow = json.load(f)
        except FileNotFoundError:
            print(f"Error: Workflow file '{filepath}' not found.")
            return
        except json.JSONDecodeError as e:
            print(f"Error: Could not decode JSON from workflow file '{filepath}'. Details: {e}")
            return

        for node_data in workflow.get("nodes", []):
            node_id = node_data["id"]
            self.nodes[node_id] = {
                "id": node_id,
                "type": node_data["type"],
                "label": node_data.get("label", node_data["type"]),
                "num_inputs": node_data.get("numInputs", 0),
                "num_outputs": node_data.get("numOutputs", 0),
                "data": node_data
            }
            self.connections_from_source[node_id] = []
            self.connections_to_target[node_id] = []

        for conn_data in workflow.get("connections", []):
            source_id = conn_data["sourceNodeId"]
            target_id = conn_data["targetNodeId"]
            
            if source_id not in self.nodes:
                print(f"Warning: Source node '{source_id}' for connection '{conn_data['id']}' not found. Skipping connection.")
                continue
            if target_id not in self.nodes:
                print(f"Warning: Target node '{target_id}' for connection '{conn_data['id']}' not found. Skipping connection.")
                continue

            connection_info = {
                "source_id": source_id,
                "source_output_key": conn_data["sourceOutputKey"],
                "target_id": target_id,
                "target_input_key": conn_data["targetInputKey"],
                "id": conn_data["id"]
            }
            self.connections_from_source[source_id].append(connection_info)
            self.connections_to_target[target_id].append(connection_info)
        
        num_loaded_connections = sum(len(v) for v in self.connections_from_source.values())
        print(f"Loaded {len(self.nodes)} nodes and {num_loaded_connections} connections from workflow.")


    def _get_topological_order(self):
        if not self.nodes:
            return []
            
        in_degree = {node_id: 0 for node_id in self.nodes}
        for node_id in self.nodes:
            in_degree[node_id] = len(self.connections_to_target.get(node_id, []))
            
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
            print("Warning: Topological sort incomplete. Possible cycle or disconnected components.")
            for node_id in self.nodes:
                if node_id not in sorted_order:
                    sorted_order.append(node_id)
        return sorted_order

    def process_event(self, sensor_data):
        print(f"\n--- Processing Event for Tablet: {sensor_data.get('tablet_id', 'Unknown')} ---")
        evaluated_outputs = {node_id: {} for node_id in self.nodes}

        if not self.node_order:
            print("  Error: Node order not determined. Cannot process event.")
            return

        for node_id in self.node_order:
            node = self.nodes.get(node_id)
            if not node:
                print(f"  Skipping: Node {node_id} referenced in order but not found in node list.")
                continue

            node_type = node["type"]
            node_label = node["label"]
            print(f"  Evaluating Node: '{node_label}' (ID: {node_id}, Type: {node_type})")

            current_node_inputs_values = {}
            incoming_connections = self.connections_to_target.get(node_id, [])
            
            for conn in incoming_connections:
                source_node_id = conn["source_id"]
                source_output_key = conn["source_output_key"]
                target_input_key = conn["target_input_key"]
                
                if source_node_id in evaluated_outputs and source_output_key in evaluated_outputs[source_node_id]:
                    current_node_inputs_values[target_input_key] = evaluated_outputs[source_node_id][source_output_key]
                else:
                    print(f"    Warning: Input '{target_input_key}' for '{node_label}' from '{source_node_id}.{source_output_key}' not found. Defaulting to False.")
                    current_node_inputs_values[target_input_key] = False 

            if node_type == "Rule":
                rule_func = RULE_IMPLEMENTATIONS.get(node_label)
                if rule_func:
                    result = False
                    is_entry_rule = (node["num_inputs"] == 0) or \
                                     (node["num_inputs"] > 0 and not any(c['target_id'] == node_id and c['target_input_key'] == 'input0' for c in incoming_connections if c['target_id'] == node_id)) # Check only connections to *this* node
                    
                    if is_entry_rule:
                        try:
                            result = rule_func(sensor_data)
                            print(f"    Rule '{node_label}' (on sensor data) -> {result}")
                        except Exception as e:
                            print(f"    Error executing entry rule '{node_label}': {e}")
                            result = False
                    elif node["num_inputs"] > 0:
                        input_val = current_node_inputs_values.get('input0', False)
                        try:
                            result = rule_func(input_val) 
                            print(f"    Rule '{node_label}' (on input0='{input_val}') -> {result}")
                        except Exception as e:
                            print(f"    Error executing rule '{node_label}' with input '{input_val}': {e}")
                            result = False
                    else:
                         print(f"    Warning: Rule '{node_label}' has unexpected input configuration. Defaulting to False.")
                         result = False

                    for i in range(node["num_outputs"]):
                         evaluated_outputs[node_id][f"output{i}"] = result
                else:
                    print(f"    Warning: No implementation for RuleNode: '{node_label}'")
                    for i in range(node.get("num_outputs", 0)):
                        evaluated_outputs[node_id][f"output{i}"] = False

            elif node_type == "AND":
                all_inputs_true = True
                if node["num_inputs"] == 0:
                    all_inputs_true = True
                elif not current_node_inputs_values and node["num_inputs"] > 0:
                    all_inputs_true = False
                else:
                    for i in range(node["num_inputs"]):
                        input_key = f"input{i}"
                        if not current_node_inputs_values.get(input_key, False):
                            all_inputs_true = False
                            break
                evaluated_outputs[node_id]["output"] = all_inputs_true
                print(f"    AND Gate -> {all_inputs_true} (Inputs: {current_node_inputs_values})")

            elif node_type == "OR":
                any_input_true = False
                if node["num_inputs"] > 0 and current_node_inputs_values:
                    for i in range(node["num_inputs"]):
                        input_key = f"input{i}"
                        if current_node_inputs_values.get(input_key, False):
                            any_input_true = True
                            break
                evaluated_outputs[node_id]["output"] = any_input_true
                print(f"    OR Gate -> {any_input_true} (Inputs: {current_node_inputs_values})")

            elif node_type == "Action":
                action_func = ACTION_IMPLEMENTATIONS.get(node_label)
                if action_func:
                    trigger_value = current_node_inputs_values.get("input0", False)
                    
                    if node_label == "Log Good Tablet" and "Log Good Tablet" in ACTION_IMPLEMENTATIONS : # For dummy workflow compatibility
                        if not trigger_value: 
                            print(f"    Action '{node_label}' (inverted logic for dummy) triggered because defect signal is False.")
                            action_func(sensor_data, False)
                        else:
                            print(f"    Action '{node_label}' (inverted logic for dummy) not triggered (defect signal: {trigger_value}).")
                    elif trigger_value: # Standard trigger on True
                        print(f"    Action '{node_label}' triggered by input0: {trigger_value}")
                        action_func(sensor_data, trigger_value)
                    else:
                        print(f"    Action '{node_label}' not triggered (input0: {trigger_value})")
                else:
                    print(f"    Warning: No implementation for ActionNode: '{node_label}'")
        
        print("--- Event Processing Complete ---")

def load_sensor_data_from_file(filepath):
    try:
        with open(filepath, 'r') as f:
            sensor_data_list = json.load(f)
        if not isinstance(sensor_data_list, list):
            print(f"Error: Sensor data file '{filepath}' should contain a JSON list of events.")
            return []
        print(f"Loaded {len(sensor_data_list)} sensor events from {filepath}")
        return sensor_data_list
    except FileNotFoundError:
        print(f"Error: Sensor data file '{filepath}' not found.")
        return []
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from sensor data file '{filepath}'. Details: {e}")
        return []

if __name__ == "__main__":
    rete_workflow_file = "pharma_workflow.json"
    sensor_data_file = "Pharma_Tablet_Sensor_Data_1000.json"

    # --- Check for actual Rete workflow JSON file ---
    try:
        with open(rete_workflow_file, 'r') as f:
            json.load(f) # Validate if it's loadable JSON
        print(f"Using existing Rete workflow file: {rete_workflow_file}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        if isinstance(e, FileNotFoundError):
            print(f"Rete workflow file '{rete_workflow_file}' not found.")
        else:
            print(f"Error decoding existing Rete workflow file '{rete_workflow_file}': {e}")
        
        # Fallback to creating and using a DUMMY workflow if the primary one fails
        print("Creating and using a DUMMY Rete workflow file: pharma_workflow_DUMMY.json for testing.")
        dummy_workflow_content = {
            "nodes": [
                {"id": "rule_dummy_cracked_id", "label": "Tablet Is Cracked", "type": "Rule", "numInputs": 0, "numOutputs": 1},
                {"id": "rule_dummy_weight_id", "label": "Tablet Weight Out of Spec", "type": "Rule", "numInputs": 0, "numOutputs": 1},
                {"id": "rule_dummy_color_id", "label": "Tablet Color Mismatch", "type": "Rule", "numInputs": 0, "numOutputs": 1},
                {"id": "logic_dummy_any_defect_id", "label": "OR", "type": "OR", "numInputs": 3},
                {"id": "action_dummy_divert_id", "label": "Divert Defective Tablet", "type": "Action", "numInputs": 1},
                {"id": "action_dummy_log_good_id", "label": "Log Good Tablet", "type": "Action", "numInputs": 1}
            ],
            "connections": [
                {"id": "conn_dummy_1", "sourceNodeId": "rule_dummy_cracked_id", "sourceOutputKey": "output0", "targetNodeId": "logic_dummy_any_defect_id", "targetInputKey": "input0"},
                {"id": "conn_dummy_2", "sourceNodeId": "rule_dummy_weight_id", "sourceOutputKey": "output0", "targetNodeId": "logic_dummy_any_defect_id", "targetInputKey": "input1"},
                {"id": "conn_dummy_3", "sourceNodeId": "rule_dummy_color_id", "sourceOutputKey": "output0", "targetNodeId": "logic_dummy_any_defect_id", "targetInputKey": "input2"},
                {"id": "conn_dummy_4", "sourceNodeId": "logic_dummy_any_defect_id", "sourceOutputKey": "output", "targetNodeId": "action_dummy_divert_id", "targetInputKey": "input0"},
                {"id": "conn_dummy_5", "sourceNodeId": "logic_dummy_any_defect_id", "sourceOutputKey": "output", "targetNodeId": "action_dummy_log_good_id", "targetInputKey": "input0"}
            ]
        }
        # Special handling for "Log Good Tablet" action for the DUMMY workflow
        if "Log Good Tablet" in ACTION_IMPLEMENTATIONS:
             ACTION_IMPLEMENTATIONS["Log Good Tablet"] = lambda data, val: print(f"  INFO   >> Tablet {data.get('tablet_id')} passed inspection (Defect signal was {val}).") if not val else None
        
        rete_workflow_file = "pharma_workflow_DUMMY.json" 
        with open(rete_workflow_file, 'w') as f:
            json.dump(dummy_workflow_content, f, indent=2)
        print(f"Using DUMMY workflow file: {rete_workflow_file}")

    
    engine = WorkflowEngine(rete_workflow_file)
    all_sensor_data = load_sensor_data_from_file(sensor_data_file)

    if not all_sensor_data:
        print(f"No sensor data loaded from '{sensor_data_file}'.")
        if not engine.nodes:
            print("Workflow also failed to load. Cannot proceed.")
        else:
            print("Generating a few dummy sensor events for demonstration as data file was empty or not found.")
            all_sensor_data = [
                {"tablet_id": "SIM-GOOD-001", "timestamp": "2023-10-27T10:00:00Z", "camera_inspection": {"is_cracked": False, "color_match_score": 0.99, "foreign_particle_detected": False, "color_observed": "white"}, "weight_scale": {"weight_mg": 500.0}},
                {"tablet_id": "SIM-CRACKED-002", "timestamp": "2023-10-27T10:00:01Z", "camera_inspection": {"is_cracked": True, "color_match_score": 0.98, "foreign_particle_detected": False, "color_observed": "white"}, "weight_scale": {"weight_mg": 501.0}},
                {"tablet_id": "SIM-WEIGHT-BAD-003", "timestamp": "2023-10-27T10:00:02Z", "camera_inspection": {"is_cracked": False, "color_match_score": 0.99, "foreign_particle_detected": False, "color_observed": "white"}, "weight_scale": {"weight_mg": 0.5}}, # Example for "weight > 0.8"
                {"tablet_id": "SIM-COLOR-BLUE-004", "timestamp": "2023-10-27T10:00:03Z", "camera_inspection": {"is_cracked": False, "color_match_score": 0.99, "foreign_particle_detected": False, "color_observed": "blue"}, "weight_scale": {"weight_mg": 500.0}}
            ]
            print(f"Generated {len(all_sensor_data)} dummy events.")


    if engine.nodes and all_sensor_data:
        for i, tablet_data in enumerate(all_sensor_data):
            print(f"\nProcessing tablet data point #{i+1}...")
            engine.process_event(tablet_data)
            if i < len(all_sensor_data) - 1:
                time.sleep(0.05)
    elif not engine.nodes:
        print("Workflow engine not initialized due to load error. Cannot process events.")
    else:
        print(f"No sensor data available from '{sensor_data_file}' or dummy generation to process.")