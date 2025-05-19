// src/editor.ts

import { createRoot } from "react-dom/client";
import { NodeEditor, GetSchemes, ClassicPreset, NodeId } from "rete";
import { AreaPlugin, AreaExtensions } from "rete-area-plugin";
import {
    ConnectionPlugin,
    Presets as ConnectionPresets
} from "rete-connection-plugin";
import {
    ReactPlugin,
    Presets as ReactPresets,
    ReactArea2D
} from "rete-react-plugin";
import {
    AutoArrangePlugin,
    Presets as ArrangePresets
} from "rete-auto-arrange-plugin";
import {
    ContextMenuExtra,
    ContextMenuPlugin,
    Presets as ContextMenuPresets
} from "rete-context-menu-plugin";

// --- Exported Data Structures --- (Same as before)
interface BaseExportedNodeData { id: string; label: string; type: string; }
interface ExportedRuleNodeData extends BaseExportedNodeData { type: "Rule"; numInputs: number; numOutputs: number; }
interface ExportedActionNodeData extends BaseExportedNodeData { type: "Action"; numInputs: 1; }
enum LogicType { AND = "AND", OR = "OR" }
interface ExportedLogicGateNodeData extends BaseExportedNodeData { type: LogicType; numInputs: number; }
type ExportedNode = ExportedRuleNodeData | ExportedLogicGateNodeData | ExportedActionNodeData;
interface ExportedConnectionData { id: string; sourceNodeId: string; sourceOutputKey: string; targetNodeId: string; targetInputKey: string; }
interface Ruleset { nodes: ExportedNode[]; connections: ExportedConnectionData[]; }

// --- Node Sizing Constants --- (Using your last specified values)
const RULE_NODE_TARGET_WIDTH = 200;
const RULE_NODE_TARGET_HEIGHT = 160;
const LOGIC_NODE_TARGET_WIDTH = 250;
const LOGIC_NODE_BASE_HEIGHT_FOR_MIN_INPUTS = 190;
const LOGIC_NODE_HEIGHT_PER_ADDITIONAL_INPUT = 36;
const LOGIC_NODE_BASE_INPUT_COUNT = 2;
const ACTION_NODE_TARGET_WIDTH = 200;
const ACTION_NODE_TARGET_HEIGHT = 120;

// --- Node Implementations --- (No changes from previous version that had correct sizing)
class RuleNode extends ClassicPreset.Node<
    { [key: `input${number}`]: ClassicPreset.Socket },
    { [key: `output${number}`]: ClassicPreset.Socket },
    { label: ClassicPreset.InputControl<"text"> }
> {
    width: number;
    height: number;
    constructor(initialLabel: string = "Rule", numInputs: number = 1, numOutputs: number = 1) {
        super(initialLabel);
        this.width = RULE_NODE_TARGET_WIDTH;
        this.height = RULE_NODE_TARGET_HEIGHT;
        this.addControl("label", new ClassicPreset.InputControl("text", { initial: initialLabel, readonly: false }));
        const socketInstance = new ClassicPreset.Socket("socket");
        for (let i = 0; i < numInputs; i++) this.addInput(`input${i}`, new ClassicPreset.Input(socketInstance, `In ${i + 1}`));
        for (let i = 0; i < numOutputs; i++) this.addOutput(`output${i}`, new ClassicPreset.Output(socketInstance, `Out ${i + 1}`));
    }
    getCustomData(): ExportedRuleNodeData {
        const labelControl = this.controls.label;
        const currentNumInputs = Object.keys(this.inputs).length;
        const currentNumOutputs = Object.keys(this.outputs).length;
        return { id: this.id, label: labelControl.value || "Unnamed Rule", type: "Rule", numInputs: currentNumInputs, numOutputs: currentNumOutputs };
    }
}

type LogicGateInputKey = `input${number}`;
class LogicGateNode extends ClassicPreset.Node<
    { [key in LogicGateInputKey]?: ClassicPreset.Socket },
    { output: ClassicPreset.Socket },
    { numInputs: ClassicPreset.InputControl<"number"> }
> {
    width: number;
    height: number;
    public readonly nodeLogicType: LogicType;
    private editorRef: NodeEditor<Schemes> | null = null;
    private areaRef: AreaPlugin<Schemes, AreaExtra> | null = null;
    constructor(logicType: LogicType, initialNumInputs: number = 2, getEditor?: () => NodeEditor<Schemes>, getArea?: () => AreaPlugin<Schemes, AreaExtra>) {
        super(logicType.toString());
        this.nodeLogicType = logicType;
        if (getEditor) this.editorRef = getEditor();
        if (getArea) this.areaRef = getArea();
        this.width = LOGIC_NODE_TARGET_WIDTH;
        this.height = LOGIC_NODE_BASE_HEIGHT_FOR_MIN_INPUTS;
        const minInputs = LOGIC_NODE_BASE_INPUT_COUNT;
        this.addControl("numInputs", new ClassicPreset.InputControl("number", {
            initial: Math.max(minInputs, initialNumInputs),
            change: (value) => {
                const currentControlDisplayValue = this.controls.numInputs?.value;
                const desiredNumInputs = Math.max(minInputs, Math.floor(value as number));
                const currentSocketCount = Object.keys(this.inputs).length;
                if (desiredNumInputs !== currentSocketCount) this.updateInputSockets(desiredNumInputs);
                if (this.controls.numInputs && (currentControlDisplayValue !== desiredNumInputs || (value as number) !== desiredNumInputs)) {
                    this.controls.numInputs.setValue(desiredNumInputs);
                }
            }
        }));
        this.addOutput("output", new ClassicPreset.Output(new ClassicPreset.Socket("socket"), "Out"));
        this.updateInputSockets(Math.max(minInputs, initialNumInputs));
    }
    private updateInputSockets(newCount: number) {
        const currentInputs = this.inputs as { [key in LogicGateInputKey]?: ClassicPreset.Input<ClassicPreset.Socket> };
        const currentInputKeys = Object.keys(currentInputs) as LogicGateInputKey[];
        const currentCount = currentInputKeys.length;
        const socketInstance = new ClassicPreset.Socket("socket");
        if (newCount === currentCount) { this._recalculateDimensions(); return; }
        if (newCount > currentCount) {
            for (let i = currentCount; i < newCount; i++) {
                const newInputKey = `input${i}` as LogicGateInputKey;
                this.addInput(newInputKey, new ClassicPreset.Input(socketInstance, `In ${i + 1}`));
            }
        } else if (newCount < currentCount) {
            for (let i = currentCount - 1; i >= newCount; i--) {
                const inputKeyToRemove = `input${i}` as LogicGateInputKey;
                if (this.editorRef && currentInputs[inputKeyToRemove]) {
                    const connections = this.editorRef.getConnections();
                    connections.forEach(conn => {
                        if (conn.target === this.id && conn.targetInput === inputKeyToRemove) this.editorRef?.removeConnection(conn.id);
                    });
                }
                if (currentInputs[inputKeyToRemove]) this.removeInput(inputKeyToRemove);
            }
        }
        this._recalculateDimensions();
    }
    private _recalculateDimensions() {
        const numInputs = Object.keys(this.inputs).length;
        let calculatedHeight = LOGIC_NODE_BASE_HEIGHT_FOR_MIN_INPUTS;
        if (numInputs > LOGIC_NODE_BASE_INPUT_COUNT) {
            const additionalInputs = numInputs - LOGIC_NODE_BASE_INPUT_COUNT;
            calculatedHeight += additionalInputs * LOGIC_NODE_HEIGHT_PER_ADDITIONAL_INPUT;
        }
        this.height = Math.max(calculatedHeight, LOGIC_NODE_BASE_HEIGHT_FOR_MIN_INPUTS);
        if (this.areaRef) this.areaRef.update("node", this.id);
    }
    getCustomData(): ExportedLogicGateNodeData {
        const numInputs = Object.keys(this.inputs).length;
        return { id: this.id, label: this.label, type: this.nodeLogicType, numInputs };
    }
}
class ActionNode extends ClassicPreset.Node<
    { input0: ClassicPreset.Socket }, {}, { label: ClassicPreset.InputControl<"text"> }
> {
    width: number; height: number;
    constructor(initialLabel: string = "Action") {
        super(initialLabel);
        this.width = ACTION_NODE_TARGET_WIDTH; this.height = ACTION_NODE_TARGET_HEIGHT;
        this.addControl("label", new ClassicPreset.InputControl("text", { initial: initialLabel, readonly: false }));
        this.addInput("input0", new ClassicPreset.Input(new ClassicPreset.Socket("socket"), "In"));
    }
    getCustomData(): ExportedActionNodeData {
        const labelControl = this.controls.label;
        return { id: this.id, label: labelControl.value || "Unnamed Action", type: "Action", numInputs: 1 };
    }
}

// --- Define Schemes, AreaExtra etc. ---
type SchemeNodeForPlugins = ClassicPreset.Node;
type SchemeConnectionForPlugins = ClassicPreset.Connection<SchemeNodeForPlugins, SchemeNodeForPlugins> & { isLoop?: boolean };
type Schemes = GetSchemes<SchemeNodeForPlugins, SchemeConnectionForPlugins>;
type MyNodeClasses = RuleNode | LogicGateNode | ActionNode;
type AreaExtra = ReactArea2D<Schemes> | ContextMenuExtra;

// --- createEditor function ---
export async function createEditor(container: HTMLElement) {
    // console.log("[Editor] Creating editor instance..."); // Basic log
    const editor = new NodeEditor<Schemes>();
    const area = new AreaPlugin<Schemes, AreaExtra>(container);
    const connectionPlugin = new ConnectionPlugin<Schemes, AreaExtra>();
    const render = new ReactPlugin<Schemes, AreaExtra>({ createRoot });
    const arrange: any = new AutoArrangePlugin();
    const selector = AreaExtensions.selector();

    AreaExtensions.selectableNodes(area, selector, {
        accumulating: AreaExtensions.accumulateOnCtrl()
    });

    const getEditorInstance = () => editor;
    const getAreaInstance = () => area;

    const removeNodeAndConnections = (nodeIdToRemove: NodeId) => {
        console.log(`[Editor] removeNodeAndConnections called for ID: ${nodeIdToRemove}`);
        const connectionsToRemove = editor.getConnections().filter(conn =>
            conn.source === nodeIdToRemove || conn.target === nodeIdToRemove
        );
        console.log(`[Editor] Found ${connectionsToRemove.length} connections to remove for node ${nodeIdToRemove}.`);
        connectionsToRemove.forEach(conn => {
            // console.log(`[Editor] Removing connection: ${conn.id}`);
            try {
                editor.removeConnection(conn.id);
            } catch (e) {
                console.error(`[Editor] Error removing connection ${conn.id}:`, e);
            }
        });
        // console.log(`[Editor] Removing node: ${nodeIdToRemove}`);
        try {
            editor.removeNode(nodeIdToRemove);
        } catch (e) {
            console.error(`[Editor] Error removing node ${nodeIdToRemove}:`, e);
        }
        console.log(`[Editor] Finished removeNodeAndConnections for ${nodeIdToRemove}. Remaining nodes: ${editor.getNodes().length}`);
    };

    const menuItems: (readonly [string, (() => SchemeNodeForPlugins) | (() => void)])[] = [
        ["Rule Node (1 In, 1 Out)", () => new RuleNode("New Rule", 1, 1)],
        ["Rule Node (2 In, 1 Out)", () => new RuleNode("Decision Rule", 2, 1)],
        ["Logic: AND", () => new LogicGateNode(LogicType.AND, 2, getEditorInstance, getAreaInstance)],
        ["Logic: OR", () => new LogicGateNode(LogicType.OR, 2, getEditorInstance, getAreaInstance)],
        ["Action Node", () => new ActionNode("New Action")],
        ["Delete", () => {
            const selectedIds = Array.from(selector.entities.keys());
            console.log("[ContextMenu] Delete action. Prefixed IDs from selector:", selectedIds);
            if (selectedIds.length === 0) {
                console.log("[ContextMenu] No nodes selected.");
                return;
            }
            selectedIds.forEach(prefixedId => {
                const actualNodeId = prefixedId.startsWith('node_') ? prefixedId.substring(5) : prefixedId;
                if (editor.getNode(actualNodeId as NodeId)) {
                    removeNodeAndConnections(actualNodeId as NodeId);
                } else {
                     console.warn(`[ContextMenu] Node ${actualNodeId} not found in editor for deletion (prefixed ID was ${prefixedId}).`);
                }
            });
            selector.unselectAll();
        }]
    ];

    const contextMenu = new ContextMenuPlugin<Schemes>({
        items: ContextMenuPresets.classic.setup(menuItems as any)
    });
    area.use(contextMenu);

    render.addPreset(ReactPresets.contextMenu.setup());
    render.addPreset(ReactPresets.classic.setup());
    connectionPlugin.addPreset(ConnectionPresets.classic.setup());
    arrange.addPreset(ArrangePresets.classic.setup());

    editor.use(area);
    area.use(connectionPlugin);
    area.use(render);
    area.use(arrange);

    AreaExtensions.simpleNodesOrder(area);
    AreaExtensions.showInputControl(area);

    const rule1 = new RuleNode("Condition A", 1, 1);
    const rule2 = new RuleNode("Condition B", 1, 1);
    const andGate = new LogicGateNode(LogicType.AND, 2, getEditorInstance, getAreaInstance);
    const resultRule = new RuleNode("Outcome X", 1, 1);
    const actionNode1 = new ActionNode("Perform Task");

    await editor.addNode(rule1);
    await editor.addNode(rule2);
    await editor.addNode(andGate);
    await editor.addNode(resultRule);
    await editor.addNode(actionNode1);
    // console.log("[Editor] Initial nodes added:", editor.getNodes().map(n => n.id));

    if (rule1.outputs['output0'] && (andGate.inputs as any)['input0' as LogicGateInputKey]) { /* ... */ }
    // ... (other initial connections, ensure they are robust or add logs if needed) ...
    if (rule1.outputs['output0'] && (andGate.inputs as any)['input0' as LogicGateInputKey]) {
        await editor.addConnection(new ClassicPreset.Connection(rule1, 'output0', andGate, 'input0' as LogicGateInputKey) as SchemeConnectionForPlugins);
    }
    if (rule2.outputs['output0'] && (andGate.inputs as any)['input1' as LogicGateInputKey]) {
        await editor.addConnection(new ClassicPreset.Connection(rule2, 'output0', andGate, 'input1' as LogicGateInputKey) as SchemeConnectionForPlugins);
    }
    if (andGate.outputs['output'] && resultRule.inputs['input0']) {
        await editor.addConnection(new ClassicPreset.Connection(andGate, 'output', resultRule, 'input0') as SchemeConnectionForPlugins);
    }
    if (resultRule.outputs['output0'] && actionNode1.inputs['input0']) {
        await editor.addConnection(new ClassicPreset.Connection(resultRule, 'output0', actionNode1, 'input0') as SchemeConnectionForPlugins);
    }

    const logGraphForArrange = () => { /* ... as before ... */ };

    await arrange.layout({ /* ... */ });
    AreaExtensions.zoomAt(area, editor.getNodes());

    // --- Window functions for App.tsx ---
    (window as any).editorInstance = editor; // Expose for direct debugging if needed

    (window as any).addEditorNode = async (type: 'Rule' | 'AND' | 'OR' | 'Action', props?: any) => {
        let nodeToAdd; const defaultProps = { label: "New Node", numInputs: 1, numOutputs: 1, ...props };
        console.log(`[App] addEditorNode: Type: ${type}`);
        switch (type) {
            case 'Rule': nodeToAdd = new RuleNode(defaultProps.label, defaultProps.numInputs, defaultProps.numOutputs); break;
            case 'AND': nodeToAdd = new LogicGateNode(LogicType.AND, defaultProps.numInputs || 2, getEditorInstance, getAreaInstance); break;
            case 'OR': nodeToAdd = new LogicGateNode(LogicType.OR, defaultProps.numInputs || 2, getEditorInstance, getAreaInstance); break;
            case 'Action': nodeToAdd = new ActionNode(defaultProps.label || "New Action"); break;
            default: console.error("Unknown node type to add:", type); return;
        }
        if (nodeToAdd) {
            await editor.addNode(nodeToAdd);
            console.log(`[Editor] Node ${nodeToAdd.id} (${type}) added. Total nodes: ${editor.getNodes().length}`);
        }
    };

    (window as any).removeSelectedEditorNodes = async () => {
        const selectedIds = Array.from(selector.entities.keys());
        console.log("[App] removeSelectedEditorNodes. Prefixed IDs from selector:", selectedIds);
        if (selectedIds.length === 0) {
            console.log("[App] No nodes selected to remove.");
            return;
        }
        selectedIds.forEach(prefixedId => {
            const actualNodeId = prefixedId.startsWith('node_') ? prefixedId.substring(5) : prefixedId;
            // console.log(`[App] Checking node for removal, actual ID: ${actualNodeId}`);
            if (editor.getNode(actualNodeId as NodeId)) {
                removeNodeAndConnections(actualNodeId as NodeId);
            } else {
                 console.warn(`[App] Node ${actualNodeId} not found in editor during removeSelectedEditorNodes.`);
            }
        });
        selector.unselectAll();
    };

    (window as any).arrangeLayout = async () => { /* ... as before ... */ };
    (window as any).exportWorkflow = () => { /* ... as before ... */ };
    // ... (ensure all window functions from previous working version are here) ...
    (window as any).arrangeLayout = async () => {
        // logGraphForArrange();
        try {
            await arrange.layout({ zoom: false, getSourceCenter: (n: SchemeNodeForPlugins) => { const actualNode = n as MyNodeClasses; return { x: actualNode.width / 2, y: actualNode.height / 2 }; }, getTargetCenter: (n: SchemeNodeForPlugins) => { const actualNode = n as MyNodeClasses; return { x: actualNode.width / 2, y: actualNode.height / 2 }; } });
            AreaExtensions.zoomAt(area, editor.getNodes());
        } catch (e) { console.error("[App] Error during arrange.layout:", e); }
    };
    (window as any).exportWorkflow = () => {
        const nodes = editor.getNodes(); const connections = editor.getConnections();
        const ruleset = generateRuleset(nodes, connections);
        console.log("[Export Workflow] Ruleset generated successfully:", JSON.stringify(ruleset, null, 2));
        alert("Workflow exported to console.");
        return ruleset;
    };


    // console.log("[Editor] createEditor finished setup.");
    return {
        destroy: () => { /* ... as before ... */ }
    };
}

// --- generateRuleset function --- (No changes needed here)
function generateRuleset(
    nodes: SchemeNodeForPlugins[],
    connections: SchemeConnectionForPlugins[]
): Ruleset { /* ... as before ... */
    const exportedNodes: ExportedNode[] = nodes.map(node => {
        const customNode = node as MyNodeClasses;
        if (customNode instanceof RuleNode) {
            return customNode.getCustomData();
        } else if (customNode instanceof LogicGateNode) {
            return customNode.getCustomData();
        } else if (customNode instanceof ActionNode) {
            return customNode.getCustomData();
        }
        console.error("[GenerateRuleset] Unknown node type:", node.label, node.id);
        return { id: node.id, type: "Unknown", label: node.label } as any;
    });
    const exportedConnections: ExportedConnectionData[] = connections.map(conn => ({
        id: conn.id,
        sourceNodeId: conn.source,
        sourceOutputKey: conn.sourceOutput as string,
        targetNodeId: conn.target,
        targetInputKey: conn.targetInput as string,
    }));
    return { nodes: exportedNodes, connections: exportedConnections };
}