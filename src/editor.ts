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

// --- Exported Data Structures ---
interface BaseExportedNodeData { id: string; label: string; type: string; source?: string; }
interface ExportedRuleNodeData extends BaseExportedNodeData {
    type: "Rule";
    variableType: string;
    codeLine: string;
    numInputs: number;
    numOutputs: 2; // Fixed to 2 for True/False outputs
}
interface ExportedActionNodeData extends BaseExportedNodeData { type: "Action"; numInputs: 1; }
enum LogicType { AND = "AND", OR = "OR" }
interface ExportedLogicGateNodeData extends BaseExportedNodeData { type: LogicType; numInputs: number; }
interface ExportedSourceNodeData extends BaseExportedNodeData { type: "Source"; source: string; numOutputs: 1; }
type ExportedNode = ExportedRuleNodeData | ExportedLogicGateNodeData | ExportedActionNodeData | ExportedSourceNodeData;
interface ExportedConnectionData { id: string; sourceNodeId: string; sourceOutputKey: string; targetNodeId: string; targetInputKey: string; }
interface Ruleset { nodes: ExportedNode[]; connections: ExportedConnectionData[]; }

// --- Node Sizing Constants ---
const SOURCE_NODE_TARGET_WIDTH = 220;
const SOURCE_NODE_TARGET_HEIGHT = 130;
const RULE_NODE_TARGET_WIDTH = 270;
const RULE_NODE_TARGET_HEIGHT = 230; // Base/min height for RuleNode with 2 controls
const LOGIC_NODE_TARGET_WIDTH = 250;
const LOGIC_NODE_BASE_HEIGHT_FOR_MIN_INPUTS = 190;
const LOGIC_NODE_HEIGHT_PER_ADDITIONAL_INPUT = 36;
const LOGIC_NODE_BASE_INPUT_COUNT = 2;
const ACTION_NODE_TARGET_WIDTH = 200;
const ACTION_NODE_TARGET_HEIGHT = 120;


// --- Node Implementations ---
class SourceNode extends ClassicPreset.Node<
    {}, // No inputs
    { output0: ClassicPreset.Socket }, // One output
    { sourceFile: ClassicPreset.InputControl<"text"> } // Control for filename
> {
    width: number;
    height: number;

    constructor(initialLabel: string = "Data Source", initialSourceFile: string = "data.csv") {
        super(initialLabel);
        this.width = SOURCE_NODE_TARGET_WIDTH;
        this.height = SOURCE_NODE_TARGET_HEIGHT;

        this.addControl(
            "sourceFile",
            new ClassicPreset.InputControl("text", { initial: initialSourceFile, readonly: false })
        );
        this.addOutput("output0", new ClassicPreset.Output(new ClassicPreset.Socket("socket"), "Out"));
    }

    getCustomData(): ExportedSourceNodeData {
        const sourceFileControl = this.controls.sourceFile;
        return {
            id: this.id,
            label: this.label,
            type: "Source",
            source: sourceFileControl.value || "",
            numOutputs: 1,
        };
    }
}

class RuleNode extends ClassicPreset.Node<
    { [key: `input${number}`]: ClassicPreset.Socket },
    { outputTrue: ClassicPreset.Socket; outputFalse: ClassicPreset.Socket; }, // Specific outputs
    { variableType: ClassicPreset.InputControl<"text">; codeLine: ClassicPreset.InputControl<"text">; }
> {
    width: number;
    height: number;

    constructor(
        initialLabel: string = "Rule",
        numInputs: number = 1,
        // numOutputs is now fixed internally to 2, so not a constructor param
        initialVariableType: string = "string",
        initialCodeLine: string = "variable == value"
    ) {
        super(initialLabel);
        this.width = RULE_NODE_TARGET_WIDTH;
        this.height = RULE_NODE_TARGET_HEIGHT; // Initial, will be set by _updateDimensions

        this.addControl(
            "variableType",
            new ClassicPreset.InputControl("text", { initial: initialVariableType, readonly: false })
        );
        this.addControl(
            "codeLine",
            new ClassicPreset.InputControl("text", { initial: initialCodeLine, readonly: false })
        );

        const socketInstance = new ClassicPreset.Socket("socket");
        for (let i = 0; i < numInputs; i++) {
            this.addInput(`input${i}`, new ClassicPreset.Input(socketInstance, `In ${i + 1}`));
        }

        this.addOutput("outputTrue", new ClassicPreset.Output(socketInstance, "T"));
        this.addOutput("outputFalse", new ClassicPreset.Output(socketInstance, "F"));
        
        this._updateDimensions(); // Call to set height based on content
    }

    private _updateDimensions() {
        const numInputs = Object.keys(this.inputs).length;
        const numOutputs = 2; // Fixed for this node

        const V_PADDING = 15;
        const NODE_TITLE_HEIGHT = 25;
        const CONTROL_ROW_HEIGHT = 35;
        const SOCKET_ITEM_HEIGHT = 25;

        let calculatedHeight = V_PADDING;
        calculatedHeight += NODE_TITLE_HEIGHT;
        calculatedHeight += CONTROL_ROW_HEIGHT * 2; // For variableType and codeLine controls
        calculatedHeight += Math.max(numInputs, numOutputs) * SOCKET_ITEM_HEIGHT;
        calculatedHeight += V_PADDING;

        this.height = Math.max(calculatedHeight, RULE_NODE_TARGET_HEIGHT);
    }

    getCustomData(): ExportedRuleNodeData {
        const varTypeControl = this.controls.variableType;
        const codeLineControl = this.controls.codeLine;
        const currentNumInputs = Object.keys(this.inputs).length;
        return {
            id: this.id,
            label: this.label,
            type: "Rule",
            variableType: varTypeControl.value || "",
            codeLine: codeLineControl.value || "",
            numInputs: currentNumInputs,
            numOutputs: 2 // Fixed
        };
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
type MyNodeClasses = SourceNode | RuleNode | LogicGateNode | ActionNode;
type AreaExtra = ReactArea2D<Schemes> | ContextMenuExtra;

// --- createEditor function ---
export async function createEditor(container: HTMLElement) {
    const editor = new NodeEditor<Schemes>();
    const area = new AreaPlugin<Schemes, AreaExtra>(container);
    const connectionPlugin = new ConnectionPlugin<Schemes, AreaExtra>();
    const render = new ReactPlugin<Schemes, AreaExtra>({ createRoot });
    const arrange: any = new AutoArrangePlugin();
    const selector = AreaExtensions.selector();

    AreaExtensions.selectableNodes(area, selector, { accumulating: AreaExtensions.accumulateOnCtrl() });

    const getEditorInstance = () => editor;
    const getAreaInstance = () => area;

    const removeNodeAndConnections = (nodeId: NodeId) => {
        editor.getConnections().filter(c => c.source === nodeId || c.target === nodeId).forEach(c => editor.removeConnection(c.id));
        editor.removeNode(nodeId);
    };

    const menuItems: (readonly [string, (() => SchemeNodeForPlugins) | (() => void)])[] = [
        ["Source Node", () => new SourceNode("Data Source", "filename.csv")],
        // Updated RuleNode factory in menu for new constructor
        ["Rule Node", () => new RuleNode("New Rule", 1, "string", "/* code here */")],
        ["Logic: AND", () => new LogicGateNode(LogicType.AND, 2, getEditorInstance, getAreaInstance)],
        ["Logic: OR", () => new LogicGateNode(LogicType.OR, 2, getEditorInstance, getAreaInstance)],
        ["Action Node", () => new ActionNode("New Action")],
        ["Delete", () => {
            Array.from(selector.entities.keys()).forEach(prefixedId => {
                const actualNodeId = prefixedId.startsWith('node_') ? prefixedId.substring(5) : prefixedId;
                if (editor.getNode(actualNodeId as NodeId)) removeNodeAndConnections(actualNodeId as NodeId);
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

    // Initial nodes
    const source1 = new SourceNode("Pill Data", "pill_data.csv");
    const rule1 = new RuleNode("High Potency Rule", 1, "float", "potency > 0.8"); // Updated constructor
    const rule2 = new RuleNode("Color Rule", 1, "string", "color == 'blue'");  // Updated constructor
    const andGate = new LogicGateNode(LogicType.AND, 2, getEditorInstance, getAreaInstance);
    const action1 = new ActionNode("DISCARD");

    await editor.addNode(source1);
    await editor.addNode(rule1);
    await editor.addNode(rule2);
    await editor.addNode(andGate);
    await editor.addNode(action1);

    // Example connections
    if (source1.outputs['output0'] && rule1.inputs['input0']) {
        await editor.addConnection(new ClassicPreset.Connection(source1, 'output0', rule1, 'input0') as SchemeConnectionForPlugins);
    }
    if (source1.outputs['output0'] && rule2.inputs['input0']) { // Connect source to second rule too
        await editor.addConnection(new ClassicPreset.Connection(source1, 'output0', rule2, 'input0') as SchemeConnectionForPlugins);
    }
    // Connect rule1's TRUE output and rule2's TRUE output to AND gate
    if (rule1.outputs['outputTrue'] && (andGate.inputs as any)['input0' as LogicGateInputKey]) {
        await editor.addConnection(new ClassicPreset.Connection(rule1, 'outputTrue', andGate, 'input0' as LogicGateInputKey) as SchemeConnectionForPlugins);
    }
    if (rule2.outputs['outputTrue'] && (andGate.inputs as any)['input1' as LogicGateInputKey]) {
        await editor.addConnection(new ClassicPreset.Connection(rule2, 'outputTrue', andGate, 'input1' as LogicGateInputKey) as SchemeConnectionForPlugins);
    }
    if (andGate.outputs['output'] && action1.inputs['input0']) {
        await editor.addConnection(new ClassicPreset.Connection(andGate, 'output', action1, 'input0') as SchemeConnectionForPlugins);
    }


    await arrange.layout({ /* ... */ });
    AreaExtensions.zoomAt(area, editor.getNodes());

    (window as any).addEditorNode = async (type: 'Source' | 'Rule' | 'AND' | 'OR' | 'Action', props?: any) => {
        let node;
        const defaultRuleProps = { label: "New Rule", numInputs: 1, variableType: "newVar", codeLine: "condition" };
        const defaultLogicProps = { numInputs: 2 };
        const defaultActionProps = { label: "New Action" };
        const defaultSourceProps = { label: "Data Source", sourceFile: "new_data.csv"};

        switch (type) {
            case 'Source':
                const sourceProps = { ...defaultSourceProps, ...props };
                node = new SourceNode(sourceProps.label, sourceProps.sourceFile);
                break;
            case 'Rule':
                const ruleProps = { ...defaultRuleProps, ...props };
                node = new RuleNode(ruleProps.label, ruleProps.numInputs, ruleProps.variableType, ruleProps.codeLine);
                break;
            case 'AND':
                const andProps = { ...defaultLogicProps, ...props };
                node = new LogicGateNode(LogicType.AND, andProps.numInputs, getEditorInstance, getAreaInstance);
                break;
            case 'OR':
                const orProps = { ...defaultLogicProps, ...props };
                node = new LogicGateNode(LogicType.OR, orProps.numInputs, getEditorInstance, getAreaInstance);
                break;
            case 'Action':
                const actionProps = { ...defaultActionProps, ...props };
                node = new ActionNode(actionProps.label);
                break;
            default: console.error("Unknown node type to add:", type); return;
        }
        if (node) await editor.addNode(node);
    };
    // ... (Rest of window functions and return destroy as before) ...
    (window as any).editorInstance = editor;
    (window as any).areaInstance = area;
    (window as any).arrangeLayout = async () => {
        try {
            await arrange.layout({ zoom: false, getSourceCenter: (n: SchemeNodeForPlugins) => { const actualNode = n as MyNodeClasses; return { x: actualNode.width / 2, y: actualNode.height / 2 }; }, getTargetCenter: (n: SchemeNodeForPlugins) => { const actualNode = n as MyNodeClasses; return { x: actualNode.width / 2, y: actualNode.height / 2 }; } });
            AreaExtensions.zoomAt(area, editor.getNodes());
        } catch (e) { console.error("[App] Error during arrange.layout:", e); }
    };
    (window as any).removeSelectedEditorNodes = async () => {
        const selectedIds = Array.from(selector.entities.keys());
        if (selectedIds.length === 0) return;
        selectedIds.forEach(prefixedId => {
            const actualNodeId = prefixedId.startsWith('node_') ? prefixedId.substring(5) : prefixedId;
            if (editor.getNode(actualNodeId as NodeId)) removeNodeAndConnections(actualNodeId as NodeId);
        });
        selector.unselectAll();
    };
    (window as any).exportWorkflow = () => {
        const nodes = editor.getNodes(); const connections = editor.getConnections();
        const ruleset = generateRuleset(nodes, connections);
        console.log("[Export Workflow] Ruleset generated successfully:", JSON.stringify(ruleset, null, 2));
        alert("Workflow exported to console.");
        return ruleset;
    };

    return {
        destroy: () => {
            area.destroy();
            delete (window as any).exportWorkflow; delete (window as any).editorInstance; delete (window as any).areaInstance;
            delete (window as any).arrangeLayout; delete (window as any).addEditorNode; delete (window as any).removeSelectedEditorNodes;
        }
    };
}

// --- generateRuleset function ---
function generateRuleset(
    nodes: SchemeNodeForPlugins[],
    connections: SchemeConnectionForPlugins[]
): Ruleset {
    const exportedNodeMap = new Map<NodeId, ExportedNode>();
    nodes.forEach(node => {
        const customNode = node as MyNodeClasses;
        let data: ExportedNode | null = null;
        if (customNode instanceof SourceNode) data = customNode.getCustomData();
        else if (customNode instanceof RuleNode) data = customNode.getCustomData();
        else if (customNode instanceof LogicGateNode) data = customNode.getCustomData();
        else if (customNode instanceof ActionNode) data = customNode.getCustomData();
        if (data) exportedNodeMap.set(node.id, data);
        else {
            console.error("[GenerateRuleset] Unknown node type during initial data map:", node.label, node.id);
            exportedNodeMap.set(node.id, { id: node.id, label: node.label || "Unknown", type: "Unknown", source: undefined } as any);
        }
    });
    const connectionsFrom = new Map<NodeId, SchemeConnectionForPlugins[]>();
    connections.forEach(conn => {
        if (!connectionsFrom.has(conn.source)) connectionsFrom.set(conn.source, []);
        connectionsFrom.get(conn.source)!.push(conn);
    });
    const sourceNodesInstances = nodes.filter(n => n instanceof SourceNode) as SourceNode[];
    sourceNodesInstances.forEach(sourceNodeInstance => {
        const sourceNodeData = exportedNodeMap.get(sourceNodeInstance.id) as ExportedSourceNodeData | undefined;
        if (!sourceNodeData || !sourceNodeData.source) return;
        const sourceName = sourceNodeData.source;
        const queue: NodeId[] = [sourceNodeInstance.id];
        const visitedForPropagation = new Set<NodeId>([sourceNodeInstance.id]);
        while (queue.length > 0) {
            const currentId = queue.shift()!;
            const outgoingConnections = connectionsFrom.get(currentId) || [];
            outgoingConnections.forEach(conn => {
                const targetId = conn.target;
                if (!visitedForPropagation.has(targetId)) {
                    visitedForPropagation.add(targetId);
                    queue.push(targetId);
                    const targetNodeExportData = exportedNodeMap.get(targetId);
                    if (targetNodeExportData && !targetNodeExportData.source && targetNodeExportData.type !== "Source") {
                       targetNodeExportData.source = sourceName;
                    }
                }
            });
        }
    });
    return {
        nodes: Array.from(exportedNodeMap.values()),
        connections: connections.map(c => ({
            id: c.id,
            sourceNodeId: c.source, sourceOutputKey: c.sourceOutput as string,
            targetNodeId: c.target, targetInputKey: c.targetInput as string,
        }))
    };
}