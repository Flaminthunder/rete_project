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
interface BaseExportedNodeData { id: string; label: string; type: string; source?: string; } // Added optional source
interface ExportedRuleNodeData extends BaseExportedNodeData { // `label` will be the node's title from super()
    type: "Rule";
    variableType: string;
    codeLine: string;
    numInputs: number;
    numOutputs: number;
}
interface ExportedActionNodeData extends BaseExportedNodeData { type: "Action"; numInputs: 1; }
enum LogicType { AND = "AND", OR = "OR" }
interface ExportedLogicGateNodeData extends BaseExportedNodeData { type: LogicType; numInputs: number; }
interface ExportedSourceNodeData extends BaseExportedNodeData {
    type: "Source";
    source: string; // The filename is the primary data for this node
    numOutputs: 1;
}

type ExportedNode = ExportedRuleNodeData | ExportedLogicGateNodeData | ExportedActionNodeData | ExportedSourceNodeData;
interface ExportedConnectionData { id: string; sourceNodeId: string; sourceOutputKey: string; targetNodeId: string; targetInputKey: string; }
interface Ruleset { nodes: ExportedNode[]; connections: ExportedConnectionData[]; }

// --- Node Sizing Constants ---
const SOURCE_NODE_TARGET_WIDTH = 220;
const SOURCE_NODE_TARGET_HEIGHT = 120;
const RULE_NODE_TARGET_WIDTH = 200;
const RULE_NODE_TARGET_HEIGHT = 160;
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
            source: sourceFileControl.value || "", // The filename is the source
            numOutputs: 1,
        };
    }
}

class RuleNode extends ClassicPreset.Node<
    { [key: `input${number}`]: ClassicPreset.Socket },
    { [key: `output${number}`]: ClassicPreset.Socket },
    {
        variableType: ClassicPreset.InputControl<"text">;
        codeLine: ClassicPreset.InputControl<"text">;
    }
> {
    width: number;
    height: number;
    constructor(
        initialLabel: string = "Rule",
        numInputs: number = 1,
        numOutputs: number = 1,
        initialVariableType: string = "string",
        initialCodeLine: string = "variable == value"
    ) {
        super(initialLabel);
        this.width = RULE_NODE_TARGET_WIDTH;
        this.height = RULE_NODE_TARGET_HEIGHT;
        this.addControl("variableType", new ClassicPreset.InputControl("text", { initial: initialVariableType }));
        this.addControl("codeLine", new ClassicPreset.InputControl("text", { initial: initialCodeLine }));
        const socketInstance = new ClassicPreset.Socket("socket");
        for (let i = 0; i < numInputs; i++) this.addInput(`input${i}`, new ClassicPreset.Input(socketInstance, `In ${i + 1}`));
        for (let i = 0; i < numOutputs; i++) this.addOutput(`output${i}`, new ClassicPreset.Output(socketInstance, `Out ${i + 1}`));
    }
    getCustomData(): ExportedRuleNodeData {
        return {
            id: this.id,
            label: this.label,
            type: "Rule",
            variableType: this.controls.variableType.value || "",
            codeLine: this.controls.codeLine.value || "",
            numInputs: Object.keys(this.inputs).length,
            numOutputs: Object.keys(this.outputs).length
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
                const desiredNumInputs = Math.max(minInputs, Math.floor(value as number));
                if (this.controls.numInputs?.value !== desiredNumInputs) this.updateInputSockets(desiredNumInputs);
                if (this.controls.numInputs && this.controls.numInputs.value !== desiredNumInputs) this.controls.numInputs.setValue(desiredNumInputs);
            }
        }));
        this.addOutput("output", new ClassicPreset.Output(new ClassicPreset.Socket("socket"), "Out"));
        this.updateInputSockets(Math.max(minInputs, initialNumInputs));
    }
    private updateInputSockets(newCount: number) {
        const currentCount = Object.keys(this.inputs).length;
        if (newCount === currentCount) { this._recalculateDimensions(); return; }
        const socketInstance = new ClassicPreset.Socket("socket");
        if (newCount > currentCount) {
            for (let i = currentCount; i < newCount; i++) this.addInput(`input${i}`, new ClassicPreset.Input(socketInstance, `In ${i + 1}`));
        } else {
            for (let i = currentCount - 1; i >= newCount; i--) {
                const inputKeyToRemove = `input${i}` as LogicGateInputKey;
                if (this.editorRef?.getConnections().some(c => c.target === this.id && c.targetInput === inputKeyToRemove)) {
                    this.editorRef.getConnections().filter(c => c.target === this.id && c.targetInput === inputKeyToRemove).forEach(c => this.editorRef?.removeConnection(c.id));
                }
                this.removeInput(inputKeyToRemove);
            }
        }
        this._recalculateDimensions();
    }
    private _recalculateDimensions() {
        const numInputs = Object.keys(this.inputs).length;
        this.height = LOGIC_NODE_BASE_HEIGHT_FOR_MIN_INPUTS + Math.max(0, numInputs - LOGIC_NODE_BASE_INPUT_COUNT) * LOGIC_NODE_HEIGHT_PER_ADDITIONAL_INPUT;
        this.areaRef?.update("node", this.id);
    }
    getCustomData(): ExportedLogicGateNodeData {
        return { id: this.id, label: this.label, type: this.nodeLogicType, numInputs: Object.keys(this.inputs).length };
    }
}

class ActionNode extends ClassicPreset.Node<
    { input0: ClassicPreset.Socket }, {}, { label: ClassicPreset.InputControl<"text"> }
> {
    width: number; height: number;
    constructor(initialLabel: string = "Action") {
        super(initialLabel);
        this.width = ACTION_NODE_TARGET_WIDTH; this.height = ACTION_NODE_TARGET_HEIGHT;
        this.addControl("label", new ClassicPreset.InputControl("text", { initial: initialLabel }));
        this.addInput("input0", new ClassicPreset.Input(new ClassicPreset.Socket("socket"), "In"));
    }
    getCustomData(): ExportedActionNodeData {
        return { id: this.id, label: this.controls.label.value || "Unnamed Action", type: "Action", numInputs: 1 };
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

    const menuItems = [
        ["Source Node", () => new SourceNode("Data Source", "filename.csv")],
        ["Rule Node", () => new RuleNode("New Rule", 1, 1, "string", "/* code here */")],
        ["Logic: AND", () => new LogicGateNode(LogicType.AND, 2, getEditorInstance, getAreaInstance)],
        ["Logic: OR", () => new LogicGateNode(LogicType.OR, 2, getEditorInstance, getAreaInstance)],
        ["Action Node", () => new ActionNode("New Action")],
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
    const rule1 = new RuleNode("High Potency", 1, 1, "float", "potency > 0.8");
    const action1 = new ActionNode("DISCARD");

    await editor.addNode(source1);
    await editor.addNode(rule1);
    await editor.addNode(action1);

    await editor.addConnection(new ClassicPreset.Connection(source1, 'output0', rule1, 'input0') as SchemeConnectionForPlugins);
    await editor.addConnection(new ClassicPreset.Connection(rule1, 'output0', action1, 'input0') as SchemeConnectionForPlugins);

    await arrange.layout();
    AreaExtensions.zoomAt(area, editor.getNodes());

    // --- Window functions for App.tsx ---
    (window as any).addEditorNode = async (type: 'Rule' | 'AND' | 'OR' | 'Action' | 'Source', props?: any) => {
        let node;
        switch (type) {
            case 'Source': node = new SourceNode(props.label, props.sourceFile); break;
            case 'Rule': node = new RuleNode(props.label, props.numInputs, props.numOutputs); break;
            case 'AND': node = new LogicGateNode(LogicType.AND, props.numInputs, getEditorInstance, getAreaInstance); break;
            case 'OR': node = new LogicGateNode(LogicType.OR, props.numInputs, getEditorInstance, getAreaInstance); break;
            case 'Action': node = new ActionNode(props.label); break;
            default: return;
        }
        await editor.addNode(node);
    };
    
    (window as any).arrangeLayout = async () => await arrange.layout();
    
    (window as any).removeSelectedEditorNodes = () => {
        selector.entities.forEach(e => removeNodeAndConnections(e.id as NodeId));
        selector.unselectAll();
    };
    
    (window as any).exportWorkflow = () => {
        const ruleset = generateRuleset(editor.getNodes(), editor.getConnections());
        console.log("[Export Workflow] Ruleset generated successfully:", JSON.stringify(ruleset, null, 2));
        alert("Workflow exported to console.");
        return ruleset;
    };

    return {
        destroy: () => {
            area.destroy();
            ['exportWorkflow', 'arrangeLayout', 'addEditorNode', 'removeSelectedEditorNodes'].forEach(name => delete (window as any)[name]);
        }
    };
}

// --- generateRuleset function (Updated for source propagation) ---
function generateRuleset(
    nodes: SchemeNodeForPlugins[],
    connections: SchemeConnectionForPlugins[]
): Ruleset {
    const connectionsFrom = new Map<NodeId, SchemeConnectionForPlugins[]>();
    connections.forEach(conn => {
        if (!connectionsFrom.has(conn.source)) connectionsFrom.set(conn.source, []);
        connectionsFrom.get(conn.source)!.push(conn);
    });

    const exportedNodeMap = new Map<NodeId, ExportedNode>();
    nodes.forEach(node => {
        const customNode = node as MyNodeClasses;
        let data: ExportedNode | null = null;
        if (customNode instanceof SourceNode) data = customNode.getCustomData();
        else if (customNode instanceof RuleNode) data = customNode.getCustomData();
        else if (customNode instanceof LogicGateNode) data = customNode.getCustomData();
        else if (customNode instanceof ActionNode) data = customNode.getCustomData();
        
        if (data) exportedNodeMap.set(node.id, data);
    });

    const sourceNodes = nodes.filter(n => n instanceof SourceNode) as SourceNode[];
    sourceNodes.forEach(sourceNodeInstance => {
        const sourceName = (exportedNodeMap.get(sourceNodeInstance.id) as ExportedSourceNodeData).source;
        const queue: NodeId[] = [sourceNodeInstance.id];
        const visited = new Set<NodeId>([sourceNodeInstance.id]);
        
        while(queue.length > 0) {
            const currentId = queue.shift()!;
            const outgoingConnections = connectionsFrom.get(currentId) || [];
            
            outgoingConnections.forEach(conn => {
                const targetId = conn.target;
                if (!visited.has(targetId)) {
                    visited.add(targetId);
                    queue.push(targetId);
                    const targetNodeData = exportedNodeMap.get(targetId);
                    if (targetNodeData && !targetNodeData.source) {
                       targetNodeData.source = sourceName;
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