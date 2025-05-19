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
interface BaseExportedNodeData { id: string; }
interface ExportedRuleNodeData extends BaseExportedNodeData { label: string; type: "Rule"; numInputs: number; numOutputs: number; }
interface ExportedActionNodeData extends BaseExportedNodeData { label: string; type: "Action"; numInputs: number; }
enum LogicType { AND = "AND", OR = "OR" }
interface ExportedLogicGateNodeData extends BaseExportedNodeData { type: LogicType; numInputs: number; }

type ExportedNode = ExportedRuleNodeData | ExportedLogicGateNodeData | ExportedActionNodeData;
interface ExportedConnectionData { id: string; sourceNodeId: string; sourceOutputKey: string; targetNodeId: string; targetInputKey: string; }
interface Ruleset { nodes: ExportedNode[]; connections: ExportedConnectionData[]; }

// --- Node Implementations ---
class RuleNode extends ClassicPreset.Node<
    { [key: `input${number}`]: ClassicPreset.Socket },
    { [key: `output${number}`]: ClassicPreset.Socket },
    { label: ClassicPreset.InputControl<"text"> }
> {
    width = 180;
    height = 120;
    public numInputsProp: number;
    public numOutputsProp: number;

    constructor(
        initialLabel: string = "Rule",
        numInputs: number = 1,
        numOutputs: number = 1
    ) {
        super(initialLabel);
        this.numInputsProp = numInputs;
        this.numOutputsProp = numOutputs;

        this.addControl(
            "label",
            new ClassicPreset.InputControl("text", { initial: initialLabel, readonly: false })
        );
        const socketInstance = new ClassicPreset.Socket("socket");
        for (let i = 0; i < this.numInputsProp; i++) {
            this.addInput(`input${i}`, new ClassicPreset.Input(socketInstance, `In ${i + 1}`));
        }
        for (let i = 0; i < this.numOutputsProp; i++) {
            this.addOutput(`output${i}`, new ClassicPreset.Output(socketInstance, `Out ${i + 1}`));
        }
        this._updateDimensions(this.numInputsProp, this.numOutputsProp);
    }

    private _updateDimensions(numInputs: number, numOutputs: number) {
        const titleHeight = 30;
        const labelControlHeight = 35;
        const socketHeight = 25;
        const padding = 10;
        const minBaseHeightWithoutSockets = titleHeight + labelControlHeight + padding;

        this.height = minBaseHeightWithoutSockets + Math.max(numInputs, numOutputs) * socketHeight;
        this.height = Math.max(this.height, 120);
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
    width = 180;
    height = 120;
    public readonly nodeLogicType: LogicType;
    private editorRef: NodeEditor<Schemes> | null = null;
    private areaRef: AreaPlugin<Schemes, AreaExtra> | null = null;

    constructor(
        logicType: LogicType,
        initialNumInputs: number = 2,
        getEditor?: () => NodeEditor<Schemes>,
        getArea?: () => AreaPlugin<Schemes, AreaExtra>
    ) {
        super(logicType.toString());
        this.nodeLogicType = logicType;
        if (getEditor) this.editorRef = getEditor();
        if (getArea) this.areaRef = getArea();

        const minInputs = 2; // AND/OR gates need at least 2 inputs

        this.addControl(
            "numInputs",
            new ClassicPreset.InputControl("number", {
                initial: Math.max(minInputs, initialNumInputs),
                change: (value) => {
                    const currentControlDisplayValue = this.controls.numInputs?.value;
                    const desiredNumInputs = Math.max(minInputs, Math.floor(value as number));
                    const currentSocketCount = Object.keys(this.inputs).length;

                    if (desiredNumInputs !== currentSocketCount) {
                        this.updateInputSockets(desiredNumInputs);
                    }

                    if (this.controls.numInputs &&
                        (currentControlDisplayValue !== desiredNumInputs || (value as number) !== desiredNumInputs)
                    ) {
                        this.controls.numInputs.setValue(desiredNumInputs);
                    }
                    this.areaRef?.update("node", this.id);
                }
            })
        );

        this.addOutput("output", new ClassicPreset.Output(new ClassicPreset.Socket("socket"), "Out"));
        this.updateInputSockets(Math.max(minInputs, initialNumInputs));
    }

    private updateInputSockets(newCount: number) {
        const currentInputs = this.inputs as { [key in LogicGateInputKey]?: ClassicPreset.Input<ClassicPreset.Socket> };
        const currentInputKeys = Object.keys(currentInputs) as LogicGateInputKey[];
        const currentCount = currentInputKeys.length;
        const socketInstance = new ClassicPreset.Socket("socket");

        if (newCount === currentCount) {
            this._recalculateDimensions();
            return;
        }

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
                        if (conn.target === this.id && conn.targetInput === inputKeyToRemove) {
                            this.editorRef?.removeConnection(conn.id);
                        }
                    });
                }
                if (currentInputs[inputKeyToRemove]) {
                    this.removeInput(inputKeyToRemove);
                }
            }
        }
        this._recalculateDimensions();
    }

    private _recalculateDimensions() {
        const numInputs = Object.keys(this.inputs).length;
        const titleHeight = 30;
        const numInputsControlHeight = 35;
        const socketHeight = 25;
        const padding = 10;
        const outputSocketHeight = 25;

        this.height = titleHeight + numInputsControlHeight + (numInputs * socketHeight) + outputSocketHeight + padding;
        this.height = Math.max(this.height, 120);
    }

    getCustomData(): ExportedLogicGateNodeData {
        const numInputs = Object.keys(this.inputs).length;
        return { id: this.id, type: this.nodeLogicType, numInputs };
    }
}

class ActionNode extends ClassicPreset.Node<
    { [key: `input${number}`]: ClassicPreset.Socket }, // Inputs
    {}, // No Outputs
    { label: ClassicPreset.InputControl<"text">; numInputs: ClassicPreset.InputControl<"number"> } // Controls
> {
    width = 180;
    height = 150; // Initial height, will be recalculated
    private editorRef: NodeEditor<Schemes> | null = null;
    private areaRef: AreaPlugin<Schemes, AreaExtra> | null = null;

    constructor(
        initialLabel: string = "Action",
        initialNumInputs: number = 1,
        getEditor?: () => NodeEditor<Schemes>,
        getArea?: () => AreaPlugin<Schemes, AreaExtra>
    ) {
        super(initialLabel);
        if (getEditor) this.editorRef = getEditor();
        if (getArea) this.areaRef = getArea();

        this.addControl(
            "label",
            new ClassicPreset.InputControl("text", { initial: initialLabel, readonly: false })
        );

        const minInputs = 1; // Action node needs at least one input

        this.addControl(
            "numInputs",
            new ClassicPreset.InputControl("number", {
                initial: Math.max(minInputs, initialNumInputs),
                change: (value) => {
                    const currentControlDisplayValue = this.controls.numInputs?.value;
                    const desiredNumInputs = Math.max(minInputs, Math.floor(value as number));
                    const currentSocketCount = Object.keys(this.inputs).length;

                    if (desiredNumInputs !== currentSocketCount) {
                        this.updateInputSockets(desiredNumInputs);
                    }

                    if (this.controls.numInputs &&
                        (currentControlDisplayValue !== desiredNumInputs || (value as number) !== desiredNumInputs)
                    ) {
                        this.controls.numInputs.setValue(desiredNumInputs);
                    }
                    this.areaRef?.update("node", this.id);
                }
            })
        );
        this.updateInputSockets(Math.max(minInputs, initialNumInputs));
    }

    private updateInputSockets(newCount: number) {
        const currentInputs = this.inputs as { [key: `input${number}`]: ClassicPreset.Input<ClassicPreset.Socket> };
        const currentInputKeys = Object.keys(currentInputs) as `input${number}`[];
        const currentCount = currentInputKeys.length;
        const socketInstance = new ClassicPreset.Socket("socket");

        if (newCount === currentCount) {
            this._recalculateDimensions();
            return;
        }

        if (newCount > currentCount) {
            for (let i = currentCount; i < newCount; i++) {
                const newInputKey = `input${i}` as `input${number}`;
                this.addInput(newInputKey, new ClassicPreset.Input(socketInstance, `Param ${i + 1}`));
            }
        } else if (newCount < currentCount) {
            for (let i = currentCount - 1; i >= newCount; i--) {
                const inputKeyToRemove = `input${i}` as `input${number}`;
                if (this.editorRef && currentInputs[inputKeyToRemove]) {
                    const connections = this.editorRef.getConnections();
                    connections.forEach(conn => {
                        if (conn.target === this.id && conn.targetInput === inputKeyToRemove) {
                            this.editorRef?.removeConnection(conn.id);
                        }
                    });
                }
                if (currentInputs[inputKeyToRemove]) {
                    this.removeInput(inputKeyToRemove);
                }
            }
        }
        this._recalculateDimensions();
    }

    private _recalculateDimensions() {
        const numInputs = Object.keys(this.inputs).length;
        const titleHeight = 30;
        const labelControlHeight = 35;
        const numInputsControlHeight = 35;
        const socketHeight = 25;
        const padding = 10;

        this.height = titleHeight + labelControlHeight + numInputsControlHeight + (numInputs * socketHeight) + padding;
        this.height = Math.max(this.height, 150);
    }

    getCustomData(): ExportedActionNodeData {
        const labelControl = this.controls.label;
        const numInputs = Object.keys(this.inputs).length;
        return {
            id: this.id,
            label: labelControl.value || "Unnamed Action",
            type: "Action",
            numInputs
        };
    }
}


// --- Define Schemes TO MATCH the `ClassicScheme` from `rete-react-plugin` ---
type SchemeNodeForPlugins = ClassicPreset.Node;
type SchemeConnectionForPlugins = ClassicPreset.Connection<SchemeNodeForPlugins, SchemeNodeForPlugins> & { isLoop?: boolean };
type Schemes = GetSchemes<SchemeNodeForPlugins, SchemeConnectionForPlugins>;

// Union of your specific node class types for your own logic and casting
type MyNodeClasses = RuleNode | LogicGateNode | ActionNode;

type AreaExtra = ReactArea2D<Schemes> | ContextMenuExtra;


export async function createEditor(container: HTMLElement) {
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
        const connectionsToRemove = editor.getConnections().filter(conn =>
            conn.source === nodeIdToRemove || conn.target === nodeIdToRemove
        );
        connectionsToRemove.forEach(conn => {
            editor.removeConnection(conn.id);
        });
        editor.removeNode(nodeIdToRemove);
    };

    const menuItems: (readonly [string, (() => SchemeNodeForPlugins) | (() => void)])[] = [
        ["Rule Node (1 In, 1 Out)", () => new RuleNode("New Rule", 1, 1)],
        ["Rule Node (2 In, 1 Out)", () => new RuleNode("Decision Rule", 2, 1)],
        ["Logic: AND", () => new LogicGateNode(LogicType.AND, 2, getEditorInstance, getAreaInstance)],
        ["Logic: OR", () => new LogicGateNode(LogicType.OR, 2, getEditorInstance, getAreaInstance)],
        ["Action Node", () => new ActionNode("New Action", 1, getEditorInstance, getAreaInstance)],
        ["Delete", () => {
            const selectedIds = Array.from(selector.entities.keys());
            if (selectedIds.length === 0) return;
            selectedIds.forEach(prefixedId => {
                const actualNodeId = prefixedId.startsWith('node_') ? prefixedId.substring(5) : prefixedId;
                if (editor.getNode(actualNodeId as NodeId)) {
                    removeNodeAndConnections(actualNodeId as NodeId);
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
    render.addPreset(ReactPresets.classic.setup({
        // You can customize node rendering here if needed, for example, to style ActionNode differently
        // node: (props) => {
        //   if (props.data instanceof ActionNode) {
        //     // return custom JSX for ActionNode
        //   }
        //   return ReactPresets.classic.Node(props); // Default classic node
        // }
    }));
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
    const actionNode1 = new ActionNode("Perform Task", 1, getEditorInstance, getAreaInstance);

    await editor.addNode(rule1);
    await editor.addNode(rule2);
    await editor.addNode(andGate);
    await editor.addNode(resultRule);
    await editor.addNode(actionNode1);

    if (rule1.outputs['output0'] && (andGate.inputs as any)['input0' as LogicGateInputKey]) {
        await editor.addConnection(new ClassicPreset.Connection(rule1, 'output0', andGate, 'input0' as LogicGateInputKey) as SchemeConnectionForPlugins);
    }
    if (rule2.outputs['output0'] && (andGate.inputs as any)['input1' as LogicGateInputKey]) {
        await editor.addConnection(new ClassicPreset.Connection(rule2, 'output0', andGate, 'input1' as LogicGateInputKey) as SchemeConnectionForPlugins);
    }
    if (andGate.outputs['output'] && resultRule.inputs['input0']) {
        await editor.addConnection(new ClassicPreset.Connection(andGate, 'output', resultRule, 'input0') as SchemeConnectionForPlugins);
    }
    if (resultRule.outputs['output0'] && (actionNode1.inputs as any)['input0']) {
        await editor.addConnection(new ClassicPreset.Connection(resultRule, 'output0', actionNode1, 'input0') as SchemeConnectionForPlugins);
    }

    await arrange.layout({
        zoom: false,
        getSourceCenter: (n: SchemeNodeForPlugins) => {
            const actualNode = n as MyNodeClasses;
            return { x: actualNode.width / 2, y: actualNode.height / 2 };
        },
        getTargetCenter: (n: SchemeNodeForPlugins) => {
            const actualNode = n as MyNodeClasses;
            return { x: actualNode.width / 2, y: actualNode.height / 2 };
        }
    });
    AreaExtensions.zoomAt(area, editor.getNodes());

    (window as any).editorInstance = editor;
    (window as any).areaInstance = area;

    (window as any).arrangeLayout = async () => {
        await arrange.layout({
            zoom: false,
            getSourceCenter: (n: SchemeNodeForPlugins) => { const actualNode = n as MyNodeClasses; return { x: actualNode.width / 2, y: actualNode.height / 2 }; },
            getTargetCenter: (n: SchemeNodeForPlugins) => { const actualNode = n as MyNodeClasses; return { x: actualNode.width / 2, y: actualNode.height / 2 }; }
        });
        AreaExtensions.zoomAt(area, editor.getNodes());
    };

    (window as any).addEditorNode = async (type: 'Rule' | 'AND' | 'OR' | 'Action', props?: any) => {
        let nodeToAdd;
        const defaultProps = { label: "New Node", numInputs: 1, numOutputs: 1, ...props };
        switch (type) {
            case 'Rule': nodeToAdd = new RuleNode(defaultProps.label, defaultProps.numInputs, defaultProps.numOutputs); break;
            case 'AND': nodeToAdd = new LogicGateNode(LogicType.AND, defaultProps.numInputs || 2, getEditorInstance, getAreaInstance); break;
            case 'OR': nodeToAdd = new LogicGateNode(LogicType.OR, defaultProps.numInputs || 2, getEditorInstance, getAreaInstance); break;
            case 'Action': nodeToAdd = new ActionNode(defaultProps.label || "New Action", defaultProps.numInputs || 1, getEditorInstance, getAreaInstance); break;
            default: console.error("Unknown node type to add:", type); return;
        }
        if (nodeToAdd) {
            await editor.addNode(nodeToAdd);
            console.log(`Node ${nodeToAdd.id} added. Current editor nodes:`, editor.getNodes().map(n=>n.id));
        }
    };

    (window as any).removeSelectedEditorNodes = async () => {
        const selectedIds = Array.from(selector.entities.keys());
        if (selectedIds.length === 0) return;
        selectedIds.forEach(prefixedId => {
            const actualNodeId = prefixedId.startsWith('node_') ? prefixedId.substring(5) : prefixedId;
            if (editor.getNode(actualNodeId as NodeId)) {
                removeNodeAndConnections(actualNodeId as NodeId);
            }
        });
        selector.unselectAll();
    };

    (window as any).exportWorkflow = () => {
        const nodes = editor.getNodes();
        const connections = editor.getConnections();
        const ruleset = generateRuleset(nodes, connections);
        console.log("Exported Workflow Rules:", JSON.stringify(ruleset, null, 2));
        alert("Workflow exported to console. Check the developer console (F12).");
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

function generateRuleset(
    nodes: SchemeNodeForPlugins[],
    connections: SchemeConnectionForPlugins[]
): Ruleset {
    const exportedNodes: ExportedNode[] = nodes.map(node => {
        const customNode = node as MyNodeClasses; // Cast to your union type
        if (customNode instanceof RuleNode) {
            return customNode.getCustomData();
        } else if (customNode instanceof LogicGateNode) {
            return customNode.getCustomData();
        } else if (customNode instanceof ActionNode) {
            return customNode.getCustomData();
        }
        console.error("Unknown node type in generateRuleset:", node.label, node.id);
        // Fallback for unknown types, though ideally this shouldn't happen
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