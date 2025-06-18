// src/App.tsx
import { useEffect } from "react";
import { useRete } from "rete-react-plugin";
import { createEditor } from "./editor";

export default function App() {
  const [ref] = useRete(createEditor);

  const handleExportWorkflow = () => {
    if ((window as any).exportWorkflow) {
      (window as any).exportWorkflow();
    } else {
      console.error("Export function not available.");
    }
  };

  const handleAddNode = (type: 'Source' | 'Rule' | 'AND' | 'OR' | 'Action') => {
    if ((window as any).addEditorNode) {
      if (type === 'Rule') {
        // RuleNode constructor is (initialLabel, numInputs, initialVariableType, initialCodeLine)
        (window as any).addEditorNode(type, {
          label: "New Rule",
          numInputs: 1, // Default inputs
          variableType: "string",
          codeLine: "/* condition */"
        });
      } else if (type === 'AND') {
        (window as any).addEditorNode(type, { numInputs: 2 });
      } else if (type === 'OR') {
        (window as any).addEditorNode(type, { numInputs: 2 });
      } else if (type === 'Action') {
        (window as any).addEditorNode(type, { label: "New Action" });
      } else if (type === 'Source') {
        (window as any).addEditorNode(type, { label: "New Source", sourceFile: "data.csv" });
      }
    } else {
      console.error("Add node function not available.");
    }
  };

  const handleRemoveSelectedNodes = () => {
    if ((window as any).removeSelectedEditorNodes) {
      (window as any).removeSelectedEditorNodes();
    } else {
      console.error("Remove selected nodes function not available.");
    }
  };

  const handleArrange = () => {
    if ((window as any).arrangeLayout) {
      (window as any).arrangeLayout();
    } else {
      console.error("Arrange function not available.");
    }
  }

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Delete' || event.key === 'Backspace') {
        if (document.activeElement && ['INPUT', 'TEXTAREA'].includes(document.activeElement.tagName.toUpperCase())) {
            return;
        }
        handleRemoveSelectedNodes();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, []);

  return (
    <div className="App">
      <div style={{ padding: "10px", background: "#f0f0f0", borderBottom: "1px solid #ccc", display: "flex", gap: "10px", flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ fontWeight: "bold", marginRight: "5px" }}>Add Nodes:</div>
        <button onClick={() => handleAddNode('Source')} style={{ backgroundColor: "#e0e0e0", border: "1px solid #b0b0b0", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>Add Source</button>
        <button onClick={() => handleAddNode('Rule')} style={{ backgroundColor: "#cfe2f3", border: "1px solid #a2c4c9", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>Add Rule</button>
        <button onClick={() => handleAddNode('AND')} style={{ backgroundColor: "#d9ead3", border: "1px solid #b6d7a8", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>Add AND</button>
        <button onClick={() => handleAddNode('OR')} style={{ backgroundColor: "#d9ead3", border: "1px solid #b6d7a8", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>Add OR</button>
        <button onClick={() => handleAddNode('Action')} style={{ backgroundColor: "#fce5cd", border: "1px solid #f4c795", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>Add Action</button>

        <div style={{ marginLeft: "15px", fontWeight: "bold", marginRight: "5px" }}>Editor Actions:</div>
        <button onClick={handleRemoveSelectedNodes} style={{ backgroundColor: "#ffdddd", border: "1px solid #ffb3b3", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>
          Delete Selected
        </button>
        <button onClick={handleExportWorkflow} style={{ backgroundColor: "#e6e6e6", border: "1px solid #cccccc", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>
          Export Workflow
        </button>
        <button onClick={handleArrange} style={{ backgroundColor: "#e6e6e6", border: "1px solid #cccccc", padding: "5px 10px", borderRadius: "4px", cursor: "pointer" }}>
          Arrange Layout
        </button>
      </div>
      <div ref={ref} style={{ height: "calc(100vh - 70px)", width: "100vw", backgroundColor: "#ddd" }}></div>
    </div>
  );
}