// src/App.tsx
import { useEffect } from "react"; // Import useEffect
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

  const handleAddNode = (type: 'Rule' | 'AND' | 'OR') => {
    if ((window as any).addEditorNode) {
      if (type === 'Rule') {
        (window as any).addEditorNode(type, { label: "New Rule", numInputs: 1, numOutputs: 1 });
      } else if (type === 'AND') {
        (window as any).addEditorNode(type, { numInputs: 2 });
      } else if (type === 'OR') {
        (window as any).addEditorNode(type, { numInputs: 2 });
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
  }, []); // Empty dependency array to run once on mount and clean up on unmount

  return (
    <div className="App">
      <div style={{ padding: "10px", background: "#f0f0f0", borderBottom: "1px solid #ccc", display: "flex", gap: "10px", flexWrap: "wrap" }}>
        <button onClick={() => handleAddNode('Rule')}>Add Rule Node</button>
        <button onClick={() => handleAddNode('AND')}>Add AND Gate</button>
        <button onClick={() => handleAddNode('OR')}>Add OR Gate</button>
        <button onClick={handleRemoveSelectedNodes} style={{ backgroundColor: "#ffdddd" }}>
          Delete Selected
        </button>
        <button onClick={handleExportWorkflow}>
          Export Workflow
        </button>
        <button onClick={handleArrange}>
          Arrange Layout
        </button>
      </div>
      {/* Ensure the ref is applied and the div has a size */}
      <div ref={ref} style={{ height: "calc(100vh - 70px)", width: "100vw", backgroundColor: "#ddd" }}></div>
    </div>
  );
}