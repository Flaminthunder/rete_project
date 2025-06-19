// src/App.tsx

import { useEffect, useState } from "react";
import { useRete } from "rete-react-plugin";
import { createEditor } from "./editor";
import './styles.css'; // Import the unified stylesheet

export default function App() {
  const [ref] = useRete(createEditor);
  const [isLoading, setIsLoading] = useState(false);
  const [defaultAction, setDefaultAction] = useState('ACCEPT');

  // This is the core function that connects the frontend to the backend
  const handleProcessWorkflow = async () => {
    setIsLoading(true); // Show a loading indicator on the button

    if (!(window as any).exportWorkflow) {
      alert("Editor is not ready. Please wait a moment and try again.");
      setIsLoading(false);
      return;
    }
    
    // 1. Get the workflow JSON from the editor
    const ruleset = (window as any).exportWorkflow();
    // 2. Combine it with the selected default action into a single payload
    const payload = { ...ruleset, defaultAction };

    try {
      // 3. Send the payload to the Flask API endpoint
      const response = await fetch('/process_workflow', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const result = await response.json();

      // 4. Handle errors returned from the backend (e.g., bad workflow, file not found)
      if (!response.ok) {
        throw new Error(result.error || `HTTP error! Status: ${response.status}`);
      }

      // 5. On success, redirect the browser to the results page URL provided by Flask
      if (result.results_page_url) {
        window.location.href = result.results_page_url;
      } else {
        throw new Error("Processing succeeded, but the server did not provide a results page URL.");
      }

    } catch (error) {
      // 6. Handle network errors or other exceptions
      console.error("Failed to process workflow:", error);
      alert(`An error occurred: ${error instanceof Error ? error.message : String(error)}`);
      setIsLoading(false); // Reset loading state on error
    }
  };

  // Helper functions to interact with the editor
  const handleAddNode = (type: 'Source' | 'Rule' | 'AND' | 'OR' | 'Action') => {
    if ((window as any).addEditorNode) (window as any).addEditorNode(type);
  };
  const handleRemoveSelectedNodes = () => {
    if ((window as any).removeSelectedEditorNodes) (window as any).removeSelectedEditorNodes();
  };
  const handleArrange = () => {
    if ((window as any).arrangeLayout) (window as any).arrangeLayout();
  };

  // Keyboard shortcut for deleting nodes
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Delete' || event.key === 'Backspace') {
        const activeEl = document.activeElement?.tagName.toUpperCase();
        if (activeEl === 'INPUT' || activeEl === 'TEXTAREA' || activeEl === 'SELECT') return;
        handleRemoveSelectedNodes();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []);

  return (
    <div className="App">
      <div className="toolbar">
        <div className="toolbar-group">
          <span className="toolbar-label">Add Node:</span>
          <button onClick={() => handleAddNode('Source')}>Source</button>
          <button onClick={() => handleAddNode('Rule')}>Rule</button>
          <button onClick={() => handleAddNode('AND')}>AND</button>
          <button onClick={() => handleAddNode('OR')}>OR</button>
          <button onClick={() => handleAddNode('Action')}>Action</button>
        </div>
        <div className="toolbar-group">
          <span className="toolbar-label">Editor:</span>
          <button onClick={handleArrange}>Arrange</button>
          <button onClick={handleRemoveSelectedNodes} className="danger">Delete</button>
        </div>
        <div className="toolbar-group process-group">
          <span className="toolbar-label">Default Action:</span>
          <select value={defaultAction} onChange={(e) => setDefaultAction(e.target.value)} disabled={isLoading}>
            <option value="ACCEPT">ACCEPT</option>
            <option value="DISCARD">DISCARD</option>
          </select>
          <button onClick={handleProcessWorkflow} disabled={isLoading} className="primary">
            {isLoading ? 'Processing...' : 'Process Workflow'}
          </button>
        </div>
      </div>
      <div ref={ref} className="editor-container"></div>
    </div>
  );
}