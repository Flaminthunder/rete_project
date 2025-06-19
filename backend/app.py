from flask import Flask, request, jsonify, render_template, send_from_directory, url_for
import json
import os
import logging
from pharma_automation import run_workflow_processing

# --- Flask App Initialization ---
# Best practice: configure static folder for the React build and template folder for results.
app = Flask(__name__,
            static_folder=os.path.join(os.path.dirname(__file__), '../build/static'),
            template_folder='templates')

# --- Configuration & Setup ---
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV_PATH = os.path.join(APP_ROOT, "pill_data.csv")
PROCESSED_OUTPUT_DIR = os.path.join(APP_ROOT, "processed_output")
REACT_BUILD_DIR = os.path.join(APP_ROOT, '../build')

# Ensure the output directory exists on startup
os.makedirs(PROCESSED_OUTPUT_DIR, exist_ok=True)

# Configure logging to be more informative than print()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# --- Static File Serving for React App ---
@app.route('/')
def index():
    """Serves the main index.html file of the React application."""
    app.logger.info(f"Serving index.html from: {REACT_BUILD_DIR}")
    return send_from_directory(REACT_BUILD_DIR, 'index.html')

@app.route('/<path:path>')
def serve_react_static_files(path):
    """Serves other static files from the React build directory (e.g., manifest, favicon)."""
    if os.path.exists(os.path.join(REACT_BUILD_DIR, path)):
        return send_from_directory(REACT_BUILD_DIR, path)
    else:
        # This primarily handles requests for /static/js, /static/css, which Flask
        # routes to the `static_folder` defined in the constructor.
        # This is a fallback for any other file.
        app.logger.warning(f"Static file not found directly, falling back to static folder for path: {path}")
        return send_from_directory(app.static_folder, path)


# --- API Endpoint for Workflow Processing ---
@app.route('/process_workflow', methods=['POST'])
def process_workflow_route():
    """
    Receives a workflow JSON, processes it using the automation engine,
    and returns the results.
    """
    app.logger.info("Received POST request at /process_workflow")
    if not request.is_json:
        app.logger.error("Request failed: Content-Type is not application/json")
        return jsonify({"error": "Request must be JSON"}), 400

    workflow_data = request.get_json()
    if not workflow_data or "nodes" not in workflow_data:
        app.logger.error("Request failed: No valid workflow data in JSON body")
        return jsonify({"error": "No valid workflow data provided"}), 400

    app.logger.info("Received valid workflow data, starting processing...")
    input_csv_filepath = DEFAULT_CSV_PATH

    if not os.path.exists(input_csv_filepath):
        app.logger.error(f"Server configuration error: Input CSV file not found at {input_csv_filepath}")
        return jsonify({"error": f"Server-side error: Input data file not found."}), 500

    try:
        # **CRITICAL CHANGE**: Wrap the call in a try block to catch specific errors
        results = run_workflow_processing(workflow_data, input_csv_filepath, PROCESSED_OUTPUT_DIR)
    except ValueError as e:
        # This catches the "cycle detected" error from the engine's topological sort
        app.logger.error(f"Workflow processing failed due to invalid graph: {e}")
        return jsonify({"error": str(e)}), 400 # 400 Bad Request is appropriate for client-provided bad data
    except Exception as e:
        # Catch any other unexpected errors from the engine
        app.logger.error(f"An unexpected error occurred during workflow processing: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500

    app.logger.info(f"Workflow processing complete. Results: {results}")

    if results.get("error"):
        # This handles errors that the engine caught internally (e.g., file write error)
        return jsonify(results), 500
    
    # Generate URLs for the frontend to use
    download_url = url_for('serve_processed_file', filename=results.get("output_file"))
    # Pass stats to the results page via query parameters for a richer display
    stats_params = results.get("stats", {}).get("decisions", {})
    results_page_url = url_for('show_results_page', filename=results.get("output_file"), **stats_params)

    return jsonify({
        "message": "Workflow processed successfully!",
        "stats": results.get("stats"),
        "output_filename": results.get("output_file"),
        "download_url": download_url,
        "results_page_url": results_page_url
    })


# --- File Download and Results Page Routes ---
@app.route('/processed_files/<path:filename>')
def serve_processed_file(filename):
    """Serves a processed file from the output directory for download."""
    app.logger.info(f"Serving processed file for download: {filename}")
    return send_from_directory(PROCESSED_OUTPUT_DIR, filename, as_attachment=True)


@app.route('/results/<path:filename>')
def show_results_page(filename):
    """Displays a page with a summary and a link to download the processed file."""
    app.logger.info(f"Showing results page for file: {filename}")
    # Regenerate download URL for the template
    download_url = url_for('serve_processed_file', filename=filename)
    # Get stats from the URL query parameters to display them
    stats = request.args.to_dict()
    return render_template('results.html', filename=filename, download_url=download_url, stats=stats)


# --- Main Execution Block ---
if __name__ == '__main__':
    if not os.path.exists(os.path.join(REACT_BUILD_DIR, 'index.html')):
        app.logger.critical(f"FATAL ERROR: React frontend build not found in {REACT_BUILD_DIR}")
        app.logger.critical("1. Ensure you are in the 'backend' directory when running this script.")
        app.logger.critical("2. Run 'npm run build' in your frontend's root directory.")
    else:
        app.logger.info(f"Starting Flask server...")
        app.logger.info(f"Serving React app from: {REACT_BUILD_DIR}")
        app.logger.info(f"API endpoint available at /process_workflow")
        app.run(debug=True, host='0.0.0.0', port=5001)