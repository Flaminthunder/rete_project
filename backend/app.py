from flask import Flask, request, jsonify, render_template, send_from_directory, url_for
import json
import os
import time # For timestamped output files
import logging # Using Python's logging module
from pharma_automation import run_workflow_processing # Your existing Python logic

# --- Configure logging ---
# It's better to configure logging once at the application level
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Get a logger for this specific module

app = Flask(__name__,
            static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), '../build/static'),
            template_folder='templates')

# --- Configuration & Setup ---
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
# This directory will be used as the base for resolving CSV filenames from Source nodes
DATA_FILES_BASE_DIR = APP_ROOT # Assume CSVs like pill_data.csv are in the 'backend' folder
PROCESSED_OUTPUT_DIR = os.path.join(APP_ROOT, "processed_output")
REACT_BUILD_DIR = os.path.join(APP_ROOT, '../build')

# Ensure the output directory exists on startup
os.makedirs(PROCESSED_OUTPUT_DIR, exist_ok=True)


# --- Static File Serving for React App ---
@app.route('/')
def index():
    """Serves the main index.html file of the React application."""
    logger.info(f"Serving index.html from: {REACT_BUILD_DIR}")
    return send_from_directory(REACT_BUILD_DIR, 'index.html')

# This route should handle files like manifest.json, favicon.ico, etc., from the build root
# and also be a fallback for other static assets if Flask's static_folder doesn't catch them.
# However, for Create React App, /static/... is usually correctly handled by `static_folder`.
@app.route('/<path:path>')
def serve_react_static_files(path):
    """Serves other static files from the React build directory."""
    # Check if the path exists directly in the build root
    potential_path = os.path.join(REACT_BUILD_DIR, path)
    if os.path.exists(potential_path) and not os.path.isdir(potential_path):
        logger.info(f"Serving static file from build root: {path}")
        return send_from_directory(REACT_BUILD_DIR, path)
    
    # Fallback: if it's not in the root, Flask might try to serve it from `static_folder` if path starts with `static_url_path`
    # If it's neither (e.g. a non-existent file or a React Router path),
    # this will result in a 404, which is often desired to let React Router handle it on the client.
    # For unhandled paths that are not static assets, let it 404 or serve index.html for client-side routing.
    # Re-serving index.html for all unhandled paths is a common SPA strategy.
    logger.warning(f"Static file '{path}' not found directly in build root. SPA fallback to index.html may occur if not a static asset path.")
    return send_from_directory(REACT_BUILD_DIR, 'index.html') # Fallback for SPA routing


# --- API Endpoint for Workflow Processing ---
@app.route('/process_workflow', methods=['POST'])
def process_workflow_route():
    """
    Receives a workflow JSON, processes it using the automation engine
    with a CSV specified by a Source node in the workflow,
    and returns the results.
    """
    logger.info("Received POST request at /process_workflow")
    if not request.is_json:
        logger.error("Request failed: Content-Type is not application/json")
        return jsonify({"error": "Request must be JSON"}), 400

    workflow_data = request.get_json()
    if not workflow_data or "nodes" not in workflow_data:
        logger.error("Request failed: No valid workflow data in JSON body")
        return jsonify({"error": "No valid workflow data provided"}), 400

    logger.info("Received valid workflow data.")
    # `run_workflow_processing` will determine the specific CSV from the Source node
    # We pass DATA_FILES_BASE_DIR as the directory where those CSVs are located.

    try:
        results = run_workflow_processing(workflow_data, DATA_FILES_BASE_DIR, PROCESSED_OUTPUT_DIR)
    except ValueError as e: # Catches cycle detection from WorkflowEngine constructor
        logger.error(f"Workflow processing failed due to invalid graph (e.g., cycle): {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"An unexpected error occurred during workflow processing: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred during processing."}), 500

    logger.info(f"Workflow processing complete. Results: {results}")

    if results.get("error"): # Handles errors caught within run_workflow_processing
        return jsonify(results), 500 # Could also be 400 if it's a data issue
    
    output_filename = results.get("output_file")
    download_url = url_for('serve_processed_file', filename=output_filename) if output_filename else None
    # Pass stats to the results page via query parameters
    stats_query_params = results.get("stats", {}).get("decisions", {}) if results.get("stats") else {}

    results_page_url = url_for('show_results_page', filename=output_filename, **stats_query_params) if output_filename else None

    return jsonify({
        "message": "Workflow processed successfully!",
        "stats": results.get("stats"),
        "output_filename": output_filename,
        "download_url": download_url,
        "results_page_url": results_page_url
    })


# --- File Download and Results Page Routes ---
@app.route('/processed_files/<path:filename>')
def serve_processed_file(filename):
    """Serves a processed file from the output directory for download."""
    logger.info(f"Serving processed file for download: {filename} from {PROCESSED_OUTPUT_DIR}")
    return send_from_directory(PROCESSED_OUTPUT_DIR, filename, as_attachment=True)

@app.route('/results/<path:filename>')
def show_results_page(filename):
    """Displays a page with a summary and a link to download the processed file."""
    logger.info(f"Showing results page for file: {filename}")
    download_url = url_for('serve_processed_file', filename=filename)
    stats = request.args.to_dict() # Retrieve stats passed as query parameters
    return render_template('results.html', filename=filename, download_url=download_url, stats=stats)


# --- Main Execution Block ---
if __name__ == '__main__':
    if not os.path.exists(os.path.join(REACT_BUILD_DIR, 'index.html')):
        logger.critical(f"FATAL ERROR: React frontend build not found in {REACT_BUILD_DIR}")
        logger.critical("1. Ensure this script ('app.py') is in the 'backend' directory.")
        logger.critical("2. Ensure the 'build' directory (from 'npm run build') is a sibling to 'backend'.")
        logger.critical("3. Run 'npm run build' in your frontend's root directory if it's missing.")
    else:
        logger.info(f"Starting Flask server...")
        logger.info(f"Serving React app (index.html) from: {REACT_BUILD_DIR}")
        logger.info(f"Serving React static assets (JS, CSS) from: {app.static_folder}")
        logger.info(f"API endpoint available at /process_workflow")
        logger.info(f"Base directory for data CSVs (from Source nodes): {DATA_FILES_BASE_DIR}")
        logger.info(f"Processed files will be saved to: {PROCESSED_OUTPUT_DIR}")
        app.run(debug=True, host='0.0.0.0', port=5001)