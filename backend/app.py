from flask import Flask, request, jsonify, render_template, send_from_directory, url_for
import json
import os
import time # For timestamped output files
from pharma_automation import run_workflow_processing # Your existing Python logic

app = Flask(__name__,
            # Assuming 'backend' and 'build' are sibling directories
            # static_url_path ensures that requests for /static/... go to this folder
            static_folder=os.path.join(os.path.dirname(__file__), '../build/static'),
            template_folder='templates') # For results.html

# Configuration
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV_PATH = os.path.join(APP_ROOT, "pill_data.csv") # Ensure this file is in the backend folder
PROCESSED_OUTPUT_DIR = os.path.join(APP_ROOT, "processed_output")
REACT_BUILD_DIR = os.path.join(APP_ROOT, '../build') # Path to the root of the React build folder

os.makedirs(PROCESSED_OUTPUT_DIR, exist_ok=True)


@app.route('/')
def index():
    """Serve React App's index.html."""
    print(f"Serving index.html from: {REACT_BUILD_DIR}")
    return send_from_directory(REACT_BUILD_DIR, 'index.html')

@app.route('/<path:path>')
def serve_react_static_files(path):
    """
    Serve other static files from the React build folder (e.g., manifest.json, favicon.ico).
    Requests for /static/css/main.css or /static/js/main.js are handled by static_folder config.
    This route handles files directly in the build root.
    """
    print(f"Attempting to serve static file: {path} from {REACT_BUILD_DIR}")
    if os.path.exists(os.path.join(REACT_BUILD_DIR, path)):
        return send_from_directory(REACT_BUILD_DIR, path)
    else:
        # Fallback for cases where path might be something like 'manifest.json'
        # that isn't caught by the more specific static routes by default.
        # This is a bit of a catch-all; might need refinement if certain files aren't found.
        # The `static_folder` in Flask's constructor primarily handles /static/... routes.
        # For root files like manifest.json, this explicit route is often needed.
        return send_from_directory(REACT_BUILD_DIR, path)


@app.route('/process_workflow', methods=['POST'])
def process_workflow_route():
    print("Received POST request at /process_workflow")
    if not request.is_json:
        print("Error: Request is not JSON")
        return jsonify({"error": "Request must be JSON"}), 400

    workflow_data = request.get_json()
    if not workflow_data:
        print("Error: No workflow data in JSON body")
        return jsonify({"error": "No workflow data provided"}), 400

    print("Received workflow data for processing:", json.dumps(workflow_data, indent=2))
    input_csv_filepath = DEFAULT_CSV_PATH
    print(f"Using input CSV: {input_csv_filepath}")

    if not os.path.exists(input_csv_filepath):
        print(f"Error: Input CSV file not found at {input_csv_filepath}")
        return jsonify({"error": f"Input CSV file not found at {input_csv_filepath}"}), 500


    results = run_workflow_processing(workflow_data, input_csv_filepath, PROCESSED_OUTPUT_DIR)
    print("Workflow processing results:", results)

    if results.get("error"):
        return jsonify(results), 500

    processed_file_url = None
    if results.get("output_file"):
        # The filename itself (e.g., "processed_20231027-123456_pill_data.csv")
        processed_file_url = url_for('serve_processed_file', filename=results.get("output_file"), _external=False)
        # _external=True would generate full URL including domain, False generates relative path

    results_page_url = None
    if results.get("output_file"):
        results_page_url = url_for('show_results_page', filename=results.get("output_file"))


    return jsonify({
        "message": "Workflow processed successfully!",
        "stats": results.get("stats"),
        "output_filename": results.get("output_file"),
        "download_url": processed_file_url, # Relative URL for download link
        "results_page_url": results_page_url # Relative URL for results page
    })

@app.route('/processed_files/<path:filename>') # Use <path:filename> to handle subdirectories if any
def serve_processed_file(filename):
    """Serves files from the processed_output directory for download."""
    print(f"Attempting to serve processed file: {filename} from {PROCESSED_OUTPUT_DIR}")
    return send_from_directory(PROCESSED_OUTPUT_DIR, filename, as_attachment=True)


@app.route('/results/<path:filename>')
def show_results_page(filename):
    """Displays a page with results and download link."""
    print(f"Showing results page for file: {filename}")
    download_url = url_for('serve_processed_file', filename=filename)
    # You could also pass stats to the template if you retrieve them here
    # For example, by saving stats to a temporary session or database, or passing as query params.
    # For simplicity, we're just using the filename and regenerating the download URL.
    return render_template('results.html', filename=filename, download_url=download_url)


if __name__ == '__main__':
    if not os.path.exists(os.path.join(REACT_BUILD_DIR, 'index.html')):
        print(f"ERROR: React frontend build not found in {REACT_BUILD_DIR}")
        print("Please ensure you have run 'npm run build' in your frontend's root directory,")
        print("and that this script is located in a 'backend' folder sibling to the 'build' folder.")
    else:
        print(f"Serving React app from: {REACT_BUILD_DIR}")
        print(f"Static assets (like CSS/JS) are served from: {app.static_folder}")
        print(f"Processed files will be saved to: {PROCESSED_OUTPUT_DIR}")
        print(f"Output files will be downloadable from /processed_files/<filename>")
        print(f"Results page will be at /results/<filename>")
        app.run(debug=True, host='0.0.0.0', port=5001)