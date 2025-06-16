import logging
import os
import datetime
from flask import Flask, request, jsonify, current_app
from analytics.data_analysis_pipeline import process_prompt
from dotenv import load_dotenv

try:
    from integrations.data_integration import (
        setup_jsonb_table,
        handle_excel_upload_request,
        logger,
    )
except ImportError:
    import sys

    # sys.path.append(os.path.join(os.path.dirname(__file__), 'path_to_your_module'))
    from integrations.data_integration import (
        setup_jsonb_table,
        handle_excel_upload_request,  # Changed
        logger,
    )
# Load environment variables from .env file (especially needed if running Flask directly)
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)


@app.before_request
def ensure_db_setup():
    if not hasattr(current_app, "db_initialized"):
        try:
            logger.info("Attempting to set up database table via Flask app...")
            setup_jsonb_table()
            current_app.db_initialized = True
            logger.info("Database table setup check complete.")
        except Exception as e:
            logger.error(f"Flask app: Database setup failed: {e}", exc_info=True)
            current_app.db_initialized = False  # Mark as failed


@app.route("/upload-excel-report", methods=["POST"])
def upload_excel_report_route():
    if not getattr(current_app, "db_initialized", False):
        logger.error("API call failed: Database not initialized.")
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Database not initialized. Check server logs.",
                }
            ),
            500,
        )

    # Basic presence checks
    if "excel_file" not in request.files:
        return (
            jsonify(
                {"status": "error", "message": "No 'excel_file' part in the request"}
            ),
            400,
        )

    excel_file = request.files["excel_file"]  # This is a FileStorage object
    company_id_str = request.form.get("company_id")
    report_name_original = request.form.get("report_name")
    # to_date_str = request.form.get("to_date")

    # Further validation and processing is now delegated to handle_excel_upload_request

    try:
        # Pass the FileStorage object and raw form strings to the handler
        response_data, status_code = handle_excel_upload_request(
            excel_file_storage=excel_file,
            company_id_str=company_id_str,
            report_name_original=report_name_original,
            # to_date_str=to_date_str
        )
        return jsonify(response_data), status_code

    except Exception as e:
        # This is a general fallback for unexpected errors during the call to the handler itself,
        # though handle_excel_upload_request should catch its own internal errors.
        logger.error(
            f"API /upload-excel-report unexpected error during call to handler: {e}",
            exc_info=True,
        )
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"An unexpected server error occurred in API route: {str(e)}",
                }
            ),
            500,
        )


@app.route("/api/query", methods=["POST"])
def query_pipeline():
    """Handles user queries sent via POST request."""
    logger.info("Received request to /api/query")

    if not request.is_json:
        logger.warning("Request is not JSON")
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()
    prompt = data.get("prompt")
    company_id = data.get("company_id")
    # to_date = data.get("to_date")
    promptId = data.get("promptId")

    if not prompt:
        logger.warning("Missing prompt in request")
        return jsonify({"error": "Missing 'prompt' in request body"}), 400

    # if not to_date:
    #     logger.warning("Missing Report Date in request")
    #     return jsonify({"error": "Missing 'Date' in request body"}), 400
    try:
        # to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").date()
        logger.info(f"Processing prompt: {prompt[:100]}...")
        result = process_prompt(prompt, company_id)
        print(result)
        logger.info("Successfully processed prompt via /api/query")
        return jsonify({"response": {
            "promptId" : promptId,
            "results": result
        }}), 200

    except ValueError as ve:
        logger.error(f"Configuration error processing prompt: {str(ve)}", exc_info=True)
        return jsonify({"error": f"Configuration error: {str(ve)}"}), 500
    except Exception as e:
        logger.error(f"Error processing prompt: {str(e)}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    logger.info("Health check accessed")
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    # Get the port number from the environment variable PORT, default to 8080
    port = int(os.environ.get("PORT", 8080))
    # Run the app. Set debug to False for production.
    app.run(debug=False, host="0.0.0.0", port=port)
