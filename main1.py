import logging
import os
import datetime
from flask import Flask, request, jsonify, current_app
from analytics.data_analysis_pipeline import process_prompt
from dotenv import load_dotenv
import uuid

try:
    from integrations.updatedIntegrations import (
        setup_db_tables, # Changed function name
        process_uploaded_report, # Changed function name
        logger,
    )
except ImportError:
    import sys
    from integrations.updatedIntegrations import (
        setup_db_tables, # Changed function name
        process_uploaded_report, # Changed function name
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
            logger.info("Attempting to set up database tables via Flask app...")
            setup_db_tables() # Call the new, comprehensive setup function
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

    if "excel_file" not in request.files:
        return (
            jsonify(
                {"status": "error", "message": "No 'excel_file' part in the request"}
            ),
            400,
        )

    excel_file = request.files["excel_file"]  # This is a FileStorage object
    company_id_str = request.form.get("company_id")
    # report_name is now the 'logical_file_name' for versioning
    logical_file_name = request.form.get("report_name") # New parameter name
    category_name = request.form.get("category_name") # New optional parameter
    user_id = request.form.get("uploaded_by")

    # Add validation for logical_file_name
    if not logical_file_name:
        return jsonify({"status": "error", "message": "Missing 'logical_file_name' in form data"}), 400

    try:
        # Pass the FileStorage object and raw form strings to the handler
        response_data, status_code = process_uploaded_report( # Changed function name
            excel_file_storage=excel_file,
            company_id=int(company_id_str), # Convert to int here
            report_name_original=logical_file_name, # Pass as original report name
            user_id=int(user_id),
            category_name=category_name, # Pass category_id if present

        )
        return jsonify(response_data), status_code

    except ValueError as e: # Catch ValueError from int conversion or handler validation
        logger.error(f"API /upload-excel-report validation error: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
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
    promptId = data.get("promptId")
    chat_id = data.get("chat_id");

    if not prompt:
        logger.warning("Missing prompt in request")
        return jsonify({"error": "Missing 'prompt' in request body"}), 400

    if company_id is None: # company_id must be provided
        logger.warning("Missing company_id in request")
        return jsonify({"error": "Missing 'company_id' in request body"}), 400

    try:
        # to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").date() # Remove if not used
        logger.info(f"Processing prompt for company {company_id}: {prompt[:100]}...")
        # The process_prompt function in analytics.data_analysis_pipeline.py
        # will now need to query the new relational tables (files, file_data, file_schemas)
        # to get the data and schema context for the LLM.
        result = process_prompt(prompt, company_id, chat_id)
        print(result) # Consider using logger.info instead of print for production
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
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=False, host="0.0.0.0", port=port)