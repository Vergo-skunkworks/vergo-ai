import logging
import os
import datetime
from flask import Flask, request, jsonify, current_app, send_file
from analytics.upgradedAnalysis import process_prompt
from dotenv import load_dotenv
import uuid
import io
import xlsxwriter

try:
    from integrations.updatedIntegrations import (
        setup_db_tables, # Changed function name
        process_uploaded_report, # Changed function name,
        process_multiple_uploaded_reports,
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


    report_files = request.files.getlist("report_files")
    company_id_str = request.form.get("company_id")
    category_name = request.form.get("category_name") # New optional parameter
    user_id = request.form.get("uploaded_by")

    if not report_files:
        return (
            jsonify(
                {"status": "error", "message": "No 'file' part in the request"}
            ),
            400,
        )

    try:
        # # Pass the FileStorage object and raw form strings to the handler
        # response_data, status_code = process_uploaded_report( # Changed function name
        #     files_storage=report_files,
        #     company_id=int(company_id_str), # Convert to int here
        #     user_id=int(user_id),
        #     category_name=category_name, # Pass category_id if present
        #
        # )
        # return jsonify(response_data), status_code

        results, status = process_multiple_uploaded_reports(
            files=report_files,
            company_id=int(company_id_str),
            user_id=int(user_id),
            category_name=category_name
        )
        return jsonify(results), status

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
    chat_id = data.get("chat_id")
    selected_categories = data.get("selected_categories")

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
        result = process_prompt(prompt, company_id, chat_id, selected_categories)
        print(result) # Consider using logger.info instead of print for production
        logger.info("Successfully processed prompt via /api/query")
        return jsonify({"response": {
            "results": result
        }}), 200

    except ValueError as ve:
        logger.error(f"Configuration error processing prompt: {str(ve)}", exc_info=True)
        return jsonify({"error": f"Configuration error: {str(ve)}"}), 500
    except Exception as e:
        logger.error(f"Error processing prompt: {str(e)}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route('/export-chart', methods=['POST'])
def export_chart():
    try:
        payload = request.json
        chart_data = payload.get("data")


        # ------------------------------------

        if not chart_data or not isinstance(chart_data, list):
            return {"error": "Invalid chart data"}, 400

        trace = chart_data[0]
        x_vals = trace.get("x", [])
        y_vals = trace.get("y", [])
        series_name = trace.get("name", "Series")
        chart_type_from_request = trace.get("type", "column") # Look for 'type' inside the trace

        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Chart Data")

        worksheet.write('A1', 'Name')
        worksheet.write('B1', series_name)

        for row, (x, y) in enumerate(zip(x_vals, y_vals), start=1):
            worksheet.write(row, 0, x)
            worksheet.write(row, 1, y)

        # --- 2. ADD DIAGNOSTIC PRINT STATEMENT ---
        # This will show us in the terminal exactly what value is being used.
        print(f"DEBUG: Attempting to create chart with type: '{chart_type_from_request}'")
        chart = workbook.add_chart({'type': chart_type_from_request})
        # -----------------------------------------

        # This check will confirm if the chart was created successfully
        if chart is None:
            # This error is now more informative if it happens again.
            return {"error": f"Failed to create chart. Invalid type provided: '{chart_type_from_request}'"}, 500

        data_len = len(x_vals)

        chart.add_series({
            'name': ['Chart Data', 0, 1],
            'categories': ['Chart Data', 1, 0, data_len, 0],
            'values': ['Chart Data', 1, 1, data_len, 1],
        })

        chart.set_title({'name': series_name})
        chart.set_x_axis({'name': 'Employee Name', 'label_position': 'low', 'num_font': {'rotation': -45}})
        chart.set_y_axis({'name': 'Value'})
        chart.set_style(10)

        worksheet.insert_chart('D2', chart, {'x_scale': 1.5, 'y_scale': 1.5})

        workbook.close()
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name=f"Excel_{chart_type_from_request}_Chart.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {"error": str(e)}, 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    logger.info("Health check accessed")
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port)