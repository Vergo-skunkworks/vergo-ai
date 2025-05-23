import logging
from flask import Flask, request, jsonify
# Make sure rag_pipeline.py is in the same directory or accessible via PYTHONPATH
from rag_pipeline import process_prompt
from dotenv import load_dotenv  # Import dotenv

# Load environment variables from .env file (especially needed if running Flask directly)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# The complex prompt previously used for testing in rag_pipeline.py
TEST_PROMPT = """
Generate 3-5 meaningful visualizations using bar and line graphs that would provide business 
insights, such as:\n- Comparison between estimated vs. actual profit percentages 
per project\n- Distribution of jobs by cost center (use count)\n- Change order value
over time (sum of value per month)\n- Projects with the largest difference between 
estimated and actual costs (top 5)\n\nAlso, provide key insights on:\n- Which project managers are 
consistently delivering the highest profit margins? (Top 3)\n- Which cost centers are most profitable? (
Top 3 based on average actual profit margin)\n- Are there patterns between job size (estimated cost) and profit margin?
(Provide a general observation)
"""

tables_to_use = [
    "estimator_analysis_pm",
    "estimator_analysis_jobs_written",
    "estimator_analysis_jobs_bid",
    "estimator_analysis_change_orders",
    "estimator_analysis_jobs_completed"

]


@app.route('/api/query', methods=['POST'])
def query_pipeline():
    """Handles user queries sent via POST request."""
    logger.info("Received request to /api/query")

    if not request.is_json:
        logger.warning("Request is not JSON")
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()
    prompt = data.get('prompt')
    company_id = data.get('company_id')

    if not prompt:
        logger.warning("Missing prompt in request")
        return jsonify({"error": "Missing 'prompt' in request body"}), 400

    try:
        logger.info(f"Processing prompt: {prompt[:100]}...")
        result = process_prompt(prompt, company_id)
        logger.info("Successfully processed prompt via /api/query")
        return jsonify({"response": result}), 200
    except ValueError as ve:
        logger.error(f"Configuration error processing prompt: {str(ve)}", exc_info=True)
        return jsonify({"error": f"Configuration error: {str(ve)}"}), 500
    except Exception as e:
        logger.error(f"Error processing prompt: {str(e)}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@app.route('/api/test_pipeline', methods=['GET'])
def test_pipeline_endpoint():
    """Runs the predefined complex test prompt through the pipeline."""
    logger.info("Received request to /api/test_pipeline")
    try:
        logger.info(f"Processing predefined test prompt...")
        # Use the hardcoded TEST_PROMPT
        prompt = "Show me the total change orders for each project manager listed in pms."
        company_id = 1
        results = process_prompt(prompt, company_id)
        logger.info("Successfully processed predefined test prompt via /api/test_pipeline")
        # Return the result
        return jsonify({"response": results}), 200
    except ValueError as ve:
        logger.error(f"Configuration error during test run: {str(ve)}", exc_info=True)
        return jsonify({"error": f"Configuration error: {str(ve)}"}), 500
    except Exception as e:
        logger.error(f"Error processing test prompt: {str(e)}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred during test run: {str(e)}"}), 500


@app.route('/', methods=['GET'])
def home():
    """Basic status endpoint."""
    logger.info("Home endpoint accessed")
    return jsonify({
        "status": "API is running",
        "endpoints": [
            {"path": "/api/query", "method": "POST", "description": "Process a custom prompt provided in JSON body."},
            {"path": "/api/test_pipeline", "method": "GET", "description": "Process a predefined complex test prompt."}
        ]
    })


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    logger.info("Health check accessed")
    # Basic health check, could be expanded (e.g., check DB connection)
    return jsonify({"status": "healthy"}), 200


if __name__ == '__main__':
    # Note: Use a production-ready server like Gunicorn or Waitress instead of Flask's built-in server for deployment.
    app.run(debug=True, host='0.0.0.0', port=5002)  # Running on port 5001 for clarity

