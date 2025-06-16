from contextlib import contextmanager
import pandas as pd
import os
import logging
import re
import json
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text, inspect
import datetime
from dotenv import load_dotenv
import io
from typing import Union, Tuple, Dict, Any, List
from werkzeug.datastructures import FileStorage

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()

# --- Constants moved from main.py ---
ALLOWED_EXTENSIONS = {"xlsx", "xls", "csv"}


def allowed_file(filename: str) -> bool:
    """Checks if the file extension is allowed."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@contextmanager
def get_db_connection():
    """Provides a managed database connection."""
    # Required environment variables
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    instance_connection_name = os.environ.get("INSTANCE_CONNECTION_NAME")

    # Validate required environment variables
    if not all([db_name, db_user, db_password, instance_connection_name]):
        missing_vars = []
        if not db_name:
            missing_vars.append("DB_NAME")
        if not db_user:
            missing_vars.append("DB_USER")
        if not db_password:
            missing_vars.append("DB_PASSWORD")
        if not instance_connection_name:
            missing_vars.append("INSTANCE_CONNECTION_NAME")

        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

    from urllib.parse import quote_plus

    # For Cloud SQL Connector using Unix domain sockets
    # Format: postgresql+psycopg2://user:password@/dbname?host=/cloudsql/instance_connection_name
    # conn_string = (
    #     f"postgresql+psycopg2://{db_user}:{quote_plus(db_password)}@/{db_name}"
    #     f"?host=35.190.189.103"
    # )

    # Alternative format (both should work):
    conn_string = (
        f"postgresql+psycopg2://{db_user}:{quote_plus(db_password)}@/{db_name}"
        f"?host=/cloudsql/{instance_connection_name}"
    )
    # conn_string = (
    #     f"postgresql+psycopg2://{db_user}:{quote_plus(db_password)}@/{db_name}"
    #     f"?host=/cloudsql/{instance_connection_name}"
    # )

    engine = None
    conn = None
    try:
        logger.debug("Attempting to connect to the database via Cloud SQL Connector.")
        engine = create_engine(
            conn_string,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,  # 30 minutes
        )
        conn = engine.connect()
        logger.debug("Database connection successful.")
        yield conn, engine
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed.")
        if engine:
            engine.dispose()
            logger.debug("Database engine disposed.")


def sanitize_name(name: Any) -> str:
    """Sanitizes a string to be a valid SQL table/column name or JSON key."""
    if not isinstance(name, str):
        name = str(name)
    name = name.lower()
    name = name.replace(" ", "_").replace("-", "_")
    name = re.sub(r"[^\w._]+", "", name)  # Allow '.', '_', alphanumeric
    name = name.lstrip("_").lstrip(".")
    name = name.rstrip("_").rstrip(".")

    if not name:
        return "invalid_name"
    return name


def setup_jsonb_table():
    """Creates or updates the JSONB table with data and data_schema columns."""
    with get_db_connection() as (conn, engine):
        try:
            result = conn.execute(
                text("SELECT current_database(), current_schema()")
            ).fetchone()
            logger.info(f"Connected to database: {result[0]}, schema: {result[1]}")

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS public.company_data (
                company_id INTEGER PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'::jsonb,
                data_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
                report_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.debug("CREATE TABLE statement executed.")

            table_check_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'company_data'
            );
            """
            table_exists = conn.execute(text(table_check_sql)).scalar()
            if not table_exists:
                raise RuntimeError(
                    "Failed to create company_data table in public schema."
                )

            alter_table_sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = 'company_data'
                    AND column_name = 'data_schema'
                ) THEN
                    ALTER TABLE public.company_data
                    ADD COLUMN data_schema JSONB NOT NULL DEFAULT '{}'::jsonb;
                END IF;
            END
            $$;
            """
            conn.execute(text(alter_table_sql))
            conn.commit()
            logger.debug("ALTER TABLE statement executed to ensure data_schema column.")

            trigger_sql = """
            CREATE OR REPLACE FUNCTION public.update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS update_company_data_timestamp ON public.company_data;
            CREATE TRIGGER update_company_data_timestamp
            BEFORE UPDATE ON public.company_data
            FOR EACH ROW
            EXECUTE FUNCTION public.update_updated_at_column();
            """
            conn.execute(text(trigger_sql))
            conn.commit()
            logger.debug("Trigger setup completed.")

            inspector = inspect(engine)
            inspector.clear_cache()  # ensure fresh inspection
            if not inspector.has_table("company_data", schema="public"):
                raise RuntimeError(
                    "company_data table not found in public schema after creation."
                )

            columns = inspector.get_columns("company_data", schema="public")
            column_names = [col["name"] for col in columns]
            expected_columns = {
                "company_id",
                "data",
                "data_schema",
                "created_at",
                "updated_at",
            }
            if not expected_columns.issubset(column_names):
                missing = expected_columns - set(column_names)
                raise RuntimeError(
                    f"Table company_data is missing expected columns: {missing}"
                )

            logger.info("JSONB table setup complete with data_schema column.")
        except Exception as e:
            logger.error(f"❌ Error setting up JSONB table: {e}", exc_info=True)
            if conn:
                conn.rollback()
            raise


def get_or_create_company_data(
    company_id: int,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Gets existing company data and schema or creates a new empty structure
    for a given company_id.
    """
    with get_db_connection() as (conn, engine):
        try:
            result = conn.execute(
                text(
                    "SELECT data, data_schema,report_metadata FROM public.company_data WHERE company_id = :company_id"
                ),
                {"company_id": company_id},
            ).fetchone()

            if result:
                company_data, data_schema, report_metadata = result
                company_data = dict(company_data) if company_data else {}
                data_schema = dict(data_schema) if data_schema else {}
                report_metadata = dict(report_metadata) if report_metadata else {}
                logger.info(
                    f"Retrieved existing data and schema for company_id {company_id}"
                )
                return company_data, data_schema, report_metadata
            else:
                empty_data = {}
                empty_schema = {}
                empty_metadata = {}
                conn.execute(
                    text(
                        "INSERT INTO public.company_data (company_id, data, data_schema, report_metadata)"
                        " VALUES (:company_id, :data, :data_schema, :report_metadata)"
                    ),
                    {
                        "company_id": company_id,
                        "data": json.dumps(empty_data),
                        "data_schema": json.dumps(empty_schema),
                        "report_metadata": json.dumps(empty_metadata),
                    },
                )
                conn.commit()
                logger.info(
                    f"Created new data and schema structure for company_id {company_id}"
                )
                return empty_data, empty_schema, empty_metadata
        except SQLAlchemyError as e:
            logger.error(
                f"❌ Error in get_or_create_company_data for company_id {company_id}: {e}",
                exc_info=True,
            )
            if conn:
                conn.rollback()
            raise



# def get_previous_company_data(
#         company_id: int, to_date: datetime.date
# ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
#     """Fetches data and schema from the most recent row before to_date for company_id."""
#     with get_db_connection() as (conn, engine):
#         try:
#             result = conn.execute(
#                 text(
#                     "SELECT data, data_schema FROM public.company_data "
#                     "WHERE company_id = :company_id AND to_date < :to_date "
#                     "ORDER BY to_date DESC LIMIT 1"
#                 ),
#                 {"company_id": company_id, "to_date": to_date},
#             ).fetchone()
#             if result:
#                 return dict(result[0]) if result[0] else {}, (
#                     dict(result[1]) if result[1] else {}
#                 )
#             return {}, {}
#         except SQLAlchemyError as e:
#             logger.error(f"❌ Error fetching previous data: {e}", exc_info=True)
#             return {}, {}

def update_column_descriptions(company_id: int, report_name: str, column_names: List[str]) -> None:
    """
    Updates the `description` column in the `company_data` table to reflect column names
    of the uploaded report. Adds missing columns with empty descriptions.

    Args:
        company_id: The company_id to target.
        report_name: The name of the uploaded report.
        column_names: List of column names in the uploaded file.
    """
    try:
        with get_db_connection() as (conn, engine):
            # Step 1: Fetch existing description JSON
            query = text("SELECT description FROM company_data WHERE company_id = :company_id LIMIT 1")
            result = conn.execute(query, {"company_id": company_id}).fetchone()
            current_description = result[0] if result and result[0] else {}

            # Ensure it's a dict
            if isinstance(current_description, str):
                current_description = json.loads(current_description)
            elif not isinstance(current_description, dict):
                current_description = {}

            # Step 2: Get existing columns for the report
            existing_columns = current_description.get(report_name, {})

            # Step 3: Check if columns match
            existing_column_set = set(existing_columns.keys())
            uploaded_column_set = set(column_names)

            if existing_column_set == uploaded_column_set:
                logger.info("Column structure matches existing. No update required.")
                return  # Structure is identical
            logger.info("Updating Description of columns")
            # Step 4: Update description
            updated_columns = {col: existing_columns.get(col, "") for col in column_names}
            current_description[report_name] = updated_columns

            # Step 5: Write back updated JSON
            update_query = text("""
                UPDATE company_data
                SET description = :description
                WHERE company_id = :company_id
            """)
            logger.info("Below is updated description")
            conn.execute(update_query, {
                "description": json.dumps(current_description),
                "company_id": company_id
            })
            conn.commit()
            logger.info(f"Updated column descriptions for report '{report_name}'.")

    except Exception as e:
        logger.error(f"❌ Failed to update column descriptions: {e}", exc_info=True)
        raise


def infer_excel_col_schema(df: pd.DataFrame) -> Dict[str, str]:
    """
    Infers the schema for a DataFrame, mapping Pandas types to SQL-like types.
    Column names are sanitized.
    """
    schema = {}
    for col in df.columns:
        sanitized_col_name = sanitize_name(col)
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            schema[sanitized_col_name] = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            schema[sanitized_col_name] = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            schema[sanitized_col_name] = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            schema[sanitized_col_name] = "DATETIME"
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            try:
                # Attempt to check if non-NA values are all datetime-like
                # This is a heuristic and might not cover all edge cases or mixed-type columns well.
                if (
                        df[col]
                                .dropna()
                                .map(
                            lambda x: isinstance(
                                x, (datetime.datetime, datetime.date, pd.Timestamp)
                            )
                        )
                                .all()
                ):
                    schema[sanitized_col_name] = "DATETIME"
                else:
                    schema[sanitized_col_name] = "TEXT"
            except Exception:  # Fallback for complex object types or errors during map
                schema[sanitized_col_name] = "TEXT"
        else:
            schema[sanitized_col_name] = "UNKNOWN"  # Should be rare
    return schema


def update_company_jsonb(
    company_id: int, data: Dict[str, Any], data_schema: Dict[str, Any], report_metadata: Dict[str, Any]
):
    """Updates the JSONB data and schema for a company."""
    with get_db_connection() as (conn, engine):
        try:
            conn.execute(
                text(
                    "UPDATE public.company_data SET data = :data,"
                    " data_schema = :data_schema, report_metadata = :report_metadata WHERE company_id = :company_id"
                ),
                {
                    "company_id": company_id,
                    "data": json.dumps(data),
                    "data_schema": json.dumps(data_schema),
                    "report_metadata": json.dumps(report_metadata),
                },
            )
            conn.commit()
            logger.info(f"Updated JSONB data and schema for company_id {company_id}")
        except SQLAlchemyError as e:
            logger.error(
                f"❌ Error updating company data for company_id {company_id}: {e}",
                exc_info=True,
            )
            if conn:
                conn.rollback()
            raise



def process_excel_report_to_jsonb(
    report_key: str,  # Expects already sanitized report name
    file_source: Union[str, io.BytesIO],
    company_id: int,
    file_extension: str = None,
) -> Dict[str, Any]:
    """
    Processes an Excel file (first sheet only), stores its data under report_key
    in JSONB, and infers its schema. Replaces existing data/schema for this report_key.
    """
    logger.info(f"Processing report '{report_key}' for company_id {company_id}.")

    company_data, data_schema,report_metadata = get_or_create_company_data(company_id)
    try:
        # Read the file based on its extension
        if file_extension == "csv":
            try:
                df = pd.read_csv(file_source, encoding="utf-8")
            except UnicodeDecodeError:
                file_source.seek(0)  # Reset stream
                df = pd.read_csv(file_source, encoding="latin-1")
        elif file_extension in {"xlsx", "xls"}:
            df = pd.read_excel(file_source, sheet_name=0, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")

        # df = pd.read_excel(file_source, sheet_name=0, engine="openpyxl")
        df = df.dropna(how="all")
        nan_count = df.isna().sum().sum()
        df = df.where(df.notna(), None)
        logger.info(
            f"Replaced {nan_count} NaN values with None in report '{report_key}'"
        )

        if df.empty:
            logger.warning(
                f"Report '{report_key}' (first sheet) is empty after removing empty rows."
            )
            company_data[report_key] = []
            data_schema[report_key] = {}
            num_records = 0
        else:
            report_specific_schema = infer_excel_col_schema(df)
            data_schema[report_key] = report_specific_schema

            records = df.to_dict(orient="records")
            serializable_records = []
            for record in records:
                serializable_record = {}
                for key, value in record.items():
                    s_key = sanitize_name(
                        key
                    )  # Sanitize individual column keys from Excel
                    if isinstance(
                        value, (pd.Timestamp, datetime.datetime, datetime.date)
                    ):
                        serializable_record[s_key] = value.isoformat()
                    elif pd.isna(value):
                        serializable_record[s_key] = None
                    else:
                        serializable_record[s_key] = value
                serializable_records.append(serializable_record)

            logger.info(
                f"Storing {len(serializable_records)} records for report '{report_key}' for company_id {company_id}"
            )
            company_data[report_key] = serializable_records
            num_records = len(serializable_records)
            # Update report metadata with last_updated timestamp
            report_metadata[report_key] = {
                "last_updated": datetime.datetime.utcnow().isoformat() + "Z"
            }
            update_company_jsonb(company_id, company_data, data_schema,report_metadata)
            column_names = df.columns.tolist()
            update_column_descriptions(company_id,report_key,column_names)

        return {
            "status": "success",
            "company_id": company_id,
            "report_name": report_key,
            "records_processed": num_records,
            "schema_updated": data_schema.get(report_key, {}),
        }
    except (
        ValueError
    ) as e:  # Catches issues like bad Excel file format if pandas raises ValueError
        logger.error(
            f"❌ Invalid Excel file format or content for report '{report_key}': {e}",
            exc_info=True,
        )
        return {
            "status": "error",
            "report_name": report_key,
            "message": f"Invalid Excel file format or content: {str(e)}",
        }
    except FileNotFoundError:  # Only if file_source was a string path and not found
        logger.error(
            f"❌ File not found: {file_source}", exc_info=True
        )  # Should not happen with BytesIO
        return {
            "status": "error",
            "report_name": report_key,
            "message": f"File not found: {file_source}",
        }
    except ImportError as e:  # Missing openpyxl
        logger.error(
            f"❌ Required Excel engine (openpyxl) not installed: {e}", exc_info=True
        )
        return {
            "status": "error",
            "report_name": report_key,
            "message": f"Required Excel engine not installed: {str(e)}",
        }
    except Exception as e:
        source_info = (
            file_source if isinstance(file_source, str) else "streamed content"
        )
        logger.error(
            f"❌ Error processing report '{report_key}' from {source_info}: {e}",
            exc_info=True,
        )
        return {"status": "error", "report_name": report_key, "message": str(e)}

def get_report_metadata(company_id: int) -> Dict[str, Any]:
    """Fetches report metadata for a given company_id."""
    with get_db_connection() as (conn, engine):
        try:
            result = conn.execute(
                text("SELECT report_metadata FROM public.company_data WHERE company_id = :company_id"),
                {"company_id": company_id}
            ).fetchone()
            if result:
                metadata = dict(result[0]) if result[0] else {}
                return {
                    "status": "success",
                    "company_id": company_id,
                    "report_metadata": metadata
                }
            return {
                "status": "error",
                "message": f"No data found for company_id {company_id}"
            }
        except SQLAlchemyError as e:
            logger.error(f"❌ Error fetching report metadata for company_id {company_id}: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

def handle_excel_upload_request(
    excel_file_storage: FileStorage,
    company_id_str: Union[str, None],
    report_name_original: Union[str, None],
) -> Tuple[Dict[str, Any], int]:
    """
    Handles the overall Excel upload request, including validation,
    file reading, and calling the processing function.
    Returns a response dictionary and an HTTP status code.
    """
    if (
        not excel_file_storage
    ):  # Should be caught by Flask route if 'excel_file' not in request.files
        return {
            "status": "error",
            "message": "No 'excel_file' object provided to handler",
        }, 400
    if excel_file_storage.filename == "":
        return {"status": "error", "message": "No selected file"}, 400
    if not company_id_str:
        return {"status": "error", "message": "Missing 'company_id' in form data"}, 400
    if not report_name_original:
        return {"status": "error", "message": "Missing 'report_name' in form data"}, 400

    if not allowed_file(excel_file_storage.filename):
        return {
            "status": "error",
            "message": "File type not allowed. Please upload .xlsx or .xls files",
        }, 400

    try:
        company_id = int(company_id_str)
    except ValueError:
        return {"status": "error", "message": "'company_id' must be an integer"}, 400

    report_key = sanitize_name(report_name_original)
    if report_key == "invalid_name" or not report_key:
        return {
            "status": "error",
            "message": f"Invalid 'report_name' provided: {report_name_original}",
        }, 400
    file_extension = excel_file_storage.filename.rsplit(".", 1)[1].lower() if "." in excel_file_storage.filename else None
    logger.info(
        f"Handler: Received request for company_id: {company_id}, report_name: '{report_name_original}' (sanitized to "
        f"'{report_key}'), file: {excel_file_storage.filename}, extension: {file_extension}"
    )

    file_content_stream = None
    try:
        file_content_stream = io.BytesIO(excel_file_storage.read())
        file_content_stream.seek(0)  # Reset stream position to the beginning for pandas

        result_data = process_excel_report_to_jsonb(
            report_key=report_key,
            file_source=file_content_stream,
            company_id=company_id,
            file_extension=file_extension
        )

        if result_data.get("status") == "success":
            logger.info(
                f"Handler: Successfully processed report '{report_key}' for company {company_id}."
            )
            return result_data, 200
        else:
            logger.error(
                f"Handler: Error processing report '{report_key}' for company {company_id}: "
                f"{result_data.get('message')}"
            )
            error_message = result_data.get(
                "message", "Unknown error during processing."
            )
            if (
                "Invalid Excel file format" in error_message
                or "File not found" in error_message
            ):  # file not found
                # from processor
                return result_data, 400  # Bad request
            return result_data, 500  # Internal server error for other processing issues

    except (
        Exception
    ) as e:  # Catch any other unexpected errors during this handling phase
        logger.error(
            f"Handler: General error for report '{report_key}' (company {company_id}): {e}",
            exc_info=True,
        )
        return {
            "status": "error",
            "message": f"An unexpected server error occurred in handler: {str(e)}",
        }, 500
    finally:
        if file_content_stream:
            file_content_stream.close()


def get_jsonb_data_summary(company_id: Union[int, None] = None) -> Dict[str, Any]:
    """Returns a summary of the JSONB data and schema in the database."""
    # ... (This function remains unchanged from your original)
    with get_db_connection() as (conn, engine):
        try:
            if company_id:
                query = text(
                    "SELECT company_id, data, data_schema FROM public.company_data WHERE company_id = :company_id"
                )
                result = conn.execute(query, {"company_id": company_id}).fetchone()
                if not result:
                    return {"message": f"No data found for company_id {company_id}"}

                db_company_id, company_data, data_schema = result
                summary = {
                    "company_id": db_company_id,
                    "reports": {},
                    "schema_details": data_schema,
                }
                if company_data:
                    for report_key, items in company_data.items():
                        if isinstance(items, list):
                            summary["reports"][report_key] = {
                                "count": len(items),
                                "sample": items[0] if items else None,
                            }
                        else:
                            summary["reports"][report_key] = {
                                "type": type(items).__name__,
                                "value": (
                                    items
                                    if not isinstance(items, dict)
                                    else "nested_object"
                                ),
                            }
                return summary
            else:
                count_query = text("SELECT COUNT(*) FROM public.company_data")
                total_companies = conn.execute(count_query).scalar_one()
                all_query = text(
                    "SELECT company_id, data_schema FROM public.company_data"
                )
                results = conn.execute(all_query).fetchall()
                summary = {"total_companies_with_data": total_companies, "details": []}
                for row in results:
                    cid, d_schema = row
                    summary["details"].append(
                        {
                            "company_id": cid,
                            "schema_overview": (
                                list(d_schema.keys()) if d_schema else []
                            ),
                        }
                    )
                return summary
        except SQLAlchemyError as e:
            logger.error(f"❌ Error getting JSONB data summary: {e}", exc_info=True)
            return {"error": str(e)}