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

# --- Constants ---
ALLOWED_EXTENSIONS = {"xlsx", "xls", "csv"}

def allowed_file(filename: str) -> bool:
    """Checks if the file extension is allowed."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@contextmanager
def get_db_connection():
    """Provides a managed database connection."""
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    instance_connection_name = os.environ.get("INSTANCE_CONNECTION_NAME") # Keeping this for Cloud SQL Connector hint

    if not all([db_name, db_user, db_password, instance_connection_name]):
        missing_vars = []
        if not db_name: missing_vars.append("DB_NAME")
        if not db_user: missing_vars.append("DB_USER")
        if not db_password: missing_vars.append("DB_PASSWORD")
        if not instance_connection_name: missing_vars.append("INSTANCE_CONNECTION_NAME")
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    from urllib.parse import quote_plus
    # Use the public IP for Cloud SQL if needed, or Unix socket path
    # For Unix socket, host=/cloudsql/instance_connection_name
    # For public IP, host=35.190.189.103 (or your actual public IP)
    conn_string = (
        f"postgresql+psycopg2://{db_user}:{quote_plus(db_password)}@/{db_name}"
        f"?host=35.190.189.103" # Replace with your actual Cloud SQL public IP or unix socket path
    )

    # Alternative format (both should work):
    # conn_string = (
    #     f"postgresql+psycopg2://{db_user}:{quote_plus(db_password)}@/{db_name}"
    #     f"?host=/cloudsql/{instance_connection_name}"
    # )

    engine = None
    conn = None
    try:
        logger.debug("Attempting to connect to the database.")
        engine = create_engine(
            conn_string,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
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
    name = re.sub(r"[^\w._]+", "", name)
    name = name.lstrip("_").lstrip(".")
    name = name.rstrip("_").rstrip(".")
    if not name:
        return "invalid_name"
    return name

# --- NEW: Cloud Storage Integration (Placeholder) ---
# You'll need to implement actual file upload to S3/GCS/Azure Blob here.
# For now, this is a mock.
# def upload_file_to_cloud_storage(file_stream: io.BytesIO, company_name: int, original_filename: str) -> str:
#     """
#     Mocks uploading a file to cloud storage and returns its path/URL.
#     In a real application, this would interact with AWS S3, GCS, Azure Blob, etc.
#     """
#     # Simulate a cloud storage path
#     safe_filename = sanitize_name(original_filename)
#     timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#     # This path is just for demonstration. In real life, it would be an S3 URL, GCS path, etc.
#     mock_path = f"gs://your-bucket/{company_id}/{timestamp}_{safe_filename}"
#     logger.info(f"Mock: Uploaded {original_filename} to {mock_path}")
#     # In a real scenario, you'd perform the actual upload here.
#     return mock_path

# --- Database Setup (Refactored for New Schema) ---
def setup_db_tables():
    """Creates all necessary tables for the new relational schema."""
    with get_db_connection() as (conn, engine):
        try:
            logger.info("Setting up database tables...")

            # Company Table (assuming it exists, or create it if not)
            create_company_table_sql = """
            CREATE TABLE IF NOT EXISTS public.company (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """
            conn.execute(text(create_company_table_sql))
            logger.debug("Table 'company' setup check complete.")

            # Files Table
            create_files_table_sql = """
            CREATE TABLE IF NOT EXISTS public.files (
                file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                company_name VARCHAR(255) NOT NULL,
                category_id UUID REFERENCES public.file_categories(category_id) ON DELETE SET NULL,
                original_file_name VARCHAR(255) NOT NULL,
                uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                is_latest BOOLEAN DEFAULT TRUE
            );
            """
            conn.execute(text(create_files_table_sql))
            logger.debug("Table 'files' setup check complete.")

            # File Data Table
            create_file_data_table_sql = """
            CREATE TABLE IF NOT EXISTS public.file_data (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                file_id UUID NOT NULL REFERENCES public.files(file_id) ON DELETE CASCADE,
                data JSONB NOT NULL, -- The actual parsed JSON data from a row/record in the file
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                schema JSONB NOT NULL
            );
            """
            conn.execute(text(create_file_data_table_sql))
            logger.debug("Table 'file_data' setup check complete.")

            conn.commit()
            logger.info("All database tables setup complete.")
        except Exception as e:
            logger.error(f"❌ Error setting up database tables: {e}", exc_info=True)
            if conn:
                conn.rollback()
            raise

# Rename setup_jsonb_table to setup_db_tables as it's more comprehensive
setup_jsonb_table = setup_db_tables


# # New function to get file details for a company
# def get_company_files_metadata(company_id: int) -> List[Dict[str, Any]]:
#     """Retrieves metadata for all files associated with a company."""
#     with get_db_connection() as (conn, engine):
#         try:
#             query = text("""
#                 SELECT
#                     f.file_id,
#                     f.original_file_name,
#                     f.logical_file_name,
#                     f.file_type,
#                     f.uploaded_at,
#                     f.is_latest,
#                     f.version,
#                     fc.category_name
#                 FROM
#                     public.files f
#                 LEFT JOIN
#                     public.file_categories fc ON f.category_name = fc.category_name
#                 WHERE
#                     f.company_id = :company_id
#                 ORDER BY
#                     f.logical_file_name, f.version DESC
#             """)
#             result = conn.execute(query, {"company_id": company_id}).fetchall()
#             files_metadata = []
#             for row in result:
#                 files_metadata.append({
#                     "file_id": str(row[0]),
#                     "original_file_name": row[1],
#                     "logical_file_name": row[2],
#                     "file_type": row[3],
#                     "uploaded_at": row[4].isoformat() if row[4] else None,
#                     "is_latest": row[5],
#                     "version": row[6],
#                     "category_name": row[7]
#                 })
#             return files_metadata
#         except SQLAlchemyError as e:
#             logger.error(f"❌ Error fetching files metadata for company_id {company_id}: {e}", exc_info=True)
#             raise

def get_file_schema_definition(file_id: str) -> Dict[str, Any]:
    """Retrieves the schema definition for a specific file."""
    with get_db_connection() as (conn, engine):
        try:
            query = text("SELECT schema_definition FROM public.file_schemas WHERE file_id = :file_id")
            result = conn.execute(query, {"file_id": file_id}).fetchone()
            if result:
                return dict(result[0])
            return {}
        except SQLAlchemyError as e:
            logger.error(f"❌ Error fetching schema for file_id {file_id}: {e}", exc_info=True)
            raise

def insert_file_data(file_id: str, company_id: int, df: pd.DataFrame, inferred_schema: Dict[str, str]):
    """
    Inserts processed DataFrame rows into file_data and the schema into file_schemas.
    Handles replacing older versions' data if a new version is uploaded.
    """
    with get_db_connection() as (conn, engine):
        try:
            # Delete existing data for this file_id (if re-processing or replacing a specific file)
            # Or, if this is a new version of a logical_file, the old data might be kept
            # but usually for analysis, we only want the data of the *latest* file_id
            # If the strategy is "replace ALL data for a logical_file_name on new upload",
            # this logic needs to be more complex, finding old file_ids and deleting their data.
            # For now, we assume file_id uniquely identifies a specific upload.
            delete_existing_data_sql = text("DELETE FROM public.file_data WHERE file_id = :file_id")
            conn.execute(delete_existing_data_sql, {"file_id": file_id})
            logger.info(f"Deleted existing data for file_id {file_id} before re-insertion.")

            # Insert data rows
            records = df.to_dict(orient="records")
            insert_data_rows = []
            for i, record in enumerate(records):
                serializable_record = {}
                for key, value in record.items():
                    s_key = sanitize_name(key) # Sanitize individual column keys from Excel
                    if isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date)):
                        serializable_record[s_key] = value.isoformat()
                    elif pd.isna(value):
                        serializable_record[s_key] = None
                    else:
                        serializable_record[s_key] = value
                insert_data_rows.append(serializable_record)

            if insert_data_rows:
                conn.execute(
                    text("INSERT INTO public.file_data (file_id, data, schema, description) VALUES (:file_id, :data, :schema, :description)"),
                    {
                        "file_id": file_id,
                        "data": json.dumps(insert_data_rows),
                        "schema": json.dumps(inferred_schema),
                        "description": json.dumps({k: "" for k in inferred_schema.keys()})
                    }
                )
                logger.info(f"Inserted {len(insert_data_rows)} rows into file_data for file_id {file_id}.")

            conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"❌ Error inserting file data or schema for file_id {file_id}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            raise

def infer_excel_col_schema(df: pd.DataFrame) -> Dict[str, str]:
    """
    Infers the schema for a DataFrame, mapping Pandas types to SQL-like types.
    Column names are sanitized. This function remains largely the same.
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
            except Exception:
                schema[sanitized_col_name] = "TEXT"
        else:
            schema[sanitized_col_name] = "UNKNOWN"
    return schema

def process_uploaded_report(
    file_storage: FileStorage,
    company_id: int,
    report_name_original: str, # This will be the logical_file_name
    user_id:int,
    category_name: Union[str, None] = None, # New: Optional category
) -> Tuple[Dict[str, Any], int]:
    """
    Processes an uploaded Excel file, stores it, and inserts its data and schema
    into the new relational tables.
    Handles versioning: marks old versions of the same logical_file_name as not latest.
    """
    logger.info(f"Processing uploaded report '{report_name_original}' for company_id {company_id}.")

    file_extension = file_storage.filename.rsplit(".", 1)[1].lower() if "." in file_storage.filename else None
    if not file_extension:
        raise ValueError("Could not determine file extension.")

    # 1. Store the file content in cloud storage (mock for now)
    file_content_stream = io.BytesIO(file_storage.read())
    file_content_stream.seek(0) # Reset stream position


    with get_db_connection() as (conn, engine):
        try:
            # 2. Mark previous versions of this logical_file_name as not latest
            logical_file_name_sanitized = sanitize_name(report_name_original)
            update_old_versions_sql = text("""
                UPDATE public.files
                SET is_latest = FALSE
                WHERE company_id = :company_id AND category_name = :category_name;
            """)
            conn.execute(update_old_versions_sql, {
                "company_id": company_id,
                "category_name": category_name
            })
            logger.info(f"Marked older versions of '{category_name}' as not latest for company {company_id}.")


            # 4. Insert new record into the 'files' table
            insert_file_sql = text("""
                INSERT INTO public.files
                (company_id, category_name, original_file_name, uploaded_by)
                VALUES (:company_id, :category_name, :original_file_name, :user_id)
                RETURNING file_id;
            """)
            result = conn.execute(insert_file_sql, {
                "company_id": company_id,
                "category_name": category_name, # Will be None if not provided
                "original_file_name": logical_file_name_sanitized,
                "user_id": user_id
            }).fetchone()
            new_file_id = str(result[0])
            conn.commit() # Commit the file metadata insertion immediately
            logger.info(f"Inserted new file record with file_id: {new_file_id}, logical_file_name: '{logical_file_name_sanitized}'")

            # 5. Read and parse the Excel/CSV data
            # Reset stream position again if it was read by upload_file_to_cloud_storage
            file_content_stream.seek(0)
            if file_extension == "csv":
                try:
                    df = pd.read_csv(file_content_stream, encoding="utf-8")
                except UnicodeDecodeError:
                    file_content_stream.seek(0)
                    df = pd.read_csv(file_content_stream, encoding="latin-1")
            elif file_extension in {"xlsx", "xls"}:
                df = pd.read_excel(file_content_stream, sheet_name=0, engine="openpyxl")
            else:
                raise ValueError(f"Unsupported file extension for parsing: {file_extension}")

            df = df.dropna(how="all") # Drop rows that are entirely NaN
            df = df.where(df.notna(), None) # Replace NaN values with None for JSON serialization

            if df.empty:
                logger.warning(f"File '{file_storage.filename}' is empty after removing empty rows.")
                num_records = 0
                inferred_schema = {}
            else:
                inferred_schema = infer_excel_col_schema(df)
                num_records = len(df)

            # 6. Insert parsed data into file_data and schema into file_schemas
            insert_file_data(new_file_id, company_id, df, inferred_schema)


            return {
                "status": "success",
                "file_id": new_file_id,
                "company_id": company_id,
                "logical_file_name": logical_file_name_sanitized,
                "records_processed": num_records,
                "schema_inferred": inferred_schema,
            } ,200

        except ValueError as e:
            logger.error(f"❌ Data processing error: {e}", exc_info=True)
            # Rollback file insertion if data processing fails
            conn.rollback()
            return {
                "status": "error",
                "message": f"File processing failed: {str(e)}",
            }, 400
        except SQLAlchemyError as e:
            logger.error(f"❌ Database error during report processing: {e}", exc_info=True)
            conn.rollback()
            return {
                "status": "error",
                "message": f"Database error during report processing: {str(e)}",
            }, 400
        except Exception as e:
            logger.error(f"❌ Unexpected error during report processing: {e}", exc_info=True)
            if conn: # Attempt rollback if connection exists
                conn.rollback()
            return {
                "status": "error",
                "message": f"An unexpected error occurred: {str(e)}",
            }, 400
        finally:
            if file_content_stream:
                file_content_stream.close()


# Rename this function to reflect its new purpose.
# It now processes a FileStorage object directly, handles file storage, and database insertions.
# handle_excel_upload_request = process_uploaded_report

def process_multiple_uploaded_reports(
    files: List[FileStorage],
    company_id: int,
    user_id: int,
    category_name: Union[str, None] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Processes multiple uploaded files and returns their processing results.
    """
    results = []
    for file in files:
        if file and allowed_file(file.filename):
            result, status_code = process_uploaded_report(
                file_storage=file,
                company_id=company_id,
                report_name_original=file.filename,
                user_id=user_id,
                category_name=category_name
            )
            results.append(result)
        else:
            results.append({
                "status": "error",
                "message": f"Unsupported or missing file: {file.filename if file else 'Unknown'}"
            })
    return results, 200