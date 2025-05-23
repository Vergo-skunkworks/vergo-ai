from contextlib import contextmanager
import pandas as pd
import os
import glob
import logging
import re
import json
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text, inspect
import datetime
from dotenv import load_dotenv

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()


@contextmanager
def get_db_connection():
    """Provides a managed database connection."""
    db_params = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT')
    }
    if not all(db_params.values()):
        raise ValueError(
            "Database connection parameters (DB_NAME, DB_USER,"
            " DB_PASSWORD, DB_HOST, DB_PORT) must be set in environment variables.")

    from urllib.parse import quote_plus
    conn_string = (f"postgresql+psycopg2://{db_params['user']}:{quote_plus(db_params['password'])}@{db_params['host']}:"
                   f"{db_params['port']}/{db_params['dbname']}")

    engine = None
    conn = None
    try:
        logger.debug("Attempting to connect to the database.")
        engine = create_engine(conn_string)
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


def sanitize_name(name):
    """Sanitizes a string to be a valid SQL table or column name."""
    name = name.lower()
    name = name.replace(" ", "_").replace("-", "_")
    name = re.sub(r'[^\w]+', '', name)
    name = name.lstrip('_')
    if not name:
        return "invalid_name"
    if name[0].isdigit():
        name = "_" + name
    return name


def setup_jsonb_table():
    """Creates or updates the JSONB table with data and data_schema columns."""
    with get_db_connection() as (conn, engine):
        try:
            # Log database and schema information
            result = conn.execute(text("SELECT current_database(), current_schema()")).fetchone()
            logger.info(f"Connected to database: {result[0]}, schema: {result[1]}")

            # Create table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS public.company_data (
                company_id INTEGER PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'::jsonb,
                data_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.debug("CREATE TABLE statement executed.")

            # Verify table exists
            table_check_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'company_data'
            );
            """
            table_exists = conn.execute(text(table_check_sql)).scalar()
            if not table_exists:
                raise RuntimeError("Failed to create company_data table in public schema.")

            # Add data_schema column if missing
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

            # Add trigger for updated_at
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

            # Verify table schema
            inspector = inspect(engine)
            inspector.clear_cache()  # Clear cache to ensure fresh metadata
            if not inspector.has_table('company_data', schema='public'):
                raise RuntimeError("company_data table not found in public schema after creation.")

            columns = inspector.get_columns('company_data', schema='public')
            column_names = [col['name'] for col in columns]
            expected_columns = {'company_id', 'data', 'data_schema', 'created_at', 'updated_at'}
            if not expected_columns.issubset(column_names):
                missing = expected_columns - set(column_names)
                raise RuntimeError(f"Table company_data is missing expected columns: {missing}")

            logger.info("JSONB table setup complete with data_schema column.")
            logger.debug(f"Table company_data columns: {column_names}")

        except Exception as e:
            logger.error(f"❌ Error setting up JSONB table: {e}", exc_info=True)
            conn.rollback()
            raise


def get_or_create_company_data(company_id):
    """Gets existing company data and schema or creates a new empty structure."""
    with get_db_connection() as (conn, engine):
        try:
            result = conn.execute(
                text("SELECT data, data_schema FROM public.company_data WHERE company_id = :company_id"),
                {"company_id": company_id}
            ).fetchone()

            if result:
                company_data, data_schema = result
                logger.info(f"Retrieved existing data and schema for company_id {company_id}")
                return company_data, data_schema
            else:
                empty_data = {}
                empty_schema = {}
                conn.execute(
                    text(
                        "INSERT INTO public.company_data (company_id, data, data_schema)"
                        " VALUES (:company_id, :data, :data_schema)"),
                    {"company_id": company_id, "data": json.dumps(empty_data), "data_schema": json.dumps(empty_schema)}
                )
                conn.commit()
                logger.info(f"Created new data and schema structure for company_id {company_id}")
                return empty_data, empty_schema

        except SQLAlchemyError as e:
            logger.error(f"❌ Error working with company data: {e}")
            conn.rollback()
            raise


def update_company_jsonb(company_id, data, data_schema):
    """Updates the JSONB data and schema for a company."""
    with get_db_connection() as (conn, engine):
        try:
            conn.execute(
                text(
                    "UPDATE public.company_data SET data = :data,"
                    " data_schema = :data_schema WHERE company_id = :company_id"),
                {"company_id": company_id, "data": json.dumps(data), "data_schema": json.dumps(data_schema)}
            )
            conn.commit()
            logger.info(f"Updated JSONB data and schema for company_id {company_id}")
        except SQLAlchemyError as e:
            logger.error(f"❌ Error updating company data: {e}")
            conn.rollback()
            raise


def infer_category_schema(df):
    """Infers the schema for a DataFrame, mapping Pandas types to SQL-like types."""
    schema = {}
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            schema[col] = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            schema[col] = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            schema[col] = "BOOLEAN"
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            schema[col] = "TEXT"
        else:
            schema[col] = "UNKNOWN"
    return schema


def process_excel_file_to_jsonb(filepath, company_id):
    """Processes an Excel file, stores data in JSONB format, and infers schema."""
    logger.info(f"Processing file for company_id {company_id}: {filepath}")

    company_data, data_schema = get_or_create_company_data(company_id)

    try:
        excel_data = pd.read_excel(filepath, sheet_name=None, engine='openpyxl')

        for sheet_name, df in excel_data.items():
            # Remove empty rows
            df = df.dropna(how='all')
            # Replace NaN values with None for JSON compatibility
            nan_count = df.isna().sum().sum()
            df = df.where(df.notna(), None)
            logger.info(f"Replaced {nan_count} NaN values with None in sheet '{sheet_name}'")
            if df.empty:
                logger.warning(f"Sheet '{sheet_name}' is empty after removing empty rows. Skipping.")
                continue

            sanitized_sheet_name = sanitize_name(sheet_name)
            data_category = sanitized_sheet_name

            # Infer schema for this category
            if not df.empty:
                category_schema = infer_category_schema(df)
                if data_category not in data_schema:
                    data_schema[data_category] = category_schema
                else:
                    # Update existing schema, preserving known types
                    for col, new_type in category_schema.items():
                        if col not in data_schema[data_category]:
                            data_schema[data_category][col] = new_type
                        elif new_type == "INTEGER" and data_schema[data_category][col] == "FLOAT":
                            data_schema[data_category][col] = "INTEGER"  # Prefer INTEGER if all values are whole
                        elif new_type != "UNKNOWN" and data_schema[data_category][col] == "UNKNOWN":
                            data_schema[data_category][col] = new_type

            # Initialize category if it doesn't exist
            if data_category not in company_data:
                company_data[data_category] = []

            # Convert records to JSON-serializable format
            records = df.to_dict(orient='records')
            serializable_records = []
            for record in records:
                serializable_record = {}
                for key, value in record.items():
                    if isinstance(value, (pd.Timestamp, datetime.datetime)):
                        # Convert Timestamp or datetime to ISO 8601 string
                        serializable_record[key] = value.isoformat()
                    elif pd.isna(value):
                        # Ensure NaN/None values are handled as None
                        serializable_record[key] = None
                    else:
                        serializable_record[key] = value
                serializable_records.append(serializable_record)

            logger.info(f"Adding {len(serializable_records)} records to {data_category} for company_id {company_id}")

            # Append new records to the category
            company_data[data_category].extend(serializable_records)

        update_company_jsonb(company_id, company_data, data_schema)

        return {
            "status": "success",
            "records_added": {sheet_name: len(df) for sheet_name, df in excel_data.items() if
                              not df.dropna(how='all').empty},
            "schema_updated": data_schema
        }

    except ValueError as e:
        logger.error(f"❌ Invalid Excel file format for {filepath}: {e}", exc_info=True)
        return {"status": "error", "message": f"Invalid Excel file format: {str(e)}"}
    except FileNotFoundError:
        logger.error(f"❌ File not found: {filepath}", exc_info=True)
        return {"status": "error", "message": f"File not found: {filepath}"}
    except ImportError as e:
        logger.error(f"❌ Required Excel engine not installed: {e}", exc_info=True)
        return {"status": "error", "message": f"Required Excel engine not installed: {str(e)}"}
    except Exception as e:
        logger.error(f"❌ Error processing file {filepath}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def get_jsonb_data_summary(company_id=None):
    """Returns a summary of the JSONB data and schema in the database."""
    with get_db_connection() as (conn, engine):
        try:
            if company_id:
                query = text(
                    "SELECT company_id, data, data_schema FROM public.company_data WHERE company_id = :company_id")
                result = conn.execute(query, {"company_id": company_id}).fetchone()
                if not result:
                    return {"message": f"No data found for company_id {company_id}"}

                company_id, company_data, data_schema = result
                summary = {
                    "company_id": company_id,
                    "categories": {},
                    "schema": data_schema
                }

                for category, items in company_data.items():
                    if isinstance(items, list):
                        summary["categories"][category] = {
                            "count": len(items),
                            "sample": items[0] if items else None
                        }
                    else:
                        summary["categories"][category] = {
                            "type": type(items).__name__,
                            "value": items if not isinstance(items, dict) else "nested_object"
                        }

                return summary
            else:
                query = text("""
                    SELECT company_id, 
                           jsonb_object_keys(data) as category, 
                           jsonb_array_length(data->jsonb_object_keys(data)) as count,
                           data_schema
                    FROM public.company_data
                """)
                result = conn.execute(query).fetchall()

                summary = {}
                for row in result:
                    company_id, category, count, data_schema = row
                    if company_id not in summary:
                        summary[company_id] = {"categories": {}, "schema": data_schema}
                    summary[company_id]["categories"][category] = count

                return summary

        except SQLAlchemyError as e:
            logger.error(f"❌ Error getting JSONB data summary: {e}")
            return {"error": str(e)}


# --- Main Execution Block ---
if __name__ == "__main__":
    excel_files_directory = './excel_files'
    company_id = 2  # Changed from 1 to 2 for new company data

    if not os.path.isdir(excel_files_directory):
        logger.error(f"Error: Directory not found: {excel_files_directory}")
        exit(1)

    setup_jsonb_table()

    xlsx_files = glob.glob(os.path.join(excel_files_directory, '*.xlsx'))

    if not xlsx_files:
        logger.warning(f"No .xlsx files found in directory: {excel_files_directory}")
        exit(0)

    logger.info(f"Found {len(xlsx_files)} Excel files to process.")

    all_results = {}

    for file_path in xlsx_files:
        file_results = process_excel_file_to_jsonb(file_path, company_id)
        all_results[os.path.basename(file_path)] = file_results

    logger.info("\n--- Processing Summary ---")
    for filename, results in all_results.items():
        logger.info(f"File: {filename}")
        if results.get('status') == 'success':
            for sheet_name, count in results.get('records_added', {}).items():
                logger.info(f"  Sheet '{sheet_name}': {count} records added.")
            logger.info(f"  Schema: {json.dumps(results.get('schema_updated', {}), indent=2)}")
        else:
            logger.info(f"  Processing failed: {results.get('message', 'Unknown error')}")

    logger.info("\n--- JSONB Data Summary ---")
    summary = get_jsonb_data_summary(company_id)
    logger.info(f"Company ID {company_id} data summary:")
    for category, info in summary.get('categories', {}).items():
        if isinstance(info, dict) and 'count' in info:
            logger.info(f"  {category}: {info['count']} records")
            if info['sample']:
                logger.info(f"  Sample: {json.dumps(info['sample'], indent=2)[:100]}...")
    logger.info(f"Schema: {json.dumps(summary.get('schema', {}), indent=2)}")
    logger.info("--- End of Summary ---")