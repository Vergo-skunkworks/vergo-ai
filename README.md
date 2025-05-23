# RAG with Excel Files: Querying Data with SQL and LLMs

One of the most common questions I receive from clients and the AI community is: **How can we build effective Retrieval-Augmented Generation (RAG) pipelines using Excel files?** Many are looking for practical resources to get started with RAG for Excel data. To address this, I’m sharing my experience with a simple yet powerful RAG method that leverages SQL and function calling.

**In this guide, we’ll walk through setting up a RAG pipeline that uses SQL to query an Excel file and answers user questions with the help of a large language model (LLM).**

## Why Excel for RAG?

Excel files are fundamentally different from text-based data sources, as traditional chunking strategies don’t apply to their tabular structure. However, Excel’s format is perfect for structured retrieval using SQL. LLMs, trained on vast amounts of SQL-related data, excel at generating accurate queries, even for complex, multi-table datasets. SQL also offers scalability and precision, reducing errors in retrieval and minimizing hallucinations in the final response.

Using a SQL-based agent for RAG is a robust and efficient approach. Let’s dive into the implementation.
## Sample Data

For this example, we’ll use a sample Excel file containing project data, such as cost centers, project managers, and contract details. Here’s a preview of the data structure from the file `Change Order Report Test Team Toolshed.xlsx`:

| cost_center | project_manager | customer_code | job_number | original_contract | change_orders | revised_contract |
|-------------|----------------|---------------|------------|-------------------|---------------|------------------|
| 2040        | 209012         | ACIC100       | 402400056  | 9629              | -2408.0       | 7221.0           |
| 2035        | 209024         | ROTH100       | 352400019  | 221100            | 1613.0        | 222713.0         |
| 4080        | 400013         | INSP200       | 802400022  | 120000            | 6594.0        | 126594.0         |
| 2034        | 209014         | LAMA610       | 342400332  | 133000            | 2675.0        | 135675.0         |
| 2060        | 109007         | CLEV100       | 602400108  | 21750             | -250.0        | 21500.0          |

The dataset contains 34 rows of project-related data, which we’ll use to demonstrate our RAG pipeline.

## Step 1: Loading and Preprocessing the Excel File

We’ll begin by loading the Excel file into a pandas DataFrame and performing some preprocessing to ensure the data is ready for SQL queries.

```python
import pandas as pd

# Load the Excel file into a DataFrame
dataframe = pd.read_excel('files/Change Order Report Test Team Toolshed.xlsx')
```

Next, we preprocess the data to make it easier to work with:

```python
# Drop the 'Date' column, which contains NaN values
df = dataframe.drop(columns=['Date'])

# Convert column names to lowercase for consistency
df.columns = df.columns.str.lower()
```

The DataFrame now has 34 rows and columns like `cost_center`, `project_manager`, `customer_code`, `job_number`, `original_contract`, `change_orders`, and `revised_contract`.

## Step 2: Storing Data in a PostgreSQL Database

To enable SQL queries, we’ll store the DataFrame in a PostgreSQL database hosted on Retool. First, we establish a connection:

```python
import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname='abc',
    user='def',
    password='ghi',
    host='jkl',
    port='mno'
)
```

Then, we use SQLAlchemy to write the DataFrame to a table named `change_order_reports`:

```python
from sqlalchemy import create_engine

# Create a SQLAlchemy engine
engine = create_engine('postgresql+psycopg2://abc:def/ghi')

# Write the DataFrame to the database
df.to_sql('change_order_reports', engine, schema='public', if_exists='replace', index=False)
```

This creates a table with 34 rows, ready for SQL queries.

## Step 3: Setting Up the LLM

We’ll use Google’s Gemini model (`gemini-1.5-flash`) to generate SQL queries and answer questions. First, configure the API key:

```python
import google.generativeai as genai
import dotenv
import os

# Load the API key from a .env file
dotenv.load_dotenv()
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))

# Create a Gemini client
gemini = genai.GenerativeModel("gemini-1.5-flash")
```

Test the model with a simple prompt:

```python
text = gemini.generate_content("Tell me something interesting about Pakistan").text
print(text)
# Output: Pakistan is home to the world's highest polo ground, located in Shandur...
```

## Step 4: Enabling SQL Queries with Function Calling

To allow the LLM to query the database, we define a function that executes SQL queries and returns results:

```python
def sql_query(query: str):
    """Run a SQL SELECT query on the PostgreSQL database and return results."""
    with engine.connect() as connection:
        return pd.read_sql_query(query, connection).to_dict(orient='records')
```

For the LLM to generate accurate queries, it needs the database schema. We query the schema dynamically and format it as JSON:

```python
import json
import pandas as pd

table_name = 'change_order_reports'
query = f"""
select 
    column_name, 
    data_type
from information_schema.columns
where table_schema = 'public' and table_name = '{table_name}'
order by ordinal_position;
"""
df = pd.read_sql(query, engine)
table_schema = {
    "table": table_name,
    "columns": [
        {"name": row['column_name'].lower(), "type": row['data_type']}
        for _, row in df.iterrows()
    ]
}
```

We then create a system prompt that includes the schema:

```python
system_prompt = f"""
You are an expert SQL analyst. When appropriate, generate SQL queries based on the user question and the database schema.
When you generate a query, use the 'sql_query' function to execute the query on the database and get the results.
Then, use the results to answer the user's question.

database_schema: [{json.dumps(table_schema)}]
""".strip()
```

## Step 5: Querying the Database with Gemini

We initialize a Gemini instance with the SQL query function and system prompt:

```python
sql_gemini = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    tools=[sql_query],
    system_instruction=system_prompt
)
```

Using Gemini’s automatic conversation management, we start a chat session and ask questions:

```python
chat = sql_gemini.start_chat(enable_automatic_function_calling=True)

# Ask about a specific contract amount
response = chat.send_message("What's the original_contract amount for job_number 402400056?").text
print(response)
# Output: The original contract amount for Job Number 402400056 is 9629.
```

More example queries:

```python
response = chat.send_message("What is the average original contract amount?").text
print(response)
# Output: The average original contract amount is 70218.5.

response = chat.send_message("What job has the highest original contract amount?").text
print(response)
# Output: The job with the highest original contract amount is job number 352400313.
```

## Manual Conversation Management (Optional)

For greater control, you can manage the conversation history manually. This involves generating a SQL query, executing it, and passing the results back to the LLM.
