import psycopg2
import psycopg2.extras
import re
import os
import json

# --- DATABASE CONNECTION DETAILS ---
# Please update these with your actual PostgreSQL connection details.
DB_SETTINGS = {
    "dbname": "tpch",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": "5432"
}

# --- CONFIGURATION ---
# The name of the SQL file containing the queries.
SQL_FILE_PATH = r"C:\Users\suhas\Downloads\E0261_P11\query_plans\queries.sql"

# The directory where the output JSON plans will be saved.
OUTPUT_DIR = r"C:\Users\suhas\Downloads\E0261_P11\query_plans\output_plans"


def parse_sql_file(filepath):
    """
    Parses the SQL file to extract individual queries and their IDs.

    The function assumes queries are separated by a specific comment block
    format that contains the 'Query ID:'.

    Args:
        filepath (str): The path to the .sql file.

    Returns:
        list: A list of tuples, where each tuple contains (query_id, query_text).
              Returns an empty list if the file cannot be read.
    """
    print(f"Attempting to read queries from '{filepath}'...")
    queries = []
    try:
        with open(filepath, 'r') as f:
            content = f.read()

        # Regex to find the query ID in the comment block and the SQL that follows it.
        # It captures the ID and the query text until the next comment block or end of file.
        pattern = re.compile(
            r"-- =+.*?\n-- Query ID: ([\w-]+).*?\n-- =+.*?\n(.*?)(?=\n-- =+|$)",
            re.DOTALL
        )

        matches = pattern.findall(content)

        for match in matches:
            query_id = match[0].strip()
            # Clean up the query text by removing trailing whitespace and semicolons
            query_text = match[1].strip().rstrip(';')
            if query_text:  # Ensure we don't add empty queries
                queries.append((query_id, query_text))

        print(f"Successfully parsed {len(queries)} queries.")
        return queries

    except FileNotFoundError:
        print(f"ERROR: The file '{filepath}' was not found.")
        return []
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        return []


def get_query_plan(conn, query_text):
    """
    Executes EXPLAIN on a given query and returns the plan in JSON format.

    Args:
        conn: An active psycopg2 connection object.
        query_text (str): The SQL query to analyze.

    Returns:
        dict: The query plan as a Python dictionary (from JSON), or None if an error occurs.
    """
    # We use EXPLAIN with ANALYZE to get the actual execution plan, not just the estimate.
    # BUFFERS provides details on memory usage.
    explain_query = f"EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) {query_text};"

    with conn.cursor() as cur:
        try:
            cur.execute(explain_query)
            # The result of EXPLAIN (FORMAT JSON) is a single row with a single column.
            result = cur.fetchone()
            if result:
                # The plan is stored in the first element of the first (and only) row.
                # psycopg2 automatically deserializes the JSON from PostgreSQL into a Python object.
                return result[0]
        except psycopg2.Error as e:
            print(f"\n--- PostgreSQL Error ---")
            print(f"Error executing EXPLAIN for a query.")
            print(f"Details: {e}")
            print(f"Problematic Query Fragment:\n{query_text[:200]}...\n")
            # We must rollback the transaction in case of an error.
            conn.rollback()
            return None


def main():
    """
    Main function to connect to the database, process queries, and save the plans.
    """
    # 1. Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        print(f"Creating output directory: '{OUTPUT_DIR}'")
        os.makedirs(OUTPUT_DIR)

    # 2. Parse the SQL file to get all queries
    queries_to_run = parse_sql_file(SQL_FILE_PATH)
    if not queries_to_run:
        print("No queries to process. Exiting.")
        return

    conn = None
    try:
        # 3. Connect to the PostgreSQL database
        print(f"\nConnecting to PostgreSQL database '{DB_SETTINGS['dbname']}'...")
        conn = psycopg2.connect(**DB_SETTINGS)
        print("Connection successful.")

        # 4. Process each query
        for i, (query_id, query_text) in enumerate(queries_to_run):
            print(f"\n[{i+1}/{len(queries_to_run)}] Processing Query ID: {query_id}")

            plan = get_query_plan(conn, query_text)

            if plan:
                # 5. Save the plan to a JSON file
                output_filename = os.path.join(OUTPUT_DIR, f"{query_id}.json")
                try:
                    with open(output_filename, 'w') as f:
                        # Use indent=2 for a nicely formatted, readable JSON file
                        json.dump(plan, f, indent=2)
                    print(f"Successfully saved plan to '{output_filename}'")
                except IOError as e:
                    print(f"Error writing to file '{output_filename}': {e}")

    except psycopg2.OperationalError as e:
        print(f"DATABASE CONNECTION FAILED: Could not connect to the database.")
        print(f"Please check your DB_SETTINGS in the script.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # 6. Ensure the database connection is closed
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == '__main__':
    main()
