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
# Define all sources to process: (label, sql file path, output directory)
SOURCES = [
    (
        "original",
        r"C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql",
        r"C:\\Users\\suhas\\Downloads\\E0261_P11\\original_query_plans",
    ),
    (
        "rewritten",
        r"C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\rewritten_queries.sql",
        r"C:\\Users\\suhas\\Downloads\\E0261_P11\\rewritten_query_plans",
    ),
    (
        "mutated",
        r"C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\mutated_queries.sql",
        r"C:\\Users\\suhas\\Downloads\\E0261_P11\\mutated_query_plans",
    ),
]

# --- CLEANING CONFIG ---
# Keys to remove from the PostgreSQL EXPLAIN (FORMAT JSON) output. These are execution-specific
# and not part of the logical structure. We will also lift the root 'Plan' node to be the root.
KEYS_TO_REMOVE = [
    "Planning Time",
    "Execution Time",
    "Actual Rows",
    "Actual Loops",
    "Actual Startup Time",
    "Actual Total Time",
    "Shared Hit Blocks",
    "Shared Read Blocks",
    "Shared Dirtied Blocks",
    "Shared Written Blocks",
    "Local Hit Blocks",
    "Local Read Blocks",
    "Local Dirtied Blocks",
    "Local Written Blocks",
    "Temp Read Blocks",
    "Temp Written Blocks",
    "I/O Read Time",
    "I/O Write Time",
    # We will process the 'Plan' node but remove the key itself to lift the root plan node up.
    "Plan",
]


def _clean_plan_node(node):
    """
    Recursively remove KEYS_TO_REMOVE from a plan node while preserving structure.

    Args:
        node: dict | list | primitive

    Returns:
        Cleaned node
    """
    if isinstance(node, dict):
        cleaned = {}
        for k, v in node.items():
            if k not in KEYS_TO_REMOVE:
                cleaned[k] = _clean_plan_node(v)
        return cleaned
    if isinstance(node, list):
        return [_clean_plan_node(item) for item in node]
    return node


def extract_and_clean_plan(plan_data):
    """
    Takes the raw EXPLAIN (FORMAT JSON) result and returns a cleaned plan dict.

    PostgreSQL returns a list with a single dict containing keys like 'Plan',
    'Planning Time', 'Execution Time', etc. We lift the dict under 'Plan' to the root
    and drop execution-specific keys.

    Args:
        plan_data: Python object as returned by psycopg2 for EXPLAIN (FORMAT JSON)

    Returns:
        dict | None: Cleaned plan dictionary, or None if structure is unexpected
    """
    try:
        if plan_data and isinstance(plan_data, list):
            root = plan_data[0] if plan_data else None
            if isinstance(root, dict):
                plan_root = root.get("Plan", {})
                return _clean_plan_node(plan_root)
        # Unexpected shape
        return None
    except Exception as e:
        print(f"Error cleaning plan structure: {e}")
        return None


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
        object: The raw plan as a Python object (list/dict) from PostgreSQL JSON, or None on error.
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
                # The plan is stored in the first element of the single row returned.
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
    Connects once to PostgreSQL, then for each configured source (original/rewritten/mutated):
    - Ensures the output directory exists
    - Parses queries from the SQL file
    - Skips IDs already exported in the output folder
    - Runs EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS)
    - Saves each plan as <QueryID>.json in that folder
    """
    conn = None
    try:
        print(f"\nConnecting to PostgreSQL database '{DB_SETTINGS['dbname']}'...")
        conn = psycopg2.connect(**DB_SETTINGS)
        print("Connection successful.")

        for label, sql_path, out_dir in SOURCES:
            print("\n" + "-" * 80)
            print(f"Processing source: {label}")
            print(f"SQL file       : {sql_path}")
            print(f"Output folder  : {out_dir}")

            # Ensure output directory exists
            if not os.path.exists(out_dir):
                print(f"Creating output directory: '{out_dir}'")
                os.makedirs(out_dir)

            # Parse queries from this source
            queries_to_run = parse_sql_file(sql_path)
            if not queries_to_run:
                print(f"No queries parsed from '{sql_path}'. Skipping.")
                continue

            # Skip already processed ones in this output folder.
            # Consider both legacy <QueryID>.json and new <QueryID>_<label>.json names as processed
            json_files = [f for f in os.listdir(out_dir) if f.endswith('.json')]
            processed_ids = set()
            for fname in json_files:
                base = fname[:-5]  # strip .json
                if base.endswith(f"_{label}"):
                    processed_ids.add(base[: -(len(label) + 1)])
                else:
                    processed_ids.add(base)

            pending = [(qid, qtext) for qid, qtext in queries_to_run if qid not in processed_ids]

            if not pending:
                print("All queries in this source already processed. Nothing to do.")
                continue

            total = len(pending)
            for i, (query_id, query_text) in enumerate(pending, start=1):
                print(f"\n[{label}] [{i}/{total}] Query ID: {query_id}")
                raw_plan = get_query_plan(conn, query_text)

                if raw_plan is not None:
                    cleaned_plan = extract_and_clean_plan(raw_plan)
                    if cleaned_plan is None:
                        print("Warning: Plan returned but structure unexpected; saving raw plan.")
                        payload = raw_plan
                    else:
                        payload = cleaned_plan
                    # Save as <QueryID>_<label>.json to disambiguate sources
                    output_filename = os.path.join(out_dir, f"{query_id}_{label}.json")
                    try:
                        with open(output_filename, 'w') as f:
                            json.dump(payload, f, indent=2)
                        print(f"Saved: {output_filename}")
                    except IOError as e:
                        print(f"Error writing to file '{output_filename}': {e}")
                else:
                    print("No plan returned; query skipped.")

    except psycopg2.OperationalError as e:
        print("DATABASE CONNECTION FAILED: Could not connect to the database.")
        print("Please check your DB_SETTINGS in the script.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == '__main__':
    main()
