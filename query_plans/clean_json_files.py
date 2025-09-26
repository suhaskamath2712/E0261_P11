import os
import json

TARGET_DIRECTORY = "C:\\Users\\suhas\\Downloads\\E0261_P11\\query_plans\\rewritten_query_plans"

# These are the keys that will be recursively removed from the JSON plan.
# They represent execution-specific statistics, not the logical plan structure.
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
    "Plan" # We will process the 'Plan' node but remove the key itself to lift the root plan node up.
]

def clean_plan_node(node):
    """
    Recursively traverses a JSON query plan node and removes specified keys.

    Args:
        node (dict or list): The current node in the JSON tree.

    Returns:
        The cleaned node.
    """
    if isinstance(node, dict):
        # Create a new dictionary to hold the cleaned items
        cleaned_dict = {}
        for key, value in node.items():
            if key not in KEYS_TO_REMOVE:
                # Recursively clean the value
                cleaned_dict[key] = clean_plan_node(value)
        return cleaned_dict
    elif isinstance(node, list):
        # Recursively clean each item in the list
        return [clean_plan_node(item) for item in node]
    else:
        # Return the value as is if it's not a dict or list
        return node

def process_and_overwrite_json_file(file_path):
    """
    Reads a JSON query plan, cleans it, and overwrites the original file.

    Args:
        file_path (str): Path to the JSON file to be cleaned in-place.
    """
    try:
        # First, read the entire file into memory
        with open(file_path, 'r') as f:
            data = json.load(f)

        cleaned_plan = None
        # The actual plan is usually nested inside a list.
        if data and isinstance(data, list) and len(data) > 0:
            plan_root = data[0]
            cleaned_plan = clean_plan_node(plan_root.get('Plan', {}))
        else:
            print(f"Warning: Could not find a valid plan structure in '{file_path}'. Skipping.")
            return

        # Now, open the same file in write mode ('w') which truncates it,
        # and write the cleaned content back.
        with open(file_path, 'w') as f:
            json.dump(cleaned_plan, f, indent=2)
        print(f"Successfully cleaned and replaced '{file_path}'")

    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{file_path}'. Skipping.")
    except IOError as e:
        print(f"Error processing file '{file_path}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred for file '{file_path}': {e}")


def main():
    """
    Main function to orchestrate the in-place cleaning of all JSON files in a directory.
    """
    target_dir = TARGET_DIRECTORY

    if not os.path.isdir(target_dir):
        print(f"Error: The specified path '{target_dir}' is not a valid directory.")
        print("Please update the TARGET_DIRECTORY variable at the top of the script.")
        return

    # Process each file in the target directory
    print(f"Starting in-place cleaning for JSON files in '{target_dir}'...")
    for filename in os.listdir(target_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(target_dir, filename)
            process_and_overwrite_json_file(file_path)
    print("Cleaning process complete.")

if __name__ == '__main__':
    main()

