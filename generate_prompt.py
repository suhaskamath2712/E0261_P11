import os
import sys

# Paths must match FileIO.java constants
BASE = r"C:\Users\suhas\Downloads\E0261_P11"
ORIGINAL_DIR = os.path.join(BASE, "original_query_plans")
REWRITTEN_DIR = os.path.join(BASE, "rewritten_query_plans")
MUTATED_DIR = os.path.join(BASE, "mutated_query_plans")

CHOICES = {
    'o': ('Original', ORIGINAL_DIR),
    'r': ('Rewritten', REWRITTEN_DIR),
    'm': ('Mutated', MUTATED_DIR),
}

PROMPT = """Compare these two query plans and if they are equivalent, output only the following:\n
            A list of Apache calcite transformations that can map from the first plan to the second plan.\n
            DO NOT OUTPUT ANYTHING ELSE. DO NOT GIVE ANY EXPLANATIONS.\n
            Output transformations from the following supported list only:\n
            If no transformations can map the first plan to the second plan, output "No transformations found".\n"""

SUPPORTED_CALCITE_TRANSFORMATIONS = """
- Projection rules
  - `ProjectMergeRule`
  - `ProjectRemoveRule`
  - `ProjectJoinTransposeRule`
  - `ProjectFilterTransposeRule`
  - `ProjectSetOpTransposeRule`
  - `ProjectTableScanRule` (resolved reflectively; version-dependent)

- Filter rules
  - `FilterMergeRule`
  - `FilterProjectTransposeRule`
  - `FilterJoinRule` (maps to `FILTER_INTO_JOIN`)
  - `FilterAggregateTransposeRule`
  - `FilterWindowTransposeRule`
  - `FilterTableScanRule` (resolved reflectively; version-/adapter-dependent)

- Join rules
  - `JoinCommuteRule`
  - `JoinAssociateRule`
  - `JoinPushExpressionsRule`
  - `JoinConditionPushRule`
  - `SemiJoinRule` (resolved reflectively; version-dependent)

- Aggregate rules
  - `AggregateProjectPullUpConstantsRule`
  - `AggregateRemoveRule`
  - `AggregateJoinTransposeRule`
  - `AggregateUnionTransposeRule`
  - `AggregateProjectMergeRule`
  - `AggregateCaseToFilterRule`

- Sort & limit rules
  - `SortRemoveRule`
  - `SortUnionTransposeRule`
  - `SortProjectTransposeRule`
  - `SortJoinTransposeRule`

- Set operation rules
  - `UnionMergeRule`
  - `UnionPullUpConstantsRule`
  - `IntersectToDistinctRule`
  - `MinusToDistinctRule`

- Window rules
  - `ProjectWindowTransposeRule`
  - `FilterWindowTransposeRule`

- Other/general rules
  - `ValuesReduceRule`
  - `ReduceExpressionsRule`
  - `PruneEmptyRules`
"""


def list_query_ids(folder):
    try:
        files = os.listdir(folder)
    except FileNotFoundError:
        return []
    ids = []
    for f in files:
        if f.lower().endswith('.json'):
            ids.append(f[:-5])
    ids.sort()
    return ids


def ask_choice(prompt, disallow=None):
    while True:
        s = input(prompt).strip().lower()
        if s == 'q' or s == 'quit':
            print('Quitting.')
            sys.exit(0)
        if s not in CHOICES:
            print("Invalid choice. Enter 'o' (original), 'r' (rewritten), or 'm' (mutated).")
            continue
        if disallow is not None and s == disallow:
            print("Choice must be different from the previous selection.")
            continue
        return s


def ask_query_id(valid_ids):
    if not valid_ids:
        print("No query files found in the selected folder. Exiting.")
        sys.exit(1)
    print("Available query IDs:")
    for idx, q in enumerate(valid_ids, start=1):
        print(f"  {idx}. {q}")
    while True:
        s = input("Enter the query number (or 'q' to quit): ").strip().lower()
        if s == 'q' or s == 'quit':
            print('Quitting.')
            sys.exit(0)
        if not s.isdigit():
            print("Please enter a numeric index corresponding to the query list above.")
            continue
        i = int(s)
        if i < 1 or i > len(valid_ids):
            print(f"Please enter a number between 1 and {len(valid_ids)}.")
            continue
        return valid_ids[i-1]


def main():
    print("Select list 1: Enter 'o' (original), 'r' (rewritten), or 'm' (mutated). Enter 'q' to quit.")
    c1 = ask_choice('List 1 choice (o/r/m): ')
    print(f"Selected List 1: {CHOICES[c1][0]}")

    print("\nSelect list 2 (must be different from list 1).")
    c2 = ask_choice('List 2 choice (o/r/m): ', disallow=c1)
    print(f"Selected List 2: {CHOICES[c2][0]}")

    folder1 = CHOICES[c1][1]
    folder2 = CHOICES[c2][1]

    ids1 = set(list_query_ids(folder1))
    ids2 = set(list_query_ids(folder2))

    common = sorted(list(ids1 & ids2))
    if not common:
        print("No common query IDs found between the selected lists.")
        print(f"Count in {CHOICES[c1][0]}: {len(ids1)}; in {CHOICES[c2][0]}: {len(ids2)}")
        sys.exit(1)

    print(f"\nFound {len(common)} common query IDs between {CHOICES[c1][0]} and {CHOICES[c2][0]}.")
    qid = ask_query_id(common)

    # Build file paths
    p1 = os.path.join(folder1, qid + '.json')
    p2 = os.path.join(folder2, qid + '.json')

    print("\nSummary:")
    print(f"  List 1: {CHOICES[c1][0]} -> {folder1}")
    print(f"  List 2: {CHOICES[c2][0]} -> {folder2}")
    print(f"  Selected Query ID: {qid}")

    print("\nPaths to plan files:")
    print(f"  {p1}")
    print(f"  {p2}")

    # Helper to read and pretty-print JSON when possible
    def print_json_file(path, label):
        print(f"\n--- {label}: {os.path.basename(path)} ---")
        if not os.path.exists(path):
            print(f"File not found: {path}")
            return
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                data = fh.read()
        except Exception as e:
            print(f"Failed reading file: {e}")
            return
        # Try to pretty-print JSON
        try:
            import json
            obj = json.loads(data)
            pretty = json.dumps(obj, indent=2, ensure_ascii=False)
            print(pretty)
        except Exception:
            # Not valid JSON or json lib failed; print raw content
            print(data)


    print("\n--- Prompt for comparing the two plans ---")
    print(PROMPT)
    print(SUPPORTED_CALCITE_TRANSFORMATIONS)
    print_json_file(p1, f"Plan 1: ")
    print_json_file(p2, f"Plan 2: ")

if __name__ == '__main__':
    main()
