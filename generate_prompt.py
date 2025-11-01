"""
generate_prompt.py
===================

Interactive helper to select two different query-plan lists (original/rewritten/mutated),
choose a common Query ID between them, and print the two plan JSONs.

Goals
-----
- Prompt the user for two lists ("o" -> original, "r" -> rewritten, "m" -> mutated) ensuring
    the second choice is different from the first.
- Discover available query IDs by scanning the selected directories for JSON files.
- The project stores JSON files with suffixes in filenames, e.g., "A1_original.json".
    This script normalizes IDs by stripping trailing "_original", "_rewritten", "_mutated"
    so that the displayed Query IDs are clean (e.g., "A1").
- After selecting a Query ID, resolve the exact filename in each folder and pretty-print
    the two JSON plan files.

Conventions and paths
---------------------
- The paths used here mirror the constants in Java's FileIO.java so that the same folders
    are used consistently:
    - C:\\Users\\suhas\\Downloads\\E0261_P11\\original_query_plans
    - C:\\Users\\suhas\\Downloads\\E0261_P11\\rewritten_query_plans
    - C:\\Users\\suhas\\Downloads\\E0261_P11\\mutated_query_plans

Inputs/Outputs
--------------
- Input: interactive choices typed by the user in the terminal.
- Output: human-readable listing of available Query IDs, a summary of selections,
    and pretty-printed JSON for the two selected files (falls back to raw text if
    JSON parsing fails).

Edge cases handled
------------------
- Missing folders or empty directories: results in empty lists; if no common IDs exist,
    the script reports and exits.
- Mixed filename styles (e.g., both "A1.json" and "A1_original.json"): the script
    strips known suffixes to build a canonical ID and matches by equality. If multiple
    files resolve to the same canonical ID, the first match in directory order is used.
- Invalid JSON files: falls back to printing raw content.
- Users can type 'q' or 'quit' at any prompt to exit.

This script prints a short prompt and a list of supported Calcite transformation names
to guide downstream usage where these two JSON plans might be compared by an LLM.
"""

import os
import sys

# Optional clipboard support: use pyperclip if available; otherwise try Windows 'clip'
try:
    import pyperclip as _clipboard
    _HAS_CLIPBOARD = True
except Exception:
    _clipboard = None
    _HAS_CLIPBOARD = False

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

PROMPT = """Compare these two query plans and if they are equivalent, output true. Otherwise, output false.\n
            If true, only the following:\n
            A list of Apache calcite transformations that can map from the first plan to the second plan.\n
            DO NOT OUTPUT ANYTHING ELSE. DO NOT GIVE ANY EXPLANATIONS.\n
            Output transformations from the following supported list only:\n
            If no transformations can map the first plan to the second plan, output "No transformations found".\n
            If no transformations are needed (plans are identical), output "No transformations needed".\n
            """

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

- Join rules
  - `JoinCommuteRule`
  - `JoinAssociateRule`
  - `JoinPushExpressionsRule`
  - `JoinConditionPushRule`

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
"""


def list_query_ids(folder):
    """Return a sorted list of canonical Query IDs present in a folder.

    Behavior
    --------
    - Scans the folder for files ending with ".json".
    - Strips a known trailing suffix ("_original", "_rewritten", "_mutated") from the
      file stem to produce a canonical Query ID. Example: "A1_original.json" -> "A1".
    - Returns the sorted unique list of these canonical IDs.

    Parameters
    ----------
    folder : str
        Absolute path to a directory containing plan JSON files.

    Returns
    -------
    list[str]
        Sorted canonical Query IDs.
    """
    try:
        files = os.listdir(folder)
    except FileNotFoundError:
        return []
    ids = []
    for f in files:
        if not f.lower().endswith('.json'):
            continue
        name = f[:-5]
        # strip known suffixes like _original, _rewritten, _mutated
        lower = name.lower()
        for suf in ('_original', '_rewritten', '_mutated'):
            if lower.endswith(suf):
                name = name[: -len(suf)]
                break
        ids.append(name)
    ids.sort()
    return ids


def ask_choice(prompt, disallow=None):
    """Prompt for 'o'/'r'/'m' with validation and optional disallow value.

    Parameters
    ----------
    prompt : str
        Prompt to display to the user.
    disallow : str | None
        If provided, the returned choice must not equal this value (used to ensure
        List 2 differs from List 1).

    Returns
    -------
    str
        One of 'o', 'r', or 'm'. Typing 'q' or 'quit' exits the program.
    """
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
    """Prompt the user to select a Query ID by typing either its numeric index or the ID itself.

    Behavior
    --------
    - Displays the list of valid IDs with 1-based numbering.
    - Accepts either a number (index) or a string that matches an ID (case-insensitive).
    - 'q' or 'quit' exits.

    Parameters
    ----------
    valid_ids : list[str]
        The set of canonical Query IDs to choose from.

    Returns
    -------
    str
        The selected Query ID string.
    """
    if not valid_ids:
        print("No query files found in the selected folder. Exiting.")
        sys.exit(1)
    print("Available query IDs:")
    for idx, q in enumerate(valid_ids, start=1):
        print(f"  {idx}. {q}")
    # Build a case-insensitive lookup for convenience
    lower_map = {vid.lower(): vid for vid in valid_ids}
    while True:
        s = input("Enter the query number or ID (or 'q' to quit): ").strip()
        sl = s.lower()
        if sl in ('q', 'quit'):
            print('Quitting.')
            sys.exit(0)
        # Accept numeric index
        if sl.isdigit():
            i = int(sl)
            if 1 <= i <= len(valid_ids):
                return valid_ids[i-1]
            print(f"Please enter a number between 1 and {len(valid_ids)}.")
            continue
        # Accept ID string (case-insensitive)
        if sl in lower_map:
            return lower_map[sl]
        print("Invalid input. Enter a valid index or one of the IDs shown above (e.g., A1, U1, Alaap).")


def find_plan_file(folder: str, qid: str) -> str | None:
    """Return the full path to the JSON file in 'folder' that matches 'qid'.

    Matching is done by stripping known suffixes from each filename in the folder
    and comparing the canonical basename to the provided 'qid'. Returns the first
    match found or None if no matching file exists.
    """
    try:
        files = os.listdir(folder)
    except Exception:
        return None
    for f in files:
        if not f.lower().endswith('.json'):
            continue
        name = f[:-5]
        lower = name.lower()
        base = name
        for suf in ('_original', '_rewritten', '_mutated'):
            if lower.endswith(suf):
                base = name[: -len(suf)]
                break
        if base == qid:
            return os.path.join(folder, f)
    return None


def read_raw(path: str) -> str | None:
    """Read a file as raw UTF-8 text; return None if missing or on error."""
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            return fh.read()
    except Exception:
        return None


def pretty_or_raw(text: str | None) -> str:
    """Return pretty-formatted JSON string when possible; otherwise the original text or a placeholder."""
    if text is None:
        return "(file not found)"
    try:
        import json
        obj = json.loads(text)
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return text


def build_clipboard_text(prompt_text: str, transformations_text: str, path1: str | None, path2: str | None) -> str:
    """Compose clipboard text in required order: Prompt, Transformations, Plan 1, JSON 1, Plan 2, JSON 2."""
    raw1 = read_raw(path1) if path1 else None
    raw2 = read_raw(path2) if path2 else None
    plan1_body = pretty_or_raw(raw1)
    plan2_body = pretty_or_raw(raw2)
    return (
        prompt_text.strip() + "\n"
        + transformations_text.strip() + "\n\n"
        + "Plan 1\n" + plan1_body + "\n\n"
        + "Plan 2\n" + plan2_body + "\n"
    )


def copy_to_clipboard(text: str) -> bool:
    """Try to copy text to clipboard using pyperclip or Windows clip; return True on success."""
    # Attempt via pyperclip
    if _HAS_CLIPBOARD and _clipboard is not None:
        try:
            _clipboard.copy(text)
            return True
        except Exception:
            pass
    # Fallback: Windows clip.exe
    try:
        import subprocess
        completed = subprocess.run(['clip'], input=text, text=True, check=True)
        return completed.returncode == 0
    except Exception:
        return False


def main():
    """Entry point for the interactive prompt.

     Steps
     -----
     1) Ask the user to choose List 1 ('o'/'r'/'m').
     2) Ask the user to choose List 2 (must differ from List 1).
     3) Compute the intersection of Query IDs available in both selected lists and prompt
         the user to choose one by numeric index.
     4) Resolve the actual JSON filenames corresponding to the chosen ID in each folder
         and pretty-print each JSON.
     """
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

    # Locate the exact plan files in each folder (filenames may include suffixes)
    p1 = find_plan_file(folder1, qid)
    p2 = find_plan_file(folder2, qid)

    print("\nSummary:")
    print(f"  List 1: {CHOICES[c1][0]} -> {folder1}")
    print(f"  List 2: {CHOICES[c2][0]} -> {folder2}")
    print(f"  Selected Query ID: {qid}")

    print("\nPaths to plan files:")
    print(f"  {p1 if p1 else '(not found)'}")
    print(f"  {p2 if p2 else '(not found)'}")

    # Build clipboard text (Prompt, Supported Transformations, Plan 1 JSON, Plan 2 JSON)
    final_text = build_clipboard_text(PROMPT, SUPPORTED_CALCITE_TRANSFORMATIONS, p1, p2)
    if final_text and copy_to_clipboard(final_text):
        print("Prompt, supported transformations, and plan JSONs copied to clipboard.")
    else:
        print("Clipboard not available. Printing the full content below in the required order; copy manually if needed:\n")
        print(final_text)

if __name__ == '__main__':
    main()
