"""Verify query ID inventory across original and rewritten SQL files.

Extracts all lines starting with `-- Query ID:` (case-sensitive) and builds:
 - original_ids: set of IDs in original_queries.sql
 - rewritten_ids: set of IDs in rewritten_queries.sql
 - union_ids: distinct logical IDs (strips trailing annotations like `(Rewritten)` or `(Corrected)` or `(Optimized)`)
 - removed_ids: known removed/deprecated IDs

Outputs a summary matching list_of_queries_2.txt expectations.
"""
from __future__ import annotations
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql_queries"
ORIGINAL = SQL_DIR / "original_queries.sql"
REWRITTEN = SQL_DIR / "rewritten_queries.sql"

REMOVED_CANONICAL = {
    "F1", "F2", "TPCH_Q11", "TPCH_Q12", "TPCH_Q14", "TPCH_Q16"
}

ID_LINE_PATTERN = re.compile(r"^--\s+Query ID:\s+(.*)$")

def normalize(id_text: str) -> str:
    # Remove parenthetical qualifiers and trim.
    cleaned = re.sub(r"\s*\(.*?\)", "", id_text).strip()
    return cleaned

def extract_ids(path: Path) -> list[str]:
    ids = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = ID_LINE_PATTERN.match(line)
        if m:
            raw = m.group(1).strip()
            ids.append(raw)
    return ids

def classify(union_ids: set[str]):
    groups = {
        "U": [], "O": [], "A": [], "N": [], "F": [], "MQ": [], "TPCH_Q": [], "ETPCH_Q": [], "LITHE": [], "Custom": []
    }
    custom_set = {"Alaap", "Nested_Test", "paper_sample"}
    for q in sorted(union_ids):
        if q in REMOVED_CANONICAL:
            continue
        if q.startswith("U"): groups["U"].append(q)
        elif q.startswith("O"): groups["O"].append(q)
        elif q.startswith("A") and q[1].isdigit(): groups["A"].append(q)
        elif q.startswith("N") and q[1].isdigit(): groups["N"].append(q)
        elif q.startswith("F") and q[1].isdigit(): groups["F"].append(q)
        elif q.startswith("MQ"): groups["MQ"].append(q)
        elif q.startswith("TPCH_Q"): groups["TPCH_Q"].append(q)
        elif q.startswith("ETPCH_Q"): groups["ETPCH_Q"].append(q)
        elif q.startswith("LITHE_"): groups["LITHE"].append(q)
        elif q in custom_set: groups["Custom"].append(q)
    return groups

def main():
    original_ids_raw = extract_ids(ORIGINAL)
    rewritten_ids_raw = extract_ids(REWRITTEN)

    original_ids = {normalize(i) for i in original_ids_raw}
    rewritten_ids = {normalize(i) for i in rewritten_ids_raw}
    union_ids = original_ids | rewritten_ids

    groups = classify(union_ids)

    total_distinct = sum(len(v) for v in groups.values())

    print("Verification Report")
    print("===================")
    print(f"Original IDs: {len(original_ids)} | Rewritten IDs: {len(rewritten_ids)}")
    print(f"Union (raw): {len(union_ids)}")
    print(f"Removed/Deprecated (excluded from total): {', '.join(sorted(REMOVED_CANONICAL))}")
    print()
    for k in ["U","O","A","N","F","MQ","TPCH_Q","ETPCH_Q","LITHE","Custom"]:
        print(f"{k} Series: {len(groups[k])} -> {', '.join(groups[k]) if groups[k] else '(none)'}")
    print()
    print(f"Total distinct (excluding removed): {total_distinct}")
    expected = 79
    if total_distinct == expected:
        print(f"Status: OK (matches expected {expected})")
    else:
        print(f"Status: MISMATCH (expected {expected}, found {total_distinct})")

    missing_in_rewritten = sorted(original_ids - rewritten_ids - REMOVED_CANONICAL)
    missing_in_original = sorted(rewritten_ids - original_ids - REMOVED_CANONICAL)
    if missing_in_rewritten:
        print("\nPresent only in original (no rewrite):", ", ".join(missing_in_rewritten))
    if missing_in_original:
        print("Present only in rewritten (new additions):", ", ".join(missing_in_original))

if __name__ == "__main__":
    main()
