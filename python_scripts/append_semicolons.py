#!/usr/bin/env python3
"""Append a semicolon to the end of each non-empty line in a file.

This is useful for files containing one SQL statement per line where some
lines may be missing a trailing ';'. The script preserves empty lines and
lines that already end with a semicolon. By default it writes to stdout; use
`-o` to write to a file or `--inplace` to modify the input file.

Examples:
  python3 python_scripts/append_semicolons.py -i sql_queries/temp.sql -o sql_queries/temp_with_semicolons.sql
  python3 python_scripts/append_semicolons.py -i sql_queries/temp.sql --inplace
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable


def process_lines(lines: Iterable[str]) -> Iterable[str]:
    for line in lines:
        # Preserve blank lines exactly
        if line.strip() == "":
            yield line
            continue

        # If line already ends with semicolon (ignoring trailing whitespace), keep original
        if line.rstrip().endswith(";"):
            yield line
            continue

        # Otherwise append a semicolon before the newline
        if line.endswith("\n"):
            yield line.rstrip("\n") + ";\n"
        else:
            yield line + ";"


def main() -> int:
    p = argparse.ArgumentParser(description="Append semicolons to each non-empty line of a file.")
    p.add_argument("-i", "--input", required=True, help="Input file")
    p.add_argument("-o", "--output", help="Output file (defaults to stdout)")
    p.add_argument("--inplace", action="store_true", help="Modify the input file in-place")

    args = p.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input file does not exist: {input_path}")
        return 2

    with input_path.open("r", encoding="utf-8") as f:
        lines = list(f.readlines())

    out_lines = list(process_lines(lines))

    if args.inplace:
        with input_path.open("w", encoding="utf-8") as f:
            f.writelines(out_lines)
        return 0

    if args.output:
        out_path = Path(args.output)
        with out_path.open("w", encoding="utf-8") as f:
            f.writelines(out_lines)
        return 0

    # Print to stdout
    for l in out_lines:
        print(l, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
