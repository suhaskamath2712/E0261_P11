#!/usr/bin/env python3
"""Format an arbitrary .sql file into the repo's `original_queries.sql`-style layout.

Goals:
- Prepend a comment containing a Query ID before each SQL statement.
- Keep the SQL text unchanged by default (byte-for-byte inside each statement, except
  for leading/trailing whitespace normalization around statement boundaries).
- Optionally pretty-print (whitespace-only) using `sqlparse` when explicitly requested.

Examples:
  python3 python_scripts/format_sql_queries.py \
    --input sql_queries/mutated_queries.sql \
    --output sql_queries/mutated_queries_formatted.sql

  # Reuse existing IDs when the input already contains comments like: "Query ID: X"
  python3 python_scripts/format_sql_queries.py -i in.sql -o out.sql --keep-existing-ids

  # Optional pretty printing (may change whitespace/keyword casing depending on sqlparse settings)
  python3 python_scripts/format_sql_queries.py -i in.sql -o out.sql --pretty
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple


_QUERY_ID_RE = re.compile(
    r"(?im)^[ \t]*(?:--|/\*)[ \t]*Query[ \t]*ID[ \t]*:[ \t]*([^\s*]+)"
)


@dataclass
class SplitChunk:
    text: str
    # 0-based byte offset of the chunk start in the original input
    start: int
    # 0-based byte offset of the chunk end in the original input (exclusive)
    end: int


def _strip_existing_original_queries_header(chunk: str) -> str:
    """Remove an existing `original_queries.sql`-style header if present.

    This only strips a very specific pattern so we don't accidentally delete
    meaningful comments.
    """

    # Normalize only for matching; we will remove the exact matched prefix from the
    # original chunk.
    header_re = re.compile(
        r"(?s)^(?P<prefix>[ \t\r\n]*"  # leading whitespace
        r"(?:--[ \t]*=+\r?\n)"  # first separator
        r"(?:--[ \t]*Query[ \t]*ID[ \t]*:.*\r?\n)"  # Query ID line
        r"(?:--[ \t]*Description[ \t]*:.*\r?\n)?"  # optional Description line
        r"(?:--[ \t]*=+\r?\n)"  # second separator
        r"[ \t]*\r?\n*)"  # optional blank line(s)
    )

    m = header_re.match(chunk)
    if not m:
        return chunk
    return chunk[len(m.group("prefix")) :]


def split_sql_statements(sql_text: str) -> List[SplitChunk]:
    """Split SQL text on semicolons that are not inside quotes/comments.

    This is intentionally conservative and aims to avoid splitting inside:
    - single quoted strings: '...'
    - double quoted identifiers: "..."
    - line comments: -- ...\n
    - block comments: /* ... */
    - PostgreSQL dollar-quoted strings: $$...$$ or $tag$...$tag$

    Returns chunks including the semicolon delimiter when present.
    """

    chunks: List[SplitChunk] = []
    n = len(sql_text)

    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False

    dollar_tag: Optional[str] = None  # e.g. "$tag$" or "$$"

    i = 0
    start = 0

    def startswith_at(s: str, idx: int) -> bool:
        return sql_text.startswith(s, idx)

    while i < n:
        ch = sql_text[i]

        # End of line comment
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # End of block comment
        if in_block_comment:
            if startswith_at("*/", i):
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        # End of dollar-quoted string
        if dollar_tag is not None:
            if sql_text.startswith(dollar_tag, i):
                i += len(dollar_tag)
                dollar_tag = None
            else:
                i += 1
            continue

        # Inside single quotes
        if in_single:
            if ch == "'":
                # SQL escaping: '' inside single-quoted strings
                if i + 1 < n and sql_text[i + 1] == "'":
                    i += 2
                else:
                    in_single = False
                    i += 1
            else:
                i += 1
            continue

        # Inside double quotes
        if in_double:
            if ch == '"':
                # Escaped double quote inside identifier: ""
                if i + 1 < n and sql_text[i + 1] == '"':
                    i += 2
                else:
                    in_double = False
                    i += 1
            else:
                i += 1
            continue

        # Not inside any string/comment: check for comment starts
        if startswith_at("--", i):
            in_line_comment = True
            i += 2
            continue
        if startswith_at("/*", i):
            in_block_comment = True
            i += 2
            continue

        # Not inside any string/comment: check for string starts
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == '"':
            in_double = True
            i += 1
            continue

        # Dollar-quoted strings (PostgreSQL): $tag$ ... $tag$
        if ch == "$":
            # Find next '$' in the tag opener.
            j = i + 1
            while j < n and sql_text[j] != "$" and (sql_text[j].isalnum() or sql_text[j] == "_"):
                j += 1
            if j < n and sql_text[j] == "$":
                dollar_tag = sql_text[i : j + 1]  # includes both '$'
                i = j + 1
                continue

        # Statement delimiter
        if ch == ";":
            end = i + 1
            chunk = sql_text[start:end]
            chunks.append(SplitChunk(text=chunk, start=start, end=end))
            start = end
            i += 1
            continue

        i += 1

    # Tail
    if start < n:
        tail = sql_text[start:n]
        # Keep tail only if it has something besides whitespace/comments.
        # Since reliably stripping comments requires a parser, we keep any non-whitespace.
        if tail.strip():
            chunks.append(SplitChunk(text=tail, start=start, end=n))

    # If no semicolons and content exists, return whole file as one chunk
    if not chunks and sql_text.strip():
        chunks = [SplitChunk(text=sql_text, start=0, end=len(sql_text))]

    return chunks


def extract_query_id(text: str) -> Optional[str]:
    m = _QUERY_ID_RE.search(text)
    if not m:
        return None
    return m.group(1).strip()


def build_header(query_id: str, description: Optional[str]) -> str:
    lines = [
        "-- =================================================================",\
        f"-- Query ID: {query_id}",
    ]
    if description:
        lines.append(f"-- Description: {description}")
    lines.append("-- =================================================================")
    return "\n".join(lines) + "\n"


def maybe_pretty_format(sql_text: str, enabled: bool) -> str:
    if not enabled:
        return sql_text

    try:
        import sqlparse  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Pretty formatting requires the optional dependency `sqlparse`. "
            "Install it (e.g., `pip install sqlparse`) or rerun without `--pretty`."
        ) from e

    # NOTE: This will modify whitespace and may change keyword casing depending on sqlparse.
    # We keep keyword_case=None to avoid changing the SQL text beyond whitespace.
    return sqlparse.format(
        sql_text,
        reindent=True,
        keyword_case=None,
        identifier_case=None,
        strip_comments=False,
        use_space_around_operators=True,
    )


def format_file(
    input_path: str,
    output_path: Optional[str],
    id_prefix: str,
    start_index: int,
    keep_existing_ids: bool,
    strip_existing_headers: bool,
    pretty: bool,
    description_template: Optional[str],
) -> str:
    with open(input_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    chunks = split_sql_statements(sql_text)

    out_parts: List[str] = []
    auto_idx = start_index

    base_name = os.path.basename(input_path)

    for chunk in chunks:
        raw = chunk.text

        # Keep SQL text unchanged as much as possible; normalize only the boundary whitespace.
        # This avoids accidental changes when the input has multiple statements with mixed spacing.
        body = raw.strip("\n")
        body = body.strip()

        if not body:
            continue

        if strip_existing_headers:
            body = _strip_existing_original_queries_header(body)

        qid = extract_query_id(body) if keep_existing_ids else None
        if not qid:
            qid = f"{id_prefix}{auto_idx}"
            auto_idx += 1

        description: Optional[str] = None
        if description_template:
            description = description_template.format(
                query_id=qid,
                input_file=base_name,
                index=auto_idx - 1,
            )

        body = maybe_pretty_format(body, enabled=pretty)

        out_parts.append(build_header(qid, description))
        out_parts.append(body)
        if not body.endswith(";"):
            # Do not invent semicolons; just keep separation readable.
            out_parts.append("\n")
        out_parts.append("\n\n")

    result = "".join(out_parts).rstrip() + "\n"

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)

    return result


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Format a .sql file into the repo's original_queries.sql-style layout, "
            "ensuring each query is preceded by a Query ID comment."
        )
    )
    p.add_argument("-i", "--input", required=True, help="Input .sql file")
    p.add_argument("-o", "--output", help="Output .sql file (defaults to stdout)")
    p.add_argument(
        "--id-prefix",
        default="Q",
        help="Prefix to use for auto-generated query IDs (default: Q)",
    )
    p.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="Starting index for auto-generated query IDs (default: 1)",
    )
    p.add_argument(
        "--keep-existing-ids",
        action="store_true",
        help="If input contains a comment like 'Query ID: X', reuse X for that query",
    )
    p.add_argument(
        "--strip-existing-headers",
        action="store_true",
        help="Strip an existing original_queries.sql-style header before adding a new one",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help=(
            "Pretty-print SQL (whitespace-only) using sqlparse. "
            "Off by default to avoid changing query text."
        ),
    )
    p.add_argument(
        "--description",
        default=None,
        help=(
            "Optional description line template. You can use {query_id}, {input_file}, {index}. "
            "Example: --description 'Auto-generated from {input_file}'"
        ),
    )

    args = p.parse_args(argv)

    try:
        result = format_file(
            input_path=args.input,
            output_path=args.output,
            id_prefix=args.id_prefix,
            start_index=args.start_index,
            keep_existing_ids=args.keep_existing_ids,
            strip_existing_headers=args.strip_existing_headers,
            pretty=args.pretty,
            description_template=args.description,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    if not args.output:
        sys.stdout.write(result)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
