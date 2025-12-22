<div align="center">

# E0261_P11 — Plan Equivalence Helper

Parse, normalize, and compare SQL query plans using Apache Calcite, with an optional PostgreSQL EXPLAIN JSON fallback.

</div>

## What this repo does

Given two SQL queries (typically **original** vs **rewritten**/**mutated**), this project tries to decide whether they are equivalent under a set of conservative normalizations.

It aims to reduce **false negatives** from harmless planner/representation differences (join commutativity, CAST noise, predicate order, etc.), while being careful about **false positives**.

## How equivalence is decided

The core logic lives in `plan_equivalence/src/main/java/com/ac/iisc/Calcite.java`.

High-level flow:

1. Parse + validate SQL with Calcite against a PostgreSQL-backed schema (`CalciteUtil.getFrameworkConfig()`).
2. Normalize each query’s logical plan using a small phased HepPlanner program (`getOptimizedRelNode`).
3. (Optional) Apply a caller-supplied list of transformation rules to the *left* plan (`applyTransformations`).
4. Normalize scalar subqueries by converting them to correlates and attempting decorrelation (`normalizeSubqueriesAndDecorrelate`) on **both** sides.
5. Compare in layers (stop at the first match):
   - **Structural digest**: `RelOptUtil.toString(rel, DIGEST_ATTRIBUTES)`
   - **Normalized digest**: input refs like `$0`, `$12` are rewritten to `$x` (`normalizeDigest`)
   - **Canonical digest**: inner joins flattened/sorted, predicates canonicalized, CASTs stripped, aggregates normalized, etc. (`canonicalDigest`)
   - **Canonical digest + AND safety-net**: lexicographically sorts textual `AND(...)` term lists (`normalizeAndOrderingInDigest`)
   - **Optional EXPLAIN fallback**: generate SQL via `RelToSqlConverter`, run `EXPLAIN (FORMAT JSON, BUFFERS)`, clean non-semantic keys, and compare the cleaned JSON.

  - **Rel→SQL helper and equality check**: the engine includes `Calcite.relNodeToSql(RelNode)` — a best-effort Rel→SQL renderer using `RelToSqlConverter` with `PostgresqlSqlDialect`. As an additional positive-only signal, the engine may render both RelNodes to SQL, normalize whitespace/semicolons, and treat the queries as equivalent if the rendered SQLs are identical. This is conservative and never used to prove non-equivalence.

### Canonicalization highlights

`canonicalDigest(RelNode)` is where most robustness lives:

- **INNER join commutativity**: nested INNER joins are flattened; child digests are sorted.
- **Predicate normalization**:
  - AND chains are decomposed, canonicalized, deduped, sorted.
  - SEARCH/SARG expressions are normalized; some single-interval cases fold into a `RANGE(...)` form.
  - Inequality pairs can fold into `RANGE(expr, lower, upper)`.
- **Literal normalization**: trims trailing padding in CHAR literals.
- **Date arithmetic folding**:
  - DATE + INTERVAL_YEAR_MONTH (numeric prefix treated as months) folded when safe.
  - DATE ± INTERVAL_DAY_TIME folded when the interval is a whole number of days.
- **Top-N sort semantics**:
  - `ORDER BY` is ignored *unless* there is `FETCH`/`OFFSET` (Top‑N). For Top‑N, sort collation (field index + direction/null direction) is included.

## Project layout

`plan_equivalence/src/main/java/com/ac/iisc/`

- `Calcite.java` — equivalence engine, canonicalization, EXPLAIN fallback.
- `CalciteUtil.java` — framework configuration and SQL pre-rewrites (LEAST/GREATEST, GROUP BY alias expansion), plus JSON-plan→RelNode structural mapping.
- `GetQueryPlans.java` — runs `EXPLAIN (FORMAT JSON, BUFFERS)` and removes execution-only keys while preserving semantic fields.
- `FileIO.java` — reads SQL blocks by Query ID from consolidated `.sql` files; reads config and schema summary.
- `LLM.java` / `LLMResponse.java` — optional LLM integration.
- `RelTreeNode.java` — tree representation used for debugging (not part of the equivalence ladder by default).
- `Test.java` — ad-hoc runner.

## Configuration

Runtime configuration is read via `FileIO` from `plan_equivalence/src/main/resources/config.properties`.

Common keys:

- `pg_url`, `pg_user`, `pg_password`, `pg_schema`
- `original_sql_path`, `rewritten_sql_path`, `mutated_sql_path`
- `schema_summary_resource` (path to `tpch_schema_summary.json`)
- `llm_model` (model name used by `LLM`; default is `gpt-5`)
- `transformation_list_path` (optional override path for `transformation_list.txt`)

## Optional: LLM integration

`com.ac.iisc.LLM` is used only when you explicitly call it from the harness.

- Requires `OPENAI_API_KEY` to be set in the environment.
- Model is configured via `llm_model` in `config.properties`.

## Docs

- `documentation/Calcite_Doc.md` — design notes and normalization details.
- `documentation/code_reference.md` — class-by-class reference.
- `documentation/documentation.md` — project-level notes and configuration.

## Utilities

This repository includes a couple of convenience Python utilities under `python_scripts/`.

- `python_scripts/format_sql_queries.py` — split any SQL file into discrete statements and emit
  an `original_queries.sql`-style file where each statement is preceded by a standardized
  `-- Query ID: <id>` comment block. The script tries not to change SQL text; use `--pretty`
  to optionally reindent/format using `sqlparse` (optional dependency).

  Example:

  ```bash
  python3 python_scripts/format_sql_queries.py \
    --input sql_queries/mutated_queries.sql \
    --output sql_queries/mutated_queries_formatted.sql \
    --id-prefix M --start-index 1
  ```

  Run `--help` for more options (preserve existing IDs, strip existing headers, description template).


