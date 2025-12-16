# E0261_P11 — Documentation

This folder contains human-readable documentation for the `plan_equivalence` Java module.

If you only want the essentials:

- `README.md` (repo root) explains *what* the tool does and the equivalence ladder.
- `documentation/Calcite_Doc.md` explains *how* Calcite is used and what canonicalization does.
- `documentation/code_reference.md` is a class-by-class reference.

## Module overview

The `plan_equivalence` module compares two SQL queries by converting them to Calcite `RelNode` logical plans and comparing multiple increasingly-tolerant digests.

At a glance:

1. **Parse + validate** SQL using Calcite with a PostgreSQL-backed schema.
2. **Normalize** each plan using a small phased HepPlanner program.
3. **(Optional)** apply a list of Calcite rules to the *left* plan.
4. **Normalize subqueries** (convert subqueries → correlates; attempt decorrelation) on both sides.
5. **Compare** structural → normalized → canonical digests.
6. **Optional** last-resort fallback: compare cleaned PostgreSQL EXPLAIN JSON.

## Key classes

All Java sources are under `plan_equivalence/src/main/java/com/ac/iisc/`.

- `Calcite.java`
  - Entry points: `compareQueries(String, String, List<String>)`, `canonicalDigest(RelNode)`.
  - Owns most canonicalization logic (join/predicate/date/search/range handling).
  - Performs best-effort subquery decorrelation.
  - Provides an optional Postgres EXPLAIN fallback.

- `CalciteUtil.java`
  - Builds the Calcite `FrameworkConfig` against a PostgreSQL schema.
  - Text rewrites used as compatibility shims before parsing:
    - `rewriteLeastGreatest` (LEAST/GREATEST → CASE)
    - `rewriteGroupByAliases` (GROUP BY alias → GROUP BY expression)
  - `jsonPlanToRelNode` maps cleaned EXPLAIN JSON into a coarse structural `RelNode` (scan/join shape).

- `GetQueryPlans.java`
  - Runs `EXPLAIN (FORMAT JSON, BUFFERS)` via JDBC.
  - Cleans plans by removing execution-only keys while **preserving semantic keys**.

- `FileIO.java`
  - Reads `config.properties` and exposes typed accessors (pg_url, file paths, etc.).
  - Extracts SQL blocks from consolidated `.sql` files by Query ID.
  - Duplicate Query IDs: prefers the **last** occurrence and prints a warning.

- `LLM.java` / `LLMResponse.java`
  - Optional integration with OpenAI’s Responses API.
  - Requires `OPENAI_API_KEY` in the environment (otherwise returns a safe “not equivalent” contract).
  - Model name is configured via `llm_model` in `config.properties`.

- `RelTreeNode.java`
  - Debugging representation of a plan as a tree with an order-insensitive canonical digest.
  - Not used as part of the default equivalence ladder.

- `Test.java`
  - Ad-hoc runner: loads SQL pairs by Query ID and runs equivalence.

## Configuration

Runtime config is read from `plan_equivalence/src/main/resources/config.properties`.

Common keys:

- PostgreSQL: `pg_url`, `pg_user`, `pg_password`, `pg_schema`
- SQL files: `original_sql_path`, `rewritten_sql_path`, `mutated_sql_path`
- Schema summary: `schema_summary_resource`
- LLM model: `llm_model`

## Notes on EXPLAIN fallback

The EXPLAIN-based fallback is **best-effort**:

- Conversion from `RelNode` → SQL can fail for some plans (notably correlated plans).
- EXPLAIN equality is not a formal proof of semantic equivalence; it’s used only as a last resort.
- Plan cleaning removes non-semantic, run-specific keys (timings, buffers, costs) but keeps semantic fields.
