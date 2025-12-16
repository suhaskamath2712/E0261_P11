# E0261_P11 — Code reference

This reference is intentionally **high signal**: it lists only classes and methods that exist in the current workspace and describes behavior as implemented today.

Java sources live in `plan_equivalence/src/main/java/com/ac/iisc/`.

## Contents

1. `Calcite`
2. `CalciteUtil`
3. `FileIO`
4. `GetQueryPlans`
5. `LLM`
6. `LLMResponse`
7. `RelTreeNode`
8. `Test`

## `Calcite`

Location: `plan_equivalence/src/main/java/com/ac/iisc/Calcite.java`

Role: the main equivalence engine.

Key public methods:

- `FrameworkConfig getFrameworkConfig()`
   - Delegates to `CalciteUtil.getFrameworkConfig()`.

- `RelNode getOptimizedRelNode(Planner planner, String sql)`
   - Parses/validates SQL, converts to `RelNode`, and runs a phased HepPlanner program.
   - Includes compatibility shims (e.g., `GROUP BY` alias rewrite).

- `boolean compareQueries(String sql1, String sql2, List<String> transformations)`
   - Entry point for comparing SQL strings.
   - Applies `transformations` (Calcite rules) only on the left plan (if provided).
   - Uses the layered digest approach described in the repo root `README.md`.

- `boolean compareQueries(RelNode rel1, RelNode rel2, List<String> transformations)`
   - Same idea as above when you already have `RelNode`s.

- `String normalizeDigest(String digest)`
   - Normalizes input references in digest text (`$0`, `$12` → `$x`).

- `String canonicalDigest(RelNode rel)`
   - Produces a stable digest tolerant to safe re-orderings and representational noise.
   - Highlights include inner-join commutativity handling, predicate decomposition/dedup/sort, SEARCH/SARG normalization, range folding, date folding, and Top‑N collation handling.

- Debug helpers: `printRelTrees`, `buildRelTree`, `compareRelTrees`, `compareRelNodes`, `relTreeCanonicalDigest`.
   - Useful for inspection; not part of the default equivalence ladder.

- `RelNode applyTransformations(RelNode rel, List<String> transformations)`
   - Applies an allow-listed set of Calcite planner rules (HepPlanner).

- `String convertRelNodetoJSONQueryPlan(RelNode rel)`
   - Best-effort `RelNode` → SQL → PostgreSQL `EXPLAIN (FORMAT JSON, BUFFERS)`.
   - Returns `null` if SQL rendering or EXPLAIN fails.

## `CalciteUtil`

Location: `plan_equivalence/src/main/java/com/ac/iisc/CalciteUtil.java`

Role: Calcite setup + compatibility shims + EXPLAIN JSON → coarse RelNode mapping.

Key public methods:

- `FrameworkConfig getFrameworkConfig()`
   - Builds a Calcite `FrameworkConfig` backed by a PostgreSQL `JdbcSchema`.

- `String rewriteGroupByAliases(String sql)`
   - Rewrites `GROUP BY <alias>` to `GROUP BY <expression>` (PostgreSQL compatibility).

- `String rewriteLeastGreatest(String sql)`
   - Rewrites simple two-argument `LEAST/GREATEST` patterns into CASE when safely recognized.

- `RelNode jsonPlanToRelNode(String jsonPlan)`
   - Converts cleaned PostgreSQL EXPLAIN JSON into a simplified logical tree that captures *scan/join shape*.
   - Predicates and physical details are intentionally ignored.

- `void printRelTrees(String sql1, String sql2)`
   - Convenience debug print.

## `FileIO`

Location: `plan_equivalence/src/main/java/com/ac/iisc/FileIO.java`

Role: configuration + reading query text blocks and auxiliary resources.

Key public methods:

- `String readSqlQuery(SqlSource source, String queryId)`
   - Reads a consolidated SQL file and extracts the block for `queryId`.
   - Duplicate Query IDs: uses the **last** occurrence and prints a warning.

- `readOriginalSqlQuery`, `readRewrittenSqlQuery`, `readMutatedSqlQuery`
   - Convenience wrappers.

- `List<String> listQueryIds(SqlSource source)`
   - Enumerates Query IDs present in the configured SQL file.

- `String readSchemaSummary()`
   - Loads `tpch_schema_summary.json` (or another configured summary resource).

- Config getters: `getProperty`, `getPgUrl`, `getPgUser`, `getPgPassword`, `getPgSchema`, plus file-path accessors.

## `GetQueryPlans`

Location: `plan_equivalence/src/main/java/com/ac/iisc/GetQueryPlans.java`

Role: PostgreSQL plan retrieval and cleaning.

Public API:

- `String getCleanedQueryPlanJSONasString(String sql)`
   - Runs `EXPLAIN (FORMAT JSON, BUFFERS) <sql>` and returns a cleaned JSON string.
   - Cleans away non-semantic execution keys while preserving semantic plan fields.

- `String getDatabaseSchema()`
   - Returns a simple schema description (via `information_schema.columns`) for prompt/context/debugging.

## `LLM`

Location: `plan_equivalence/src/main/java/com/ac/iisc/LLM.java`

Role: optional OpenAI integration.

Public API:

- `List<String> getSupportedTransformations()`
   - Reads the allow-list from `transformation_list.txt` (resource).

- `String contactLLM(String sqlAJSON, String sqlBJSON)` (+ overload with `previousResponse`)
   - Calls the OpenAI Responses API.
   - If `OPENAI_API_KEY` is missing, returns a safe “not equivalent” response.
   - Model is selected from `llm_model` in `config.properties` (default: `gpt-5`).

- `LLMResponse getLLMResponse(String sqlA, String sqlB)` (+ overload with `previousResponse`)
   - Obtains cleaned EXPLAIN JSON for both SQLs and then calls `contactLLM`.

## `LLMResponse`

Location: `plan_equivalence/src/main/java/com/ac/iisc/LLMResponse.java`

Role: parse/validate the LLM response into:

- `boolean areQueriesEquivalent()`
- `List<String> getTransformationSteps()`

The parser is intentionally strict about allowed transformation names.

## `RelTreeNode`

Location: `plan_equivalence/src/main/java/com/ac/iisc/RelTreeNode.java`

Role: debug tree representation.

Key methods:

- `String canonicalDigest()` (order-insensitive child digest)
- `boolean equalsIgnoreChildOrder(RelTreeNode other)`
- `String toString()` (pretty tree)

## `Test`

Location: `plan_equivalence/src/main/java/com/ac/iisc/Test.java`

Role: developer runner / harness. It loads SQL pairs (typically original vs rewritten) and invokes `Calcite.compareQueries(...)`.
