E0261_P11 — Project Documentation (Updated)
==========================================

This document describes the Java classes in the `plan_equivalence` module and documents public and important internal methods including parameters, return values, thrown exceptions, and behavioral notes. It reflects recent updates to normalization (join/predicate/aggregate) and EXPLAIN retrieval error handling.

Table of contents
-----------------
- Calcite.java
- CalciteUtil.java
- FileIO.java
- GetQueryPlans.java
- LLM.java
- LLMResponse.java
- Main.java
- RelTreeNode.java
- Test.java


---------------------------------------------------------------------
Calcite.java
---------------------------------------------------------------------
Purpose
-------
Utility class that integrates with Apache Calcite. Responsibilities:
- Build a Calcite Framework (backed by a PostgreSQL JDBC schema).
- Parse/validate SQL strings into Calcite `RelNode` trees.
- Run a HepPlanner-based optimization pipeline (phased rules) for normalization.
- Provide layered plan comparison utilities and normalization/canonicalization helpers.

Glossary (Digests)
------------------
- Structural digest: order‑sensitive plan fingerprint (`RelOptUtil.toString(..., DIGEST_ATTRIBUTES)`).
- Normalized digest: input refs `$N`→`$x` and spacing collapsed (`normalizeDigest`).
- Canonical digest: inner‑join children flattened/sorted; predicates canonicalized; CASTs stripped; aggregates ordered; preserves projection and non‑INNER join order (`canonicalDigest`).
- Tree canonical digest: order‑insensitive tree digest (`RelTreeNode.canonicalDigest`).

Important public methods
------------------------
All methods are static on `Calcite`.

1) Framework and plan construction

- public static FrameworkConfig getFrameworkConfig()
  - Returns: `FrameworkConfig` bound to the configured PostgreSQL schema.
  - Behavior: Loads PostgreSQL driver, creates a Calcite connection to obtain the root schema, wraps a `PGSimpleDataSource` as a `JdbcSchema`, registers it under `PG_SCHEMA` and returns a config with a parser configured to `Lex.MYSQL` (folds unquoted identifiers to lower-case). Throws a RuntimeException if driver or connection setup fails.

- public static RelNode getOptimizedRelNode(Planner planner, String sql) throws Exception
  - Parameters:
    - `planner`: Calcite `Planner` created using `Frameworks.getPlanner(config)`
    - `sql`: SQL string to parse and optimize
  - Returns: Optimized `RelNode` after applying a three-phase `HepPlanner` program
    (phase 1: basic simplification; phase 2: join normalization and predicate push; phase 3: project collapse and final simplification).
  - Throws: `Exception` on parse/validate/optimization error.
  - Notes: Performs light SQL sanitization: strips trailing semicolons and converts `!=` to `<>` before parsing.

- public static RelNode getRelNode(Planner planner, String sql) throws Exception
  - Parameters: same as `getOptimizedRelNode`.
  - Returns: Logical (unoptimized) `RelNode` produced by parse/validate/rel conversion.
  - Throws: `Exception` on parse/validation error.
  - Notes: Also strips trailing semicolons and normalizes `!=`.

2) Comparison APIs

- public static boolean compareQueries(String sql1, String sql2, List<String> transformations)
  - Parameters:
    - `sql1`: First SQL string (left-hand side of comparison).
    - `sql2`: Second SQL string (right-hand side of comparison).
    - `transformations`: Optional list of transformation rule names to apply to the first plan before comparison (see `applyTransformations`).
  - Returns: `true` if queries are considered equivalent under a series of matching steps, `false` otherwise.
  - Matching strategy (tries in order):
    1. Structural digest equality (Calcite digest with attributes).
    2. Normalized digest equality (`normalizeDigest` neutralizes `$N` input refs).
    3. Canonical digest equality (`canonicalDigest`) — treats inner-join children as unordered, strips CASTs, normalizes commutative expressions.
    4. Tree equality ignoring child order (`RelTreeNode` canonical digest equality).
    5. Final fallback: `rel1.equals(rel2)` or `rel1.deepEquals(rel2)`.
  - Error handling: parsing/plan errors cause method to return `false` (conservative approach).

- public static boolean compareQueriesDebug(String sql1, String sql2, List<String> transformations, String tag)
  - Parameters: same as `compareQueries`, plus a `tag` (string) used to label debug prints.
  - Behavior: Runs the same matching sequence as `compareQueries`. If equivalence is not found it prints layered diagnostics:
    - Structural digests for both plans
    - Normalized digests
    - Canonical digests
    - Tree canonical digests
  - Returns: `true` if equivalent; otherwise prints diagnostics and returns `false`. Parser/planner exceptions are caught and printed with the `tag` included.

3) Digest & canonicalization helpers

- public static String normalizeDigest(String digest)
  - Parameters: `digest` — raw digest string produced by `RelOptUtil.toString(rel, SqlExplainLevel.DIGEST_ATTRIBUTES)`
  - Returns: A normalized digest where all input references like `$0`, `$12` are replaced with `$x`, and duplicate spaces collapsed. Returns `null` if input is `null`.
  - Purpose: Reduce sensitivity to field indices that shift when join inputs swap.

- public static String canonicalDigest(RelNode rel)
  - Returns: A canonical, stable string representing a `RelNode` where:
    - Inner joins are made child-order-insensitive by flattening and sorting child digests
    - CASTs are recursively stripped in expressions
    - Commutative/symmetric operators normalized
    - Predicates are decomposed into conjuncts, canonicalized, deduplicated, and sorted
    - Aggregates are normalized by sorting `groupSet` and aggregate calls deterministically (function name + input index, DISTINCT tagged)
    - UNION nodes flattened and child digests sorted
    - Sort keys are ignored (fetch/offset preserved)
  - Notes: Preserves projection output order; outer joins are NOT made order-insensitive.
  - Step‑by‑step:
    1) Traverse safely using a path‑set.
    2) Project: list output expressions in order; recurse.
    3) Filter: canonicalize predicate; recurse.
    4) INNER Join: flatten; sort child digests; split/normalize/deduplicate/sort conjuncts; build order‑insensitive join string.
    5) Non‑INNER Join: keep left/right order and normalized condition.
    6) Union: flatten matching ALL/DISTINCT; sort children; build stable string.
    7) Sort: ignore keys; include fetch/offset; recurse.
    8) Aggregate: sort group keys; format/sort calls deterministically; recurse.
    9) Other: emit normalized type plus canonicalized children.

  Recent canonicalization robustness improvements
  ---------------------------------------------
  - CHAR literal trimming: fixed-width CHAR padding is ignored when comparing literal values.
  - SEARCH/SARG normalization: SEARCH expressions are normalized and single-interval SARGs folded to `RANGE(...)`.
  - Conjunct decomposition and range folding: AND-chains are decomposed, inequalities folded into `RANGE(...)` expressions, then deduped and sorted.
  - Date + INTERVAL folding: `DATE + INTERVAL_YEAR_MONTH` with a numeric prefix is interpreted as months and folded to concrete DATE literals when safe.
  - Range grouping key stability: range grouping uses a raw RexNode key, avoiding conflation of distinct input refs normalized to `$x`.

- private static String canonicalizeRex(RexNode node)
  - Input: `RexNode` expression
  - Returns: Normalized string for expression; behavior includes:
    - Recursively strip CASTs via `stripAllCasts`
    - For AND/OR/PLUS/TIMES: sort operands
    - For EQUALS/NOT_EQUALS: treat as symmetric
    - For inequalities: flip `>`/`>=` to `<`/`<=` with deterministic operand ordering
    - Fallback: use `normalizeDigest(n.toString())`

- private static RexNode stripAllCasts(RexNode node)
  - Input: `RexNode`
  - Returns: Equivalent `RexNode` with all CAST operators removed recursively. Rebuilds calls where operands changed.

- private static void flattenUnionInputs(RelNode rel, boolean targetAll, List<RelNode> out)
  - Flattens nested `Union` inputs of the same ALL/DISTINCT property into `out`.

4) Tree utilities

- public static RelTreeNode buildRelTree(RelNode rel)
  - Builds a `RelTreeNode` representation from a Calcite `RelNode` using a `RelVisitor`. Useful for tree-based comparisons and debug printing. Tree equality can ignore child order via canonical digest.

- public static String relTreeCanonicalDigest(RelNode rel)
  - Returns the canonical digest of the `RelTreeNode` built from `rel`.

- public static boolean compareRelNodes(RelNode r1, RelNode r2, boolean ignoreChildOrder)
  - Compares `RelNode` trees by building `RelTreeNode` and checking equality with or without child order sensitivity.

5) Transformation application

- public static RelNode applyTransformations(RelNode rel, List<String> transformations)
  - Parameters: `rel` (input plan), `transformations` (list of rule names as strings).
  - Returns: New `RelNode` after applying each named rule via a one-off `HepPlanner` using the mapped `CoreRules`.
  - Behavior: Maps lower-cased names to a set of supported `CoreRules`. Unknown names are ignored. This is used to test LLM-suggested transformations or to experiment with specific rule effects.
  - Notes: Carefully choose transformations — they can change plan semantics if used incorrectly. Use only rules validated by the project.

Non-public helpers and printing utilities
---------------------------------------
- printRelTrees(String sql1, String sql2) — convenience debug function that prints the `RelTreeNode` for a single SQL string.
- summarizeNode(RelNode) — produce short node labels used by `RelTreeNode` builder (e.g., `Project[...]`, `Filter(...)`, `Join(...,condition)`).

Behavioral and design notes
---------------------------
- The canonicalization is intentionally conservative with respect to outer join semantics and does not collapse LEFT/RIGHT/FULL joins even if predicates appear symmetric. Doing so without schema constraints (FKs / NOT NULL) can produce false positives.
- Laddered matching strategy reduces false negatives due to syntax/planner noise while preserving a conservative check for true semantic differences.
 - EXPLAIN JSON retrieval is wrapped in try‑catch in `convertRelNodetoJSONQueryPlan` and returns `null` on failure.
 - Deprecation: `RelDecorrelator.decorrelateQuery(RelNode)` is used in a best‑effort manner; migration planned when available.


---------------------------------------------------------------------
CalciteUtil.java
---------------------------------------------------------------------
Purpose
-------
Utility companion to `Calcite` that hosts shared helpers not directly tied to comparison logic.
- Builds a Calcite `FrameworkConfig` backed by a PostgreSQL JDBC schema.
- Performs lightweight SQL text rewrites (e.g., LEAST/GREATEST → CASE) as a defensive preprocessing step before parsing.
- Maps cleaned PostgreSQL EXPLAIN JSON plans into coarse structural `RelNode` trees for fallback comparison.
- Provides small debug helpers such as `printRelTrees`.

Important public methods
------------------------
All methods are static on `CalciteUtil`.

- `public static FrameworkConfig getFrameworkConfig()`
  - Returns: `FrameworkConfig` bound to the configured PostgreSQL schema, using `PGSimpleDataSource` and `JdbcSchema`.
  - Notes: This is the implementation behind `Calcite.getFrameworkConfig()`; the latter is retained as a thin wrapper for compatibility.

- `public static String rewriteLeastGreatest(String sql)`
  - Rewrites two-argument `LEAST(a,b)` / `GREATEST(a,b)` calls into CASE expressions where safely detectable.
  - Used by `Calcite.getOptimizedRelNode` before parsing.

- `public static RelNode jsonPlanToRelNode(String jsonPlan)`
  - Builds a structural `RelNode` from a cleaned PostgreSQL EXPLAIN JSON plan (scan and join tree only, predicates ignored).

- `public static void printRelTrees(String sql1, String sql2)`
  - Convenience debug method that prints the `RelTreeNode` for a planned query using the shared framework configuration.


---------------------------------------------------------------------
FileIO.java
---------------------------------------------------------------------
Purpose
-------
Stateless helper for file reading/writing used by the test driver and plan-capture utilities. Centralizes path constants to the working workspace used by this project and provides convenience helpers to read SQL query blocks and plan JSONs.

Key classes and methods
-----------------------
- public enum SqlSource { ORIGINAL, REWRITTEN, MUTATED }
  - Enumerates where to load SQL queries from; used by `readSqlQuery`.

- public static String readSqlQuery(SqlSource source, String queryId) throws IOException
  - Parameters:
    - `source`: which consolidated SQL file to read from
    - `queryId`: the Query ID string to locate (e.g., "U1", "O2")
  - Returns: The SQL block text for the requested query, trimmed. Throws `IOException` if file cannot be read or queryId not found.
  - Behavior: Reads the consolidated SQL file. File paths and other runtime parameters are configured in `src/main/resources/config.properties` and accessed at runtime via `com.ac.iisc.FileIO`; you do not need to modify Java sources to change these paths.

- public static String readOriginalSqlQuery(String queryId) throws IOException
  - Convenience wrapper for `readSqlQuery(SqlSource.ORIGINAL, queryId)`.

- public static String readRewrittenSqlQuery(String queryId) throws IOException
  - Convenience wrapper for rewritten set.

- public static String readMutatedSqlQuery(String queryId) throws IOException
  - Convenience wrapper for mutated set.

- private static String extractSqlBlockById(String fileContent, String queryId)
  - Internal parser that extracts the SQL text block following the header for the matching `Query ID`.

- public static String readOriginalQueryPlan(String queryId) throws IOException
  - Loads a JSON plan file from `original_query_plans` directory. Supports `queryId` supplied either as bare ID or filename ending with `.json`.

- public static String readRewrittenQueryPlan(String queryId) throws IOException
  - Loads from `rewritten_query_plans` dir.

- public static String readMutatedQueryPlan(String queryId) throws IOException
  - Loads from `mutated_query_plans` dir.

- public static String readTextFile(String absolutePath) throws IOException
  - Generic file read helper that returns UTF-8 content and performs validation.

- public static void ensureDirectory(String absoluteDirPath) throws IOException
  - Ensure directory exists (creates parents as needed).

- public static void writeTextFile(String absolutePath, String content) throws IOException
  - Writes UTF-8 content and creates parent directories if needed.

Notes and caveats
-----------------
 - Runtime paths and DB connection settings are read from `src/main/resources/config.properties` via `com.ac.iisc.FileIO`. Edit the properties file to adjust paths or database settings instead of changing the Java source.
- The SQL extraction relies on consistent header separators and `-- Query ID:` lines in the consolidated SQL files.


---------------------------------------------------------------------
GetQueryPlans.java
---------------------------------------------------------------------
Purpose
-------
Utilities to run PostgreSQL EXPLAIN (FORMAT JSON, BUFFERS) for each SQL and produce a cleaned JSON plan suitable for stable comparison (execution-time fields removed). The EXPLAIN retrieval call is wrapped with defensive error handling (caller sees `null` on failure).

Important methods
-----------------
- private static JSONArray explainPlan(Connection conn, String sql) throws SQLException
  - Parameters:
    - `conn`: an open JDBC `Connection` to PostgreSQL
    - `sql`: SQL string to explain
  - Returns: `org.json.JSONArray` representing PostgreSQL EXPLAIN JSON output (the raw array returned by the DB), or `null` if no rows returned.
  - Throws: `SQLException` on execution errors.
  - Behavior: Executes `EXPLAIN (FORMAT JSON, BUFFERS) <sql>` and parses the resulting JSON string to an `org.json.JSONArray`.

- private static Object cleanPlanTree(Object node)
  - Recursively removes execution-specific keys from objects/arrays using the `KEYS_TO_REMOVE` set.
  - Input: JSON tree node (JSONObject, JSONArray, or primitive)
  - Returns: Cleaned JSON structure (new containers created; input not mutated).

- private static JSONObject extractAndClean(JSONArray raw)
  - Expects PostgreSQL EXPLAIN JSON shape of `[ { "Plan": { ... }, ... } ]`.
  - Lifts the nested `Plan` object to the top-level and returns a cleaned `JSONObject` or `null` for unexpected shapes.

- public static String getCleanedQueryPlanJSONasString(String sql) throws SQLException
  - Parameters: `sql`: SQL text to EXPLAIN
  - Returns: Pretty-printed JSON string of the cleaned plan, or `null` if not obtainable
  - Throws: `SQLException` for DB errors
  - Behavior: Opens a JDBC connection (using DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT constants), runs `explainPlan`, extracts and cleans the Plan, and returns an indented JSON string (4 spaces). Caller responsibility: handle SQLException. The Calcite wrapper catches `SQLException` and returns `null` for robustness.

- public static String getDatabaseSchema()
  - Returns: Multi-line string enumerating schemas, tables, and columns with data types by querying `information_schema.columns`.
  - Error handling: SQL exceptions are caught internally; on error or empty result, returns an empty string.
  - Notes: Uses the same DB constants; connection is short-lived. Output is grouped as:
    `Schema: <schema>` → `Table: <table>` → `column : data_type`.

Notes
-----
- This tool runs queries with ANALYZE and BUFFERS; ensure you run it against a safe, test instance.
- The cleaning step removes timing and buffer statistics to make a plan shape stable for comparison across runs.


---------------------------------------------------------------------
LLM.java
---------------------------------------------------------------------
Purpose
-------
Lightweight wrapper around an LLM client to ask whether two plans are equivalent and (if so) to provide a list of Apache Calcite transformation rule names that map the first plan to the second.

Important fields & methods
--------------------------
- private static final String PROMPT_1 / PROMPT_2
  - Text blocks that describe the strict JSON response contract (including
    `reasoning`, `equivalent`, `transformations`, and `preconditions`) and
    embed the schema summary, supported transformations, and sample plans.

- private static final List<String> SUPPORTED_TRANSFORMATIONS
  - Immutable allow-list of transformation names that the LLM may return. These correspond to rules validated by `Calcite.applyTransformations`.

- public static List<String> getSupportedTransformations()
  - Returns the allowed list.

- public static String contactLLM(String sqlAJSON, String sqlBJSON)
  - Parameters:
    - `sqlAJSON`: cleaned JSON plan for the first query
    - `sqlBJSON`: cleaned JSON plan for the second query
  - Returns: Assistant text extracted from the LLM response. The helper constructs a strict prompt that includes both cleaned plan JSON blobs and the allow-list of supported transformation names.
  - Notes: Behavior details:
    - The method checks `OPENAI_API_KEY` in the environment and returns a safe "not equivalent" contract string if missing (avoid interactive prompts).
    - It uses the Responses API and performs best-effort extraction of the assistant's output text from the SDK `Response` object (falls back to a conservative non-equivalent value if extraction fails).
    - The prompt requests a strict JSON contract: a single JSON object with fields
      `reasoning` (free-form analysis), `equivalent` (string; `"true"`, `"false"`, or
      `"dont_know"`), `transformations` (array of transformation names, may be
      empty), and `preconditions` (array of per-transformation precondition
      objects). The code continues to accept the legacy line-oriented format for
      backward compatibility and currently only consumes `equivalent` and
      `transformations`.

- public static LLMResponse getLLMResponse(String sqlA, String sqlB)
  - Parameters: `sqlA` and `sqlB` are SQL strings (not precomputed JSON). The method obtains cleaned JSON plans via `GetQueryPlans.getCleanedQueryPlanJSONasString` and then contacts the LLM via `contactLLM`.
  - Returns: `LLMResponse` parsed/validated from the raw LLM output. Returns `null` if obtaining plans failed (Calcite wrapper returns `null` on EXPLAIN errors).

Notes
-----
- LLM integration is optional. The rest of the codebase functions without it.
- Keep secrets and keys out of source control and environment-managed.


---------------------------------------------------------------------
LLMResponse.java
---------------------------------------------------------------------
Purpose
-------
Parses the LLM output contract. The preferred contract is a JSON object with the
following keys:

- `reasoning`: optional string — free-form explanation (ignored by this class).
- `equivalent`: string — one of `"true"`, `"false"`, or `"dont_know"`.
- `transformations`: array — ordered list of transformation names (may be empty).
- `preconditions`: optional array — objects describing preconditions for each
  transformation (ignored by this class).

For backward compatibility, the parser also accepts the legacy line-oriented format where the first line is `true`/`false` and subsequent lines list transformation names (or a single sentinel like `No transformations needed`).

Constructors & behavior
-----------------------
- public LLMResponse(boolean queriesAreEquivalent, List<String> transformationSteps)
  - Simple constructor taking explicit values.

- public LLMResponse(String responseText)
  - Parses raw `responseText` from the LLM, validates first line is `true` or `false`, trims and parses subsequent lines into transformation steps, and validates that each transformation is present in the allowed list.
  - Throws `IllegalArgumentException` if the response is malformed or if transformation names are not in the allowed list.

Getters/Setters
---------------
- public boolean areQueriesEquivalent()
- public List<String> getTransformationSteps()
- public void setQueriesAreEquivalent(boolean)
- public void setTransformationSteps(List<String>)

Notes
-----
- On parsing, commonly returned special tokens ("No transformations needed", "No transformations found") are treated as an empty list of transformations.


---------------------------------------------------------------------
Main.java
---------------------------------------------------------------------
Purpose
-------
Minimal command-line entrypoint demonstrating use of the other utilities. Behavior:
1. Read two SQL strings from stdin.
2. Perform a fast textual equality check.
3. Use `Calcite.compareQueries` to test equivalence without the LLM.
4. If not equal, call `LLM.getLLMResponse` for suggested transforms and validate them via `Calcite.compareQueries`.

Important private method
------------------------
- private static boolean compareQueries(String sqlA, String sqlB)
  - Uses a three-stage approach: textual equality, `Calcite.compareQueries`, then LLM-invoked transform validation.
  - Returns final boolean equivalence result.

- public static void main(String[] args)
  - Reads two lines from stdin and prints the boolean result of `compareQueries`.

Notes
-----
- `Main` demonstrates the end-to-end flow but is not intended as a production CLI. Consider adding argument parsing and error handling for a robust tool.


---------------------------------------------------------------------
RelTreeNode.java
---------------------------------------------------------------------
Purpose
-------
Small tree wrapper used to represent a `RelNode` plan in a language-agnostic way. Used by `Calcite.buildRelTree` and by tree-based comparisons.

Important methods
-----------------
- public RelTreeNode()
  - Empty constructor.

- public RelTreeNode(String label)
  - Construct with node label (e.g., `Project[...]`, `Join(...,cond)`).

- public void addChild(RelTreeNode child)
  - Appends `child` to the children list if non-null.

- public List<RelTreeNode> getChildren()
  - Returns mutable list of children (internal list is returned; caller should not mutate unexpectedly).

- public String canonicalDigest()
  - Builds an order-insensitive canonical digest of the subtree rooted at this node. Format: `label[sorted(childDigest|...)]`.

- public boolean equalsIgnoreChildOrder(RelTreeNode other)
  - True if trees are structurally equivalent up to permutation of children.

Design notes
------------
- `toString()` produces an indented, human-friendly display of the tree (useful in logs).


---------------------------------------------------------------------
Test.java
---------------------------------------------------------------------
Purpose
-------
A small developer-oriented batch driver to compare original and rewritten SQL queries by id (reads SQL via `FileIO` and compares via `Calcite.compareQueriesDebug`).

Behavior and configuration
--------------------------
- `queryIDList` (static) controls which query IDs the driver processes. Edit this list to select queries for batch runs.
- For each id:
  1. Load SQL A via `FileIO.readOriginalSqlQuery(id)`
  2. Load SQL B via `FileIO.readRewrittenSqlQuery(id)`
  3. Call `Calcite.compareQueriesDebug(sqlA, sqlB, null, id)` and print a result line (prints layered digests when not equivalent).
  4. If not equivalent, try again with transformations (example uses `List.of("UnionMergeRule")`).
- At the end prints summary counts.

Exceptions
----------
- `main` declares `throws Exception` — any IO or planning exceptions propagate and cause the process to fail. For CI/automation, wrap with handling and return non-zero exit codes on failures.


---------------------------------------------------------------------
Usage guidance and recommended extensions
---------------------------------------------------------------------
- If you need looser equivalence semantics that rely on schema facts (foreign keys, not-null, functional dependencies), add a metadata-aware pass and an optional `lenient` mode in `Calcite.compareQueries` to take advantage of that metadata.
- To add unit tests, create small synthetic queries where expected equivalences are known (inner-join commutativity, predicate commutativity, safe cast removal). Use a small test harness that compiles classes and invokes `Calcite.compareQueries`.
- If you want command-line automation, extend `Main` with argument parsing (e.g., `--id <ID>`, `--file <path>`, `--lenient`) and proper exit codes.


---------------------------------------------------------------------
Appendix: Editing notes and caveats
---------------------------------------------------------------------
- File paths in `FileIO` and DB connection details in `Calcite` and `GetQueryPlans` are hard-coded to the local workspace and simple test DB credentials; update before running on other systems.
- LLM integration uses a third-party client; if not configured, `LLM.getLLMResponse` will fail to contact the API. The core comparison logic in `Calcite` does not require LLM.
- Some SQL patterns may still produce planner-dependent differences. Use `Calcite.compareQueriesDebug` to inspect layered digests to decide whether to add additional safe normalizations.

Version notes
-------------
- Some Calcite APIs used for decorrelation may be deprecated depending on Calcite version. The code isolates decorrelation and treats failures as non-blocking (best-effort only).


If you want, I can:
- Generate Javadoc-style markdown for each class (split into separate files).
- Add small examples (inputs/outputs) per key method in `documentation.md`.
- Produce a short `BUILD.md` with `javac` or `mvn` commands tailored to your environment (PowerShell examples).