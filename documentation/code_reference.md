# E0261_P11 Code Reference (Markdown Edition)

This document replaces the earlier LaTeX attempt. It provides a detailed, prose-oriented reference for each Java class in `plan_equivalence/src/main/java/com/ac/iisc/`, including every public (and notable private) method with behavior, parameters, return values, side‑effects, failure modes, performance notes, and subtle normalization rules.

---
## Table of Contents
1. [Calcite](#calcite)
2. [FileIO](#fileio)
3. [GetQueryPlans](#getqueryplans)
4. [LLM](#llm)
5. [LLMResponse](#llmresponse)
6. [RelTreeNode](#reltreenode)
7. [TransformationProbe](#transformationprobe)
8. [Test](#test)

---
## Calcite
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/Calcite.java`

### Purpose
### Purpose (Updated)
Central integration point with Apache Calcite. It:
- Builds a Calcite `FrameworkConfig` backed by a PostgreSQL schema via `PGSimpleDataSource`; parser configured with `Lex.MYSQL`.
- Parses + validates SQL into `RelNode` logical plans.
- Applies a phased HepPlanner optimization pipeline for plan normalization (projection collapsing, join reassociation/commutation, predicate simplification, etc.).
- Implements multi-stage equivalence comparison (structural digest → normalized digest → canonical digest → order-insensitive tree digest).
- Canonicalizes plans by neutralizing input refs, removing all CASTs, flattening inner join factors, sorting commutative/symmetric expressions, and ignoring ordering where semantics allow.
- Applies user-specified Calcite `CoreRules` by name through a registry (`RULE_MAP`) with limited fixpoint passes.
- Performs subquery removal and decorrelation to align scalar-subquery forms with equivalent join+aggregate forms.
- Planner reuse: Calcite planners are not reliably reusable across multiple parse/validate cycles; a new planner is created for each query.
- Canonicalization: Uses a DAG-safe traversal (IdentityHashMap path set) to avoid infinite recursion when a plan has shared subgraphs.
### Purpose (Updated)
Obtains PostgreSQL execution plans (`EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS)`) for supplied SQL and returns a cleaned JSON representation with execution-specific metrics removed, leaving only logical plan structure.
### Error Handling
- External call wrapped in try-catch returning `null` on failure.

### Digest Types (Glossary)
- Structural digest: `RelOptUtil.toString(rel, DIGEST_ATTRIBUTES)` — order‑sensitive textual fingerprint of the operator tree and attributes.
- Normalized digest: `normalizeDigest(String)` — replaces input refs like `$0`→`$x`, collapses spaces; reduces sensitivity to child swaps and minor formatting.
- Canonical digest: `canonicalDigest(RelNode)` — inner joins flattened/sorted; predicates canonicalized; CASTs stripped; aggregates ordered; preserves projection and non‑INNER join order.
- Tree canonical digest: `RelTreeNode.canonicalDigest()` — order‑insensitive digest of the tree structure used for last‑resort comparisons.

### Public API Methods (Detailed)
### Purpose (Updated)
Ad-hoc runner for evaluating equivalence across selected queries and testing transformation + LLM-assisted alignment.
### Error Handling
- Batch demo: iterate IDs, compare original vs rewritten, print layered diagnostics when not equivalent.
2. Open a Calcite connection to obtain root schema.
3. Wrap `PGSimpleDataSource` in a `JdbcSchema` added under the configured schema name.
4. Configure parser with `Lex.MYSQL` (folds unquoted identifiers to lower-case).

Errors: Throws `RuntimeException` wrapping `SQLException` or `ClassNotFoundException` if driver or JDBC connection fails.

#### `public static RelNode getOptimizedRelNode(Planner planner, String sql) throws Exception`
Parses, validates, converts, and optimizes a SQL string.
- Sanitizes trailing semicolons; normalizes `!=` → `<>`.
- Converts validated `SqlNode` to a logical `RelNode`.
- Applies three HepPlanner phases:
  - Phase 1: predicate simplification + project collapse/remove.
  - Phase 2: join normalization (associate/commute) with filter pushdown.
  - Phase 3: project transpose/merge/remove + final predicate simplification.
Returns the best expression from HepPlanner.

Failure: Any parse/validation/optimization error surfaces as an `Exception`.

#### `public static boolean compareQueries(String sql1, String sql2, List<String> transformations)`
Determines equivalence of two SQL queries through staged digests. Workflow:
1. Direct string equality fast-path.
2. Build planner and optimize query A; apply requested transformations (if provided).
3. Normalize subqueries + decorrelate (best-effort).
4. Optimize query B with symmetric transformation and normalization.
5. Compare structural digests (`RelOptUtil.toString` with `DIGEST_ATTRIBUTES`).
6. If mismatch: neutralize input references in both digests (`$\d+` → `$x`).
7. If mismatch: compute canonical digest (see below).
8. If mismatch: compare tree canonical digests (order-insensitive).
9. Returns `true` if any tier matches, else `false`.

Resilience: Catches all planning exceptions and treats them as non-equivalent.

#### `public static boolean compareQueriesDebug(String sql1, String sql2, List<String> transformations, String tag)`
Same strategy as `compareQueries` but prints intermediate digests when not equivalent. Helpful for diagnostics.

#### `public static String normalizeDigest(String digest)`
Utility replacing all input refs (`$0`, `$12`) with `$x` and collapsing spaces. Stabilizes digests against index shifts from join reorderings.

#### `public static String canonicalDigest(RelNode rel)`
Produces a canonical string representation tolerant of:
- Inner join child order: flatten nested inner joins, canonicalize factors, sort digests.
- Commutative and symmetric expressions: AND/OR/PLUS/TIMES sorted; EQUALS/NOT_EQUALS sorted and normalized.
- CAST noise: recursively stripped from every Rex expression.
- Join conditions: decomposed into conjuncts, canonicalized individually, deduplicated, sorted.
- Set operations: flattened unions (same ALL/DISTINCT) and sorted children.

Projection order and non-inner join child order are preserved (semantic significance).
Step‑by‑step summary:
1) Traverse safely using a path‑set.
2) Project: list output expressions in order; recurse.
3) Filter: canonicalize predicate; recurse.
4) INNER Join: flatten; sort child digests; split/normalize/deduplicate/sort conjuncts; build order‑insensitive join string.
5) Non‑INNER Join: keep left/right order and normalized condition.
6) Union: flatten matching ALL/DISTINCT; sort children; build stable string.
7) Sort: ignore keys; include fetch/offset; recurse.
8) Aggregate: sort group keys; format/sort calls deterministically; recurse.
9) Other: emit normalized type plus canonicalized children.

#### `public static RelTreeNode buildRelTree(RelNode rel)` / `public static String relTreeCanonicalDigest(RelNode rel)`
Converts a `RelNode` DAG to a simple ordered tree (`RelTreeNode`) via `RelVisitor`. The canonical digest of that tree ignores per-node child order. Used as the final comparison fallback.

#### `public static RelNode applyTransformations(RelNode rel, List<String> transformations)`
Applies a composite Hep program of user-requested `CoreRules` (looked up case-insensitively in `RULE_MAP`). Up to three passes run; early exit when structural digest stabilizes.

Failure Handling: Unknown rule names ignored. Exceptions during planning bubble up; caller usually catches them.

#### `public static boolean compareRelNodes(RelNode r1, RelNode r2, boolean ignoreChildOrder)` / `public static boolean compareRelTrees(RelTreeNode a, RelTreeNode b, boolean ignoreChildOrder)`
Convenience structural comparisons, optionally treating children as unordered sets.

### Private / Internal Helpers (Selected)
- `canonicalizeRex(RexNode)`: strips CASTs, sorts operands for commutative operators, normalizes ordering comparisons into `<`/`<=` forms, treats equals/unequals symmetrically.
- `stripAllCasts(RexNode)`: recursively remove CAST nodes, cloning calls where operand lists change.
- `collectInnerJoinFactors(...)`: flattens nested inner joins while gathering conditions.
- `decomposeConj(RexNode, List<String>)`: recursively splits AND-chains into individual conjunct digests.
- `flattenUnionInputs(RelNode, boolean, List<RelNode>)`: collects all union inputs sharing the same ALL/DISTINCT property.
- `normalizeSubqueriesAndDecorrelate(RelNode)`: applies subquery conversion rules then attempts `RelDecorrelator.decorrelateQuery`; performs cleanup passes.

### Error & Edge Case Handling
- Null inputs return explicit markers (`"null"` in digests) instead of throwing.
- Cycle detection: `canonicalDigestInternal` returns `TypeName[...cycle...]` when revisiting a node.
- Decorrelator failures are swallowed to avoid aborting comparison.
 - `RelDecorrelator.decorrelateQuery(RelNode)` is deprecated in some versions; used best‑effort and isolated.

### Performance Considerations
- Multiple planner instantiations per comparison; caching could reduce overhead but risks stale metadata.
- Canonicalization traverses entire graph; complex joins may incur O(n log n) due to sorting digests.
- Rule application limited by match limit (1000) and pass count (3) to prevent runaway rewrites.

---
## FileIO
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/FileIO.java`

### Purpose
Provides centralized, stateless utilities for reading query SQL blocks and performing basic file operations (read/write/ensure directory). Encapsulates regex parsing of consolidated SQL files to extract queries by `Query ID`.

### Key Concepts
- Consolidated SQL files contain structured headers marking query blocks:
  ```
  -- ================================================
  -- Query ID: U1 (OptionalSuffix)
  -- Description: ...
  -- ================================================
  <SQL until next separator>
  ```
- Query ID matching ignores parenthetical suffixes (e.g., `(Rewritten)`).

### Public API Methods
#### `public static String readSqlQuery(SqlSource source, String queryId)`
Reads the consolidated file for the given source (ORIGINAL/REWRITTEN/MUTATED), extracts the block for `queryId`. Throws:
- `IllegalArgumentException` for blank ID
- `IOException` if ID not found or file missing.

#### Convenience wrappers
- `readOriginalSqlQuery(String)`
- `readRewrittenSqlQuery(String)`
- `readMutatedSqlQuery(String)`

All delegate to `readSqlQuery` with corresponding enum.

#### `public static List<String> listQueryIds(SqlSource source)`
Scans file for `-- Query ID:` lines; returns de‑duplicated IDs in discovery order (LinkedHashSet).

#### `public static String readTextFile(String absolutePath)`
Strict UTF‑8 file read; validates path non-null, existence; throws `IOException` on missing file.

#### `public static void ensureDirectory(String absoluteDirPath)`
Creates directory (and parents) if absent.

#### `public static void writeTextFile(String absolutePath, String content)`
Writes UTF‑8 text, creating parents when necessary; converts null content to empty string.

### Private Helper
`extractSqlBlockById(String fileContent, String queryId)` performs a two-step regex-based header find then locates next separator to isolate SQL payload.

### Failure Modes & Edge Cases
- Malformed header (e.g., missing closing separator) returns `null` causing upstream `IOException`.
- Suffixes in header `(Rewritten)` are ignored during matching.

### Performance
- Entire file loaded into memory for regex scanning; acceptable for moderate-sized SQL corpora. For large datasets, a streaming parser may be preferred.

---
## GetQueryPlans
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/GetQueryPlans.java`

### Purpose
Obtains PostgreSQL execution plans (`EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS)`) for supplied SQL and returns a cleaned JSON representation with execution-specific metrics removed, leaving only logical plan structure.

### Core Responsibilities
1. Connect to PostgreSQL using configured host/db/user/pass.
2. Run EXPLAIN with ANALYZE (executes the query). WARNING: Resource-intensive for large workloads.
3. Parse returned JSON array; lift nested `{"Plan": {...}}` to root.
4. Remove metric keys (timings, I/O block counts, loops, etc.) defined in `KEYS_TO_REMOVE`.

### Public API
#### `public static String getCleanedQueryPlanJSONasString(String sql) throws SQLException`
- Creates connection (try-with-resources).
- Calls private `explainPlan(conn, sql)` → raw `JSONArray`.
- Cleans & pretty-prints result (indent=4) or returns `null` if shape unexpected.

#### `public static String getDatabaseSchema()`
- Opens a short-lived JDBC connection using the class DB constants.
- Queries `information_schema.columns` and formats a human-readable schema:
   `Schema: <schema>`, `Table: <table>`, then `column : data_type` lines.
- Returns an empty string if schema is unavailable or an error occurs (exceptions handled internally).

### Private Methods
#### `private static JSONArray explainPlan(Connection conn, String sql)`
Constructs `EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) <sql>`; executes prepared statement; returns first column as parsed `JSONArray` or `null`.

#### `private static Object cleanPlanTree(Object node)`
Recursive deep copy stripping keys from JSON object/array elements.

#### `private static JSONObject extractAndClean(JSONArray raw)`
Validates raw shape, lifts `Plan`, invokes `cleanPlanTree`, returns cleaned root object.

#### `private static Object removeImplementationDetails(JSONObject plan)`
Optional normalizer that maps executor-specific node types to generic names (e.g., "Hash Join"→"Join", "Seq Scan"→"Scan"), and renames certain fields (e.g., "Hash Cond"→"Join Condition", "Relation Name"→"Relation", "Index Name"→"Index"). Not invoked by default; available for consumers that want additional genericization beyond key removal.

### Failure Modes
- Connection failure → `SQLException` thrown to caller.
- Unexpected JSON shape → returns `null` (caller must handle).
- Key removal: If `Plan` missing, `null` returned.

### Performance & Cautions
- `ANALYZE` causes actual execution: use sparingly for expensive queries.
 JSON cleaning allocates new objects; negligible unless plan size is extremely large. Genericization, if enabled, adds minor additional traversal cost.

---
## LLM
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/LLM.java`

### Purpose
Optional integration point for a language model to assist in determining equivalence and proposing potential transformation sequences to map one plan to another.

### Behavior Summary
- Builds a strict prompt that enumerates supported transformations and embeds both cleaned plan JSON payloads.
- Requires `OPENAI_API_KEY` in environment; absence yields fallback contract `false\nNo transformations found` (no network call).
- Uses OpenAI Responses API (`model="gpt-5"`).
- Extracts assistant text via defensive string parsing (`extractAssistantText`).

### Public API
#### `public static List<String> getSupportedTransformations()`
Returns immutable list of transformation rule names allowed in LLM output. Used for validation in `LLMResponse`.

#### `public static String contactLLM(String sqlAJSON, String sqlBJSON)`
Workflow:
1. Check API key; fallback if missing.
2. Compose prompt with transformation allow-list + both JSON plans.
3. Invoke API client; retrieve response.
4. Extract assistant text; fallback contract if extraction fails.
5. Trim and return content.

Return Contract (JSON object):
- The LLM is instructed to return a single JSON object with the following fields:
    - `reasoning`: string — free-form, step-by-step analysis of how the plans compare.
    - `equivalent`: string — one of `"true"`, `"false"`, or `"dont_know"`.
    - `transformations`: array — an ordered list of transformation names (may be empty).
    - `preconditions`: array — objects describing minimal preconditions for each
       transformation (same order as `transformations`).

Only `equivalent` and `transformations` are consumed by `LLMResponse`; the
other fields are included for interpretability and retry guidance.

Example:
```json
{
   "reasoning": "The only difference is a redundant Project node above an Aggregate on the left input, which can be merged without changing semantics.",
   "equivalent": "true",
   "transformations": ["AggregateProjectMergeRule"],
   "preconditions": [
      { "requires": "Project directly on top of Aggregate with no column reordering or expression changes" }
   ]
}
```

#### `public static LLMResponse getLLMResponse(String sqlA, String sqlB)`
Obtains cleaned plans via `GetQueryPlans`, calls `contactLLM`, parses output into `LLMResponse`. On parsing failure returns a constructed `LLMResponse(false, List.of())`.

### Private Helper
`extractAssistantText(String)` attempts to locate patterns in `Response.toString()` output. Fragile by nature; upgrading SDK to use typed getters would be safer.

### Failure Modes
- Missing key → fallback non-equivalent contract (avoids exceptions).
- API/parse errors → fallback non-equivalent contract logged to stderr.

### Security & Operational Notes
- API key must not be committed; rely on environment variable.
- Prompt strictly instructs model not to output extraneous explanation to ease parsing.

---
## LLMResponse
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/LLMResponse.java`

### Purpose
Represents and validates the structured contract returned by the LLM.

### Contract
The preferred contract is a JSON object with the following shape:

- `reasoning`: optional string — free-form explanation (ignored by this class).
- `equivalent`: string — one of `"true"`, `"false"`, or `"dont_know"`.
- `transformations`: array of strings — an ordered list of allowed transformation names; may be empty.
- `preconditions`: optional array — objects describing preconditions for each
   transformation (ignored by this class).

For backwards compatibility the class also accepts the legacy line-oriented format where the first line
is `true`/`false` and subsequent lines are transformation names or a single sentinel like `No transformations needed`.

### Constructors
#### `public LLMResponse(boolean queriesAreEquivalent, List<String> transformationSteps)`
Direct construction with explicit values.

#### `public LLMResponse(String responseText)`
Parsing steps:
1. Null/blank check → `IllegalArgumentException`.
2. Normalize newlines; split into lines.
3. Validate first line is `true`/`false`.
4. Accumulate remaining non-blank lines.
5. If exactly one special sentinel line, clear list.
6. Validate every transformation is in `LLM.getSupportedTransformations()`; otherwise throw.
7. Snapshot list immutably.

### Accessors / Mutators
- `areQueriesEquivalent()` / `getTransformationSteps()`
- Setters present (`setQueriesAreEquivalent`, `setTransformationSteps`)—mutability exists; consider removing setters for immutability.

### Failure Modes
- Unsupported transformation name → `IllegalArgumentException`.
- Malformed first line → `IllegalArgumentException`.

### Notes
- Treats absence of transformations (empty) as either no steps needed or none applicable depending on first line semantics elsewhere.

---
## RelTreeNode
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/RelTreeNode.java`

### Purpose
Lightweight tree abstraction over Calcite `RelNode` graphs for structural comparisons (order-sensitive or order-insensitive). Each node holds a label string and ordered children.

### Key Methods
#### Constructors
- `RelTreeNode()` empty
- `RelTreeNode(String label)` labeled

#### `addChild(RelTreeNode child)`
Appends child preserving insertion order; ignores null.

#### `String canonicalDigest()`
Generates order-insensitive digest by recursively collecting child canonical digests and sorting them: `label[child1|child2|...]`. Leaf becomes `label[]`.

#### `boolean equalsIgnoreChildOrder(RelTreeNode other)` / static variant
Compare using canonical digest equality.

#### `toString()`
Indented, pre-order textual representation suitable for debugging.

### Equality & Hashing
Overrides `equals` and `hashCode` for order-sensitive comparison (normal list equality). Use canonical digest for permutation-insensitive comparison.

### Notes
- Does not attempt cycle detection (trees expected, produced by visitor that linearizes DAG with one parent assignment per visit path).
- Null labels printed as `(null)` to avoid NPEs.

---
## TransformationProbe
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/TransformationProbe.java`

### Purpose
Systematic diagnostic tool enumerating supported transformation rules and testing each rule individually against each query's optimized plan to identify crashing or problematic transformations.

### Workflow
1. Collect query cases from ORIGINAL and REWRITTEN sources via `FileIO.listQueryIds`.
2. For each rule name from `LLM.getSupportedTransformations()`:
   - Build planner & optimize query.
   - Apply transformation using `Calcite.applyTransformations(rel, List.of(rule))`.
   - Capture exceptions and classify result.
3. Write CSV report: `rule,source,queryId,result,error`.

### Result Codes
- `OK`: Transformation applied without exception and returned non-null plan.
- `PLAN_ERROR`: Base optimization or transformation produced null or failed before rule application.
- `CRASH`: Exception during transformation application.

### Private Helpers
- `loadCases(SqlSource)`: Builds case list with query ID and SQL text.
- `csv(String)`: Escapes commas/quotes for CSV integrity.
- `shortMsg(Throwable)`: Class name + truncated message (<=180 chars) for concise error logging.

### Failure Modes
- IO errors reading queries: skipped silently.
- Any throwable during planning recorded as `PLAN_ERROR`.

### Performance Considerations
- Quadratic loops (#rules × #queries) may be expensive; consider parallelization or caching if set grows.

---
## Test
**Location:** `plan_equivalence/src/main/java/com/ac/iisc/Test.java`

### Purpose
Ad-hoc runner for evaluating equivalence across selected queries and testing transformation + LLM-assisted alignment.

### Behavior
1. Defines `queryIDList` (currently hard-coded to `["Q2", "Q9", "Q20"]` with comments about problematic transformations/infinite loop potential).
2. For each query ID:
   - Reads original and rewritten SQL (`FileIO.readOriginalSqlQuery`, `FileIO.readRewrittenSqlQuery`).
   - Attempts direct equivalence (`Calcite.compareQueriesDebug`).
   - If not equivalent, applies a fixed list of transformations as a second attempt.
   - Invokes LLM (`LLM.getLLMResponse`) to obtain predicted equivalence and transformation suggestions.
   - If LLM asserts equivalence and returns steps, applies them and tracks correctness.
3. Prints per-query line and final aggregate statistics.

### Metrics Tracked
- Number of queries the LLM claimed equivalent / non-equivalent.
- Number of transformation lists provided.
- Number of provided transformation lists that successfully yield equivalence.

### Notes & Caveats
- Hard-coded transformations list may not be comprehensive; maintained manually.
- Potential infinite loops or crashes flagged by comments—should be further analyzed (e.g., rule interactions).
- Reading rewritten vs mutated queries: currently compares original vs rewritten only; mutated comparison logic could be added similarly.

### Extensibility Suggestions
- Parameterize query list via command-line args.
- Add timing statistics per comparison stage.
- Emit JSON/CSV summary for downstream analytics.

---
## General Cross-Cutting Concerns
### Error Handling Philosophy
- Avoid throwing on equivalence computation; treat failures as non-equivalent (`compareQueries` returns false).
- IO and parsing errors elevated (e.g., FileIO) to force caller correction.

### Potential Improvements
- Replace fragile LLM response string parsing with structured SDK accessors.
- Introduce caching layer for `FrameworkConfig` or prepared planners if metadata unchanged.
- Add cycle detection for `RelTreeNode` construction if future changes introduce shared subgraph duplication into tree representation.
- Provide richer reason codes for non-equivalence (e.g., difference classification: join-order, predicate mismatch, projection mismatch).

### Performance Hotspots
- HepPlanner passes over large join graphs.
- Canonical digest generation with repeated sorting.
- LLM calls (network latency) when batch processing many queries.

### Security / Operational Notes
- PostgreSQL credentials currently hard-coded; move to external config or environment variables.
- Validate LLM transformations before applying (already enforced by `LLMResponse`).
- Consider rate limiting or caching LLM responses for repeated plan pairs.

---
## Glossary
- **RelNode**: Calcite's relational algebra plan node interface.
- **Digest**: String produced by `RelOptUtil.toString` or custom canonicalization capturing plan structure.
- **HepPlanner**: Rule-based planner in Calcite applying a program of transformation rules.
- **Decorrelate**: Transform correlated subqueries into join + aggregate forms when possible.

---
## Usage Examples
### Compare Two Queries (Without Transformations)
```java
Planner planner = Frameworks.getPlanner(Calcite.getFrameworkConfig());
boolean eq = Calcite.compareQueries("SELECT * FROM lineitem", "SELECT * FROM lineitem", null);
System.out.println(eq); // true
```

### Apply Specific Transformations
```java
List<String> rules = List.of("ProjectMergeRule", "FilterJoinRule.FilterIntoJoinRule");
boolean eq = Calcite.compareQueries(sqlA, sqlB, rules);
```

### Obtain Cleaned JSON Plan
```java
String cleaned = GetQueryPlans.getCleanedQueryPlanJSONasString("SELECT * FROM orders o JOIN customer c ON o.custkey=c.custkey");
System.out.println(cleaned);
```

### Introspect Database Schema
```java
String schema = GetQueryPlans.getDatabaseSchema();
System.out.println(schema);
// Example (truncated):
// Schema: public
//   Table: customer
//     custkey : integer
//     name    : text
//   Table: orders
//     orderkey : integer
//     custkey  : integer
```

### LLM-Assisted Equivalence
```java
LLMResponse resp = LLM.getLLMResponse(sqlA, sqlB);
if (resp != null && resp.areQueriesEquivalent()) {
    boolean eq = Calcite.compareQueries(sqlA, sqlB, resp.getTransformationSteps());
    System.out.println("LLM-assisted equivalence: " + eq);
}
```
