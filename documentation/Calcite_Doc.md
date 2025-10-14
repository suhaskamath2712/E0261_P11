# Calcite.java — API Documentation

This document describes the public and non-trivial internal functions, helpers, and transformation classes implemented in `Calcite.java` (package `com.ac.iisc`). It explains purpose, inputs, outputs, usage examples, dependencies, edge cases, and related notes for each symbol.

> Location: `plan_equivalence/src/main/java/com/ac/iisc/Calcite.java`

## Overview

`Calcite.java` is a utility focused on two complementary capabilities:

- Parsing SQL strings (and extracting SQL blocks from files) into Apache Calcite `SqlNode` (AST) instances.
- Providing lightweight AST-level rewrites (schema-free) and a small framework to convert `SqlNode` -> `RelNode` (logical plan) and apply selected Calcite `RelOptRule` transformations via a `HepPlanner`.

The file intentionally separates syntactic (AST-level) rewrites from semantic, planner-based rule transformations. It contains helper builders to create a `FrameworkConfig` backed by a JDBC schema (so validation and conversion succeed) and helper utilities for comparison/equivalence checks.

Dependencies (required at build/runtime):

- org.apache.calcite:calcite-core
- A JDBC driver (for `buildJdbcFrameworkConfig`, e.g., `org.postgresql:postgresql`) when using JDBC-backed frameworks
- This project’s `FileIO` utility used for file reading

Notes on equivalence and correctness:

- AST-level transformations are syntactic and schema-free. They are safe for transformations that don't require type/catalog resolution.
- RelOptRule-based transformations (applied to `RelNode`) require a valid schema and may depend on optional adapter classes being on the classpath.
- The `equivalent` method uses a stringified plan comparison after normalization by a small rule set; this is a heuristic and not a formal proof.


---

## Symbols (alphabetical by function/class)

### `parseQueries(Path sqlFile)` (private static)

- Purpose: Read a `.sql` file that contains multiple queries split into documented blocks and return a map of Query IDs to SQL text.
- Signature: `private static Map<String, String> parseQueries(java.nio.file.Path sqlFile) throws IOException`
- Inputs:
  - `sqlFile`: path to the SQL file that uses header blocks such as:

    -- ====================
    -- Query ID: <ID>
    -- ====================
    <SQL here>;

- Outputs: `Map<String,String>` mapping `QueryID -> SQL` (SQL strings are normalized by stripping one trailing semicolon if present). Input order is preserved via `LinkedHashMap`.
- Errors: Throws `IOException` on file I/O errors.
- Edge cases & notes:
  - If the file contains no recognized blocks, an empty map is returned.
  - If two blocks use the same `Query ID`, the last block wins (map overwrite).
  - The regex is DOTALL-based and tuned to the current file format; unusual header formatting may prevent correct extraction.
- Related: `loadQueryFromFile` which uses this helper.


### `loadQueryFromFile(String path, String queryId)` (public static)

- Purpose: High-level helper to read a SQL file and return a single query by ID (or the first if no ID supplied).
- Signature: `public static String loadQueryFromFile(String path, String queryId) throws IOException`
- Inputs:
  - `path`: filesystem path to the .sql file
  - `queryId`: the Query ID to select; may be `null` or blank to return the first discovered query
- Output: SQL string (without trailing semicolon) or `null` if no queries are found or the specified ID does not exist.
- Errors: Throws `IOException` when reading the file.
- Edge cases:
  - Returns `null` rather than throwing when the requested ID is not found. This is by design to give callers flexibility.
- Example usage:

  String sql = Calcite.loadQueryFromFile("C:\\...\\original_queries.sql", "TPCH_Q11");


### `SQLtoSqlNode(String sql)` (public static)

- Purpose: Parse a SQL string into a Calcite `SqlNode` using a parser configuration with settings friendly to Postgres-style identifiers.
- Signature: `public static SqlNode SQLtoSqlNode(String sql) throws SqlParseException`
- Inputs:
  - `sql`: non-null, non-blank SQL statement
- Output: `SqlNode` representing the parsed statement (AST)
- Errors: Throws `SqlParseException` on syntax errors. Throws `IllegalArgumentException` if `sql` is null/blank.
- Parser configuration details:
  - `Lex.ORACLE` as a base lexical policy
  - `Quoting.DOUBLE_QUOTE`
  - `Casing.TO_LOWER` for unquoted identifiers
  - `caseSensitive = false` and `identifierMaxLength = 128`
- Notes:
  - If you need a different dialect, modify the `SqlParser.Config` accordingly.
- Example usage:

  SqlNode node = Calcite.SQLtoSqlNode("SELECT * FROM orders WHERE order_id = 10");


### `sqlNodesEqual(SqlNode left, SqlNode right)` (public static)

- Purpose: Structural equality test for two `SqlNode` instances using Calcite's deep-equality.
- Signature: `public static boolean sqlNodesEqual(SqlNode left, SqlNode right)`
- Inputs: Two `SqlNode` objects (either may be `null`).
- Output: boolean `true` if both are structurally equal, `false` otherwise.
- Behavior:
  - Returns `true` if both references are the same.
  - Returns `false` if one is `null` and the other is not.
  - Otherwise uses `left.equalsDeep(right, Litmus.IGNORE)` to perform a deep, structural comparison while ignoring parser position details.
- Notes:
  - Structural equality ignores formatting and positions, but requires identical AST shapes and literal values.


### `SqlTransformation` (private interface)

- Purpose: Internal functional interface representing a transformation over a `SqlNode`.
- Signature: `private interface SqlTransformation { SqlNode apply(SqlNode in); }`
- Notes: Implemented by the AST transformation `SqlShuttle` classes below.


### `applyTransformations(SqlNode plan, String[] list)` (public static)

- Purpose: Apply an ordered sequence of named AST-level transformations (syntactic rewrites) to a `SqlNode`.
- Signature: `public static SqlNode applyTransformations(SqlNode plan, String[] list)`
- Inputs:
  - `plan`: the input `SqlNode` AST (may be `null`)
  - `list`: array of transformation names (see available names below). Unknown names are ignored.
- Output: the transformed `SqlNode`. If input is `null` or the list is empty, the original plan is returned.
- Available transformation names (case-sensitive):
  - `simplifyDoubleNegation` — collapses `NOT(NOT(x))` to `x`.
  - `normalizeConjunctionOrder` — sorts operands of `AND` deterministically.
  - `pushNotDown` — applies De Morgan to push `NOT` inside `AND`/`OR`.
  - `foldBooleanConstants` — simplifies `AND`/`OR` with boolean constants.
- Edge cases:
  - Transformations are applied in order; some transforms may enable others.
  - All transformations are safe syntactic rewrites that do not require schema resolution.
- Example usage:

  SqlNode transformed = Calcite.applyTransformations(node, new String[]{"pushNotDown", "foldBooleanConstants"});


### `SimplifyDoubleNegation` (private static class)

- Purpose: A `SqlShuttle` transformation implementing the `simplifyDoubleNegation` transformation.
- Behavior: Rewrites `NOT(NOT(x))` into `x` during AST traversal.
- Usage: Instantiated and invoked by `applyTransformations` when the transformation name `simplifyDoubleNegation` is passed.
- Notes: Uses `SqlKind.NOT` and handles nested calls by first visiting children.


### `NormalizeConjunctionOrder` (private static class)

- Purpose: A `SqlShuttle` transformation implementing `normalizeConjunctionOrder`.
- Behavior: Flattens nested `AND` calls, sorts the operands deterministically by `toString()`, and rebuilds a left-associative `AND` tree.
- Usage: Invoked by `applyTransformations` for the `normalizeConjunctionOrder` name.
- Notes: This transformation only normalizes ordering (no semantic change).


### `PushNotDown` (private static class)

- Purpose: A `SqlShuttle` transformation implementing `pushNotDown`.
- Behavior: Applies De Morgan transformations:
  - `NOT(AND(a,b,...))` -> `OR(NOT(a), NOT(b), ...)`
  - `NOT(OR(a,b,...))` -> `AND(NOT(a), NOT(b), ...)`
- Usage: Invoked by `applyTransformations` for the `pushNotDown` name.
- Notes: This is a syntactic rewrite and does not require any type information.


### `FoldBooleanConstants` (private static class)

- Purpose: A `SqlShuttle` transformation implementing `foldBooleanConstants`.
- Behavior: Simplifies boolean combinations:
  - `a AND TRUE` -> `a`; `a AND FALSE` -> `FALSE`
  - `a OR TRUE` -> `TRUE`; `a OR FALSE` -> `a`
  - Also handles cases where all operands fold to constants.
- Usage: Invoked by `applyTransformations` for the `foldBooleanConstants` name.
- Notes: Uses private helpers `asBoolean` and `booleanLiteral`.


### `flatten(SqlCall call, SqlKind kind, List<SqlNode> out)` (private static helper)

- Purpose: Flatten nested n-ary SqlCalls of the same kind into a list of operands.
- Signature: `private static void flatten(SqlCall call, SqlKind kind, List<SqlNode> out)`
- Behavior: Recursively collects operands such that `AND(AND(a,b), c)` becomes `[a, b, c]`.
- Notes: Used by conjunction/De Morgan helpers.


### `buildNary(SqlOperator op, List<SqlNode> ops)` (private static helper)

- Purpose: Build a left-associative n-ary `SqlCall` from a list of operands using the supplied `SqlOperator`.
- Signature: `private static SqlNode buildNary(org.apache.calcite.sql.SqlOperator op, List<SqlNode> ops)`
- Behavior: If exactly two operands, directly create a binary call; otherwise, iteratively build left-associative calls.
- Notes: Preserves the operator semantics while creating properly-formed AST nodes.


### `asBoolean(SqlNode n)` (private static helper)

- Purpose: Convert a `SqlNode` into a `Boolean` when it is a boolean literal; return `null` otherwise.
- Signature: `private static Boolean asBoolean(SqlNode n)`
- Behavior: Checks for `SqlKind.LITERAL` and casts the literal value to `Boolean` when applicable.


### `booleanLiteral(boolean v)` (private static helper)

- Purpose: Construct a `SqlLiteral` boolean node with parser position `SqlParserPos.ZERO`.
- Signature: `private static SqlNode booleanLiteral(boolean v)`


### `toRelNode(SqlNode sqlNode, FrameworkConfig config)` (public static)

- Purpose: Validate a `SqlNode` and convert it into a Calcite `RelNode` logical plan using the supplied `FrameworkConfig` (which must provide schema/catalog information).
- Signature: `public static RelNode toRelNode(SqlNode sqlNode, FrameworkConfig config) throws ValidationException, RelConversionException`
- Inputs:
  - `sqlNode`: the parsed AST
  - `config`: a FrameworkConfig with a `defaultSchema` that exposes the tables/columns referenced by the `sqlNode`
- Output: a `RelNode` (logical plan)
- Errors: May throw `ValidationException` or `RelConversionException` if validation/conversion fails (for example if the schema is missing tables/columns).
- Notes & usage:
  - The method creates a `Planner` via `Frameworks.getPlanner(config)`, calls `validate` and then `rel` on the validated AST to obtain a `RelRoot` and returns `rel`.
  - Without an appropriate `FrameworkConfig` configured with a valid schema, conversion will fail.


### `buildJdbcFrameworkConfig(String jdbcUrl, String jdbcDriver, String user, String pass)` (public static)

- Purpose: Create a `FrameworkConfig` whose default schema is a JDBC-backed `JdbcSchema` sub-schema named `db`.
- Signature: `public static FrameworkConfig buildJdbcFrameworkConfig(String jdbcUrl, String jdbcDriver, String user, String pass)`
- Behavior:
  - Creates a Calcite `root` schema, constructs a `DataSource` with `JdbcSchema.dataSource(...)`, creates and adds a `JdbcSchema` under the name `db`, and sets `defaultSchema` to that sub-schema.
  - Reuses the same parser configuration settings as `loadQuerySQL`.
- Output: `FrameworkConfig` ready for use with `toRelNode`.
- Dependencies: Requires the JDBC driver for the target database to be present on the classpath at runtime.
- Notes: SQL that references tables must refer to objects available in the provided database schema.


### `buildPostgresFrameworkConfig(String host, int port, String database, String user, String pass)` (public static)

- Purpose: Convenience wrapper that composes a PostgreSQL JDBC URL and delegates to `buildJdbcFrameworkConfig` using the `org.postgresql.Driver`.
- Signature: `public static FrameworkConfig buildPostgresFrameworkConfig(String host, int port, String database, String user, String pass)`
- Example:

  FrameworkConfig cfg = Calcite.buildPostgresFrameworkConfig("localhost", 5432, "tpch", "postgres", "pwd");


### `equivalent(String sqlA, String sqlB, FrameworkConfig config)` (public static)

- Purpose: Heuristic equivalence test: parse two SQL strings to `SqlNode`, convert them to `RelNode` using the supplied `FrameworkConfig`, normalize both plans with a small deterministic rule set, and compare their stringified `RelNode` representations.
- Signature: `public static boolean equivalent(String sqlA, String sqlB, FrameworkConfig config) throws Exception`
- Inputs:
  - SQL strings `sqlA` and `sqlB`
  - `config`: `FrameworkConfig` providing schema details necessary for validation and conversion
- Output: boolean `true` if the normalized stringified plans are identical; `false` otherwise
- Throws: May throw parsing, validation, or conversion exceptions; the signature is broad (`throws Exception`) to surface any unexpected failure.
- Caveats:
  - The comparison is a heuristic; stringified plans can differ even when two queries are semantically equivalent. For higher confidence, extend normalization or compare canonical logical properties.


### `applyRelTransformations(RelNode root, String[] ruleNames)` (public static)

- Purpose: Apply a sequence of named `RelOptRule` instances to a `RelNode` using a `HepPlanner` program. Names map to `CoreRules` entries or adapter rule collections discovered reflectively.
- Signature: `public static RelNode applyRelTransformations(RelNode root, String[] ruleNames)`
- Inputs:
  - `root`: the input logical plan (`RelNode`) to transform
  - `ruleNames`: array of friendly rule names like `ProjectMergeRule`, `FilterMergeRule`, `JoinCommuteRule`, etc.
- Output: `RelNode` representing the planner's best expression found by `HepPlanner` (may be identical to input)
- Behavior & notes:
  - Builds a `HepProgramBuilder`, resolves each friendly name to `RelOptRule` instances using `resolveCoreRules` and `resolveAdapterRules`, and adds them to the program.
  - Unrecognized rule names are logged to `System.err` and skipped.
  - Adapter rules (e.g., `EnumerableRules`, `JdbcRules`) are discovered reflectively so there is no hard compile-time dependency on adapter packages.
- Limitations:
  - Some rules are version-dependent; `resolveCoreRules` uses reflection to find similar named constants when exact constants are not available across Calcite versions.


### `resolveCoreRules(String name)` (private static)

- Purpose: Map a friendly rule name to one or more `CoreRules` constants. Handles many common rule names and also delegates to `findCoreRulesMatching` for version-flexible discovery.
- Signature: `private static List<RelOptRule> resolveCoreRules(String name)`
- Output: list of resolved `RelOptRule` instances (possibly empty)
- Notes:
  - Designed to be resilient across Calcite versions by performing name-based reflective searches for some entries when direct constants may be absent.


### `findCoreRulesMatching(Predicate<String> namePredicate)` (private static)

- Purpose: Reflectively scan the public static fields of `CoreRules` and return those `RelOptRule` instances whose field names satisfy `namePredicate`.
- Signature: `private static List<RelOptRule> findCoreRulesMatching(Predicate<String> namePredicate)`
- Output: list of matching `RelOptRule` instances
- Error handling: Reflection failures are caught and ignored; partial results are returned where possible.


### `resolveAdapterRules(String adapterClassName, Predicate<String> nameFilter)` (private static)

- Purpose: Reflectively load adapter rule set classes (for example, `org.apache.calcite.adapter.enumerable.EnumerableRules` or `org.apache.calcite.adapter.jdbc.JdbcRules`) and return `RelOptRule` fields whose names satisfy `nameFilter`.
- Signature: `private static List<RelOptRule> resolveAdapterRules(String adapterClassName, Predicate<String> nameFilter)`
- Output: list of matching adapter `RelOptRule` instances (possibly empty)
- Behavior:
  - If the adapter class is not on the classpath, prints a note to `System.err` and returns an empty list.
  - Catches `Throwable` broadly to avoid crashing when reflection issues occur; logs a brief message on failure.
- Notes: This design intentionally avoids hard compile-time dependencies on adapter jars.


### `main(String[] args)` (public static)

- Purpose: Minimal demo entry point. Loads the first query in `ORIGINAL_SQL_FILE` (or a specific ID when provided as `args[0]`), parses it into a `SqlNode`, and prints the SQL and parsed AST.
- Signature: `public static void main(String[] args)`
- Behavior:
  - `args[0]` (optional): Query ID to select from `ORIGINAL_SQL_FILE`.
  - Prints to `stdout`/`stderr` for success and error reporting.
- Example invocation (via Maven exec plugin):

  mvn -q -f .\plan_equivalence\pom.xml -DskipTests exec:java -Dexec.mainClass=com.ac.iisc.Calcite

  mvn -q -f .\plan_equivalence\pom.xml -DskipTests exec:java -Dexec.mainClass=com.ac.iisc.Calcite -Dexec.args="TPCH_Q11"

- Notes: The main method is a convenience/debugging aid, not a test harness.


---

## Recommended usage patterns

1. Parse SQL and do AST-only rewrites (no schema required):

```java
String sql = Calcite.loadQueryFromFile("C:/.../original_queries.sql", "TPCH_Q11");
SqlNode node = Calcite.SQLtoSqlNode(sql);
SqlNode normalized = Calcite.applyTransformations(node, new String[]{"pushNotDown","foldBooleanConstants"});
```

2. Compare two SQL statements syntactically (AST-level):

```java
SqlNode a = Calcite.SQLtoSqlNode(sql1);
SqlNode b = Calcite.SQLtoSqlNode(sql2);
bool eq = Calcite.sqlNodesEqual(a, b);
```

3. Convert to RelNode and compare plans (requires a schema):

```java
FrameworkConfig cfg = Calcite.buildPostgresFrameworkConfig("localhost",5432,"tpch","postgres","pwd");
RelNode relA = Calcite.toRelNode(Calcite.SQLtoSqlNode(sqlA), cfg);
RelNode relB = Calcite.toRelNode(Calcite.SQLtoSqlNode(sqlB), cfg);
RelNode normA = Calcite.applyRelTransformations(relA, new String[]{"ProjectMergeRule","FilterMergeRule"});
RelNode normB = Calcite.applyRelTransformations(relB, new String[]{"ProjectMergeRule","FilterMergeRule"});
boolean same = String.valueOf(normA).equals(String.valueOf(normB));
```

Note: Replace the Postgres convenience with `buildJdbcFrameworkConfig` if you use a different database or driver.


---

## Limitations, tips, and follow-ups

- The `equivalent` method is heuristic-based. If you need rigorous equivalence checks, consider:
  - A larger, deterministic normalization rule set.
  - Comparing logical plan properties (e.g., relational algebra equivalence checks) instead of stringified output.
- Calcite versions vary. `resolveCoreRules` and `resolveAdapterRules` use reflection to increase resilience. If you pin a Calcite version, you can simplify rule resolution to direct `CoreRules` constants.
- To avoid surprising behavior with `JdbcSchema`, ensure the runtime classpath contains the JDBC driver used by `buildJdbcFrameworkConfig`.
- The AST regex used by `parseQueries` expects a particular header format. If your SQL files use a different marker style, update the regex or replace the block-extraction logic.


---

## Quick reference: Transformation names supported by `applyTransformations`

- `simplifyDoubleNegation`
- `normalizeConjunctionOrder`
- `pushNotDown`
- `foldBooleanConstants`


---

## Apache Calcite planner transformations supported by `applyRelTransformations`

`Calcite.applyRelTransformations(RelNode root, String[] ruleNames)` accepts friendly rule names that are mapped to Calcite `CoreRules` (and some adapter rules discovered reflectively). The following names are recognized by the implementation in `Calcite.java`:

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
  - `ValuesReduceRule` (resolved reflectively; version-dependent)
  - `ReduceExpressionsRule` (resolved reflectively; adds common reduce-expr rules)
  - `PruneEmptyRules` (collects available `PRUNE_EMPTY*` rules reflectively)

- Adapter rule sets (optional; discovered reflectively if on classpath)
  - `EnumerableRules` — selects a subset (e.g., PROJECT/FILTER/SORT related rules)
  - `JdbcRules` (or `JDBCRules`) — selects a subset (e.g., PROJECT/FILTER/SORT related rules)

Notes
- Some names are resolved reflectively to accommodate Calcite version differences. If a specific constant is unavailable, similar rule constants are searched by name.
- Adapter rules require the corresponding adapter classes on the classpath; otherwise they’re skipped with a log message.

