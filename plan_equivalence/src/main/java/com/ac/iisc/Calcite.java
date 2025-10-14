/*
 * =====================================================================================
 *  Calcite.java
 *
 *  Purpose
 *  -------
 *  This utility demonstrates how to:
 *    1) Load SQL text from a file that contains multiple queries delimited by
 *       documented blocks (with "-- Query ID: <ID>" headers),
 *    2) Select a specific query by its ID (or default to the first), and
 *    3) Parse that SQL into an Apache Calcite SqlNode (the SQL AST).
 *
 *  Key Features
 *  ------------
 *  - Public helper loadQueryFromFile(String path, String queryId): Returns a single SQL string
 *    selected by its ID from the file. If queryId is null/blank, returns the first query.
 *  - Public helper loadQuerySQL(String sql): Parses any SQL string into a Calcite SqlNode.
 *  - parseQueries(Path sqlFile): Internal helper that reads and extracts all queries into a Map.
 *
 *  Design Notes
 *  ------------
 *  - We intentionally keep parsing (regex) separate from Calcite parsing to make the logic testable.
 *  - The regex expects UNIX newlines (\n). On Windows, Files.readString still returns a string with
 *    \n separators (Java normalizes line endings), so the pattern remains valid. If your tooling preserves
 *    \r\n, the DOTALL and wildcard sections still match safely, but you can also normalize line endings
 *    (content.replace("\r\n", "\n")) in parseQueries if needed.
 *  - Calcite's PostgreSQL dialect is not a separate Lex; using Lex.ORACLE with double quotes and
 *    lower-casing unquoted identifiers provides a close approximation for generic SQL parsing.
 *  - This class uses FileIO.readTextFile for all file reads to centralize I/O behavior.
 *
 *  Usage Examples
 *  --------------
 *  // 1) Load the first query from the file and parse to SqlNode
 *  String sql = Calcite.loadQueryFromFile("C:\\...\\original_queries.sql", null);
 *  SqlNode node = Calcite.loadQuerySQL(sql);
 *
 *  // 2) Load a specific query by ID and parse
 *  String sql2 = Calcite.loadQueryFromFile("C:\\...\\original_queries.sql", "TPCH_Q11");
 *  SqlNode node2 = Calcite.loadQuerySQL(sql2);
 *
 *  Potential Extensions
 *  --------------------
 *  - Add a validate step with a Frameworks/Planner configuration and schemas if needed.
 *  - Convert SqlNode to RelNode (logical plan) via a Planner when a schema is available.
 *  - Expose parseQueries in FileIO for reuse across modules.
 *
 *  Dependencies
 *  ------------
 *  - org.apache.calcite:calcite-core (for SqlParser/SqlNode)
 *  - This module's FileIO utility for file reading
 *
 *  Thread-safety
 *  -------------
 *  - Methods are stateless (aside from constants) and can be used concurrently.
 *
 *  Error Handling
 *  --------------
 *  - loadQueryFromFile returns null when not found; callers can decide how to proceed.
 *  - loadQuerySQL throws SqlParseException on syntax errors.
 *
 * =====================================================================================
 */
package com.ac.iisc;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Litmus;

/**
 * Calcite parsing helper: reads query text from files (by ID) and parses SQL strings
 * into Apache Calcite SqlNode instances. This class is designed to be called from
 * other classes (e.g., unit tests, runners) without requiring a Calcite schema.
 */
public class Calcite {

    /**
     * Default path to the original queries file. This is used by main for convenience,
     * but external callers are encouraged to pass explicit paths to loadQueryFromFile.
     */
    private static final Path ORIGINAL_SQL_FILE = Paths.get(
        "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql");

    /**
     * Parse queries from the provided SQL file using blocks like:
     * -- =====
     * -- Query ID: <ID>
     * -- =====
     * <SQL...>;
     * 
     * Extracts queries from a single .sql file into a map of (QueryID -> SQL text).
     *
     * Expected block format:
     *   -- =============================
     *   -- Query ID: <SOME_ID>
     *   -- =============================
     *   SELECT ...
     *   ...
     *
     * Implementation details:
     * - We leverage a regex with DOTALL to capture across multiple lines.
     * - Group 1 captures the query ID; Group 2 captures the SQL text until the next header.
     * - We remove a trailing semicolon to simplify downstream parsing/combining.
     *
     * Pitfalls:
     * - Ensure your blocks are consistently formatted; stray headers can break grouping.
     * - If your query text itself contains sequences that look like headers, consider tightening
     *   the regex or using a more robust sectioning scheme.
     */
    private static Map<String, String> parseQueries(java.nio.file.Path sqlFile) throws IOException {
        // Read entire file (UTF-8) via FileIO to centralize I/O behavior and error handling.
        String content = FileIO.readTextFile(sqlFile.toString());
        Pattern p = Pattern.compile(
                "-- =+.*?\n-- Query ID: ([\\w-]+).*?\n-- =+.*?\n(.*?)(?=\n-- =+|$)",
                Pattern.DOTALL);
        Matcher m = p.matcher(content);
        Map<String, String> map = new LinkedHashMap<>(); // keep input order
        while (m.find()) {
            String id = m.group(1).trim();      // Extracted Query ID
            String sql = m.group(2).trim();     // Extracted SQL body for this ID

            // Remove one trailing semicolon to normalize statements.
            // This helps avoid duplicate semicolons if you later combine statements.
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            if (!id.isEmpty() && !sql.isEmpty()) {
                map.put(id, sql);
            }
        }
        return map;
    }

    /**
     * Public helper to load a single SQL query from a file by Query ID.
     * If queryId is null/blank, returns the first query in the file (by order).
     * Returns null if no queries are found or the ID does not exist.
     * 
     * Load a single SQL statement from a file by its Query ID.
     *
     * Contract
     * --------
     * Inputs:
     *  - path: Absolute path to the .sql file with query blocks.
     *  - queryId: The "Query ID" value declared in the header; when null/blank, returns the first query.
     * Outputs:
     *  - Returns the SQL text without a trailing semicolon, or null if not found / no queries present.
     * Errors:
     *  - Throws IOException if the file cannot be read.
     *
     * Edge Cases & Notes:
     *  - If multiple queries share the same ID (shouldn’t happen), the last one wins.
     *  - Returns null rather than throwing when an ID is not present; this makes it ergonomic for callers
     *    who often want to fallback or report gracefully.
     */
    public static String loadQueryFromFile(String path, String queryId) throws IOException {
        Path p = Paths.get(path);
        Map<String, String> queries = parseQueries(p);
        if (queries.isEmpty()) {
            return null; // No queries in file
        }
        if (queryId == null || queryId.isBlank()) {
            return queries.values().iterator().next(); // First query by input order
        }
        return queries.get(queryId); // May be null if not found (by design)
    }

    /**
     * Parse a SQL string using Apache Calcite and return the parsed SqlNode.
     * Uses a Postgres-friendly parser config (double quotes, lower-cased unquoted identifiers, case-insensitive).
     * 
     * Parse a SQL string into a Calcite SqlNode.
     *
     * Why these parser settings?
     *  - Lex.ORACLE: Calcite does not ship a dedicated PostgreSQL Lex; ORACLE is a reasonable base.
     *  - Quoting.DOUBLE_QUOTE: Standard SQL quoting for identifiers (matches Postgres behavior for quoted IDs).
     *  - Casing.TO_LOWER + case-insensitive: Unquoted identifiers become lower-case (similar to Postgres).
     *  - identifierMaxLength: Keeps identifiers within a typical bound.
     *
     * @param sql The SQL statement to parse (must be non-null/non-blank)
     * @return The parsed SqlNode (AST)
     * @throws SqlParseException If the SQL is syntactically invalid.
     * @throws IllegalArgumentException If the SQL is null/blank.
     */
    public static SqlNode SQLtoSqlNode(String sql) throws SqlParseException {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql must not be null or blank");
        }
        // Configure Calcite's SqlParser; tweak these if your SQL dialect needs differ.
        SqlParser.Config config = SqlParser.config()
                .withLex(Lex.ORACLE)
                .withQuoting(Quoting.DOUBLE_QUOTE)
                .withUnquotedCasing(Casing.TO_LOWER)
                .withCaseSensitive(false)
                .withIdentifierMaxLength(128);

        // Create a parser instance bound to our SQL string and parse to a statement/Query SqlNode.
        SqlParser parser = SqlParser.create(sql, config);
        return parser.parseQuery();
    }

    /**
     * Compare two SqlNodes for structural equality using Calcite's equalsDeep API.
     * This checks that the parsed SQL abstract syntax trees are the same shape and values
     * (ignoring parser source positions). Returns false if either node is null or the
     * structures differ.
     */
    public static boolean sqlNodesEqual(SqlNode left, SqlNode right) {
        if (left == right) return true;
        if (left == null || right == null) return false;
        // equalsDeep performs a deep, structural comparison. Litmus.IGNORE avoids throwing
        // and simply returns a boolean indicating success/failure.
        return left.equalsDeep(right, Litmus.IGNORE);
    }

    // =====================================================================================
    //  Lightweight AST Transformations (SqlNode-level)
    //
    //  Purpose
    //  -------
    //  Provide a simple mechanism to apply a sequence of named transformations to a Calcite
    //  SqlNode (the parsed SQL AST) without requiring schemas or converting to RelNode.
    //
    //  Scope & Limitations
    //  -------------------
    //  - These are syntactic/AST-level rewrites using SqlShuttle visitors. They do NOT require
    //    catalog resolution or type information and thus work without a schema.
    //  - For semantic/logical "rule" transformations (RelOptRules) you would normally use a
    //    Planner (HepPlanner/Volcano) over RelNodes, which requires a schema. This utility is
    //    intentionally schema-free and keeps to safe, local rewrites.
    //
    //  Available transformation names (case-sensitive):
    //  - "simplifyDoubleNegation":    NOT(NOT(x))  => x
    //  - "normalizeConjunctionOrder": Sort operands of AND deterministically (by toString)
    //  - "pushNotDown":               Apply De Morgan on NOT(AND/OR(...)) to push NOT inward
    //  - "foldBooleanConstants":      Simplify AND/OR with TRUE/FALSE (e.g., a AND TRUE => a)
    //
    //  Usage:
    //     SqlNode transformed = Calcite.applyTransformations(node,
    //         new String[]{"simplifyDoubleNegation", "pushNotDown", "foldBooleanConstants"});
    // =====================================================================================

    /** Functional interface for AST transformations. */
    private interface SqlTransformation {
        SqlNode apply(SqlNode in);
    }

    /**
     * Apply a list of AST-level transformations, in order, to the given plan.
     *
     * Contract
     * --------
     * Inputs:
     *  - plan: Parsed SQL AST (SqlNode). Must be non-null to have any effect.
     *  - list: Array of transformation names (see list above). Unknown names are ignored.
     * Output:
     *  - A transformed SqlNode (same instance when no changes are made).
     * Error Handling:
     *  - Null/empty list results in a no-op; the original plan is returned.
     */
    public static SqlNode applyTransformations(SqlNode plan, String[] list) {
        if (plan == null || list == null || list.length == 0) return plan;
        SqlNode out = plan;
        for (String name : list) {
            SqlTransformation t = null;
            if ("simplifyDoubleNegation".equals(name)) {
                t = new SimplifyDoubleNegation();
            } else if ("normalizeConjunctionOrder".equals(name)) {
                t = new NormalizeConjunctionOrder();
            } else if ("pushNotDown".equals(name)) {
                t = new PushNotDown();
            } else if ("foldBooleanConstants".equals(name)) {
                t = new FoldBooleanConstants();
            } else {
                t = null;
            }
            if (t != null) {
                out = t.apply(out);
            }
        }
        return out;
    }

    /**
     * NOT(NOT(x)) => x
     */
    private static class SimplifyDoubleNegation extends SqlShuttle implements SqlTransformation {
        @Override public SqlNode apply(SqlNode in) { return in.accept(this); }

        @Override public SqlNode visit(SqlCall call) {
            // First rewrite children
            call = (SqlCall) super.visit(call);
            if (call.getKind() == SqlKind.NOT) {
                SqlNode inner = call.operand(0);
                if (inner instanceof SqlCall) {
                    SqlCall innerCall = (SqlCall) inner;
                    if (innerCall.getKind() == SqlKind.NOT) {
                        // NOT(NOT(x)) -> x
                        return innerCall.operand(0);
                    }
                }
            }
            return call;
        }
    }

    /**
     * Sort AND operands deterministically (by toString). This is a normalization step only.
     */
    private static class NormalizeConjunctionOrder extends SqlShuttle implements SqlTransformation {
        @Override public SqlNode apply(SqlNode in) { return in.accept(this); }

        @Override public SqlNode visit(SqlCall call) {
            call = (SqlCall) super.visit(call);
            if (call.getKind() == SqlKind.AND) {
                List<SqlNode> list = new ArrayList<>();
                flatten(call, SqlKind.AND, list);
                list.sort(Comparator.comparing(Object::toString));
                return buildNary(SqlStdOperatorTable.AND, list);
            }
            return call;
        }
    }

    /**
     * Push NOT inward using De Morgan over AND/OR.
     * NOT(AND(a,b,...)) => OR(NOT a, NOT b, ...)
     * NOT(OR(a,b,...))  => AND(NOT a, NOT b, ...)
     */
    private static class PushNotDown extends SqlShuttle implements SqlTransformation {
        @Override public SqlNode apply(SqlNode in) { return in.accept(this); }

        @Override public SqlNode visit(SqlCall call) {
            call = (SqlCall) super.visit(call);
            if (call.getKind() == SqlKind.NOT) {
                SqlNode inner = call.operand(0);
                if (inner instanceof SqlCall) {
                    SqlCall innerCall = (SqlCall) inner;
                    if (innerCall.getKind() == SqlKind.AND || innerCall.getKind() == SqlKind.OR) {
                        List<SqlNode> ops = new ArrayList<>();
                        flatten(innerCall, innerCall.getKind(), ops);
                        List<SqlNode> nots = new ArrayList<>(ops.size());
                        for (SqlNode n : ops) {
                            nots.add(SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, n));
                        }
                        if (innerCall.getKind() == SqlKind.AND) {
                            return buildNary(SqlStdOperatorTable.OR, nots);
                        } else {
                            return buildNary(SqlStdOperatorTable.AND, nots);
                        }
                    }
                }
            }
            return call;
        }
    }

    /**
     * Simplify boolean constants in AND/OR expressions:
     * - a AND TRUE => a; a AND FALSE => FALSE
     * - a OR TRUE => TRUE; a OR FALSE => a
     */
    private static class FoldBooleanConstants extends SqlShuttle implements SqlTransformation {
        @Override public SqlNode apply(SqlNode in) { return in.accept(this); }

        @Override public SqlNode visit(SqlCall call) {
            call = (SqlCall) super.visit(call);
            if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
                List<SqlNode> ops = new ArrayList<>();
                flatten(call, call.getKind(), ops);

                boolean isAnd = call.getKind() == SqlKind.AND;
                List<SqlNode> kept = new ArrayList<>();
                for (SqlNode n : ops) {
                    Boolean b = asBoolean(n);
                    if (b == null) {
                        kept.add(n);
                        continue;
                    }
                    if (isAnd) {
                        if (!b) {
                            // AND FALSE => FALSE
                            return booleanLiteral(false);
                        }
                        // drop TRUE
                    } else { // OR
                        if (b) {
                            // OR TRUE => TRUE
                            return booleanLiteral(true);
                        }
                        // drop FALSE
                    }
                }
                if (kept.isEmpty()) {
                    // AND of all TRUE => TRUE; OR of all FALSE => FALSE
                    return booleanLiteral(isAnd);
                }
                if (kept.size() == 1) {
                    return kept.get(0);
                }
                return buildNary(isAnd ? SqlStdOperatorTable.AND : SqlStdOperatorTable.OR, kept);
            }
            return call;
        }
    }

    // ----------------------------- helpers ---------------------------------
    /** Flatten nested n-ary calls of the same kind (e.g., AND(AND(a,b), c) => [a,b,c]). */
    private static void flatten(SqlCall call, SqlKind kind, List<SqlNode> out) {
        for (int i = 0; i < call.getOperandList().size(); i++) {
            SqlNode n = call.getOperandList().get(i);
            if (n instanceof SqlCall) {
                SqlCall c = (SqlCall) n;
                if (c.getKind() == kind) {
                    flatten(c, kind, out);
                } else {
                    out.add(n);
                }
            } else {
                out.add(n);
            }
        }
    }

    /** Build an n-ary call (left-associative) from a list of operands. */
    private static SqlNode buildNary(org.apache.calcite.sql.SqlOperator op, List<SqlNode> ops) {
        if (ops.size() == 2) {
            return op.createCall(SqlParserPos.ZERO, ops.get(0), ops.get(1));
        }
        // Build left-associative tree: (((a op b) op c) op d) ...
        SqlNode acc = op.createCall(SqlParserPos.ZERO, ops.get(0), ops.get(1));
        for (int i = 2; i < ops.size(); i++) {
            acc = op.createCall(SqlParserPos.ZERO, acc, ops.get(i));
        }
        return acc;
    }

    /** Convert a SqlNode to Boolean if it is a boolean literal, else null. */
    private static Boolean asBoolean(SqlNode n) {
        if (n.getKind() == SqlKind.LITERAL && n instanceof SqlLiteral) {
            SqlLiteral lit = (SqlLiteral) n;
            Object v = lit.getValue();
            if (v instanceof Boolean) return (Boolean) v;
        }
        return null;
    }

    /** Construct a boolean literal SqlNode. */
    private static SqlNode booleanLiteral(boolean v) {
        return SqlLiteral.createBoolean(v, SqlParserPos.ZERO);
    }

    // =====================================================================================
    //  RelNode Transformation Support (Planner rules)
    //
    //  Many of Apache Calcite's powerful transformations are defined as RelOptRules and
    //  operate on RelNode trees (logical plans) rather than SqlNode ASTs. The methods
    //  below provide a lightweight way to:
    //    1) Convert a SqlNode into a RelNode using a FrameworkConfig (requires a schema), and
    //    2) Apply a sequence of named CoreRules via a HepPlanner.
    //
    //  IMPORTANT
    //  ---------
    //  - You MUST provide a FrameworkConfig with a valid default schema (tables, fields) for
    //    conversion to RelNode to succeed. Without a schema, Calcite cannot validate or convert.
    //  - Some rules are adapter-specific (e.g., JDBC/Enumerable rules that push down to a source)
    //    and require appropriate conventions and adapters available on the classpath.
    //
    //  Example usage:
    //    FrameworkConfig config = ... // build with your schema
    //    RelNode rel = Calcite.toRelNode(sqlNode, config);
    //    RelNode transformed = Calcite.applyRelTransformations(rel, new String[] {
    //        "ProjectMergeRule", "FilterMergeRule", "JoinCommuteRule"
    //    });
    // =====================================================================================

    /**
     * Convert a SqlNode to a RelNode using a provided FrameworkConfig (must include schema).
     *
     * @param sqlNode Parsed SQL AST
     * @param config  Framework configuration including default schema and parser settings
     * @return RelNode logical plan
     * @throws ValidationException    if validation fails
     * @throws RelConversionException if conversion fails
     */
    public static RelNode toRelNode(SqlNode sqlNode, FrameworkConfig config)
            throws ValidationException, RelConversionException {
        Planner planner = Frameworks.getPlanner(config);
        // The Planner API supports passing an existing SqlNode to validate/rel.
        SqlNode validated = planner.validate(sqlNode);
        RelRoot root = planner.rel(validated);
        return root.rel;
    }

    /**
     * Convert a SQL string to a RelNode using the Planner's parse→validate→rel flow.
     * This ensures the Planner's internal state machine (RESET → PARSED → VALIDATED) is followed
     * consistently across Calcite versions.
     */
    public static RelNode toRelNode(String sql, FrameworkConfig config)
            throws SqlParseException, ValidationException, RelConversionException {
        Planner planner = Frameworks.getPlanner(config);
        SqlNode parsed = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        RelRoot root = planner.rel(validated);
        return root.rel;
    }

    /**
     * Convenience: Load a query by ID from a .sql file and convert to RelNode using the safe
     * parse→validate→rel flow. Reduces boilerplate in callers.
     *
     * @param sqlFilePath Absolute path to the .sql file with Query ID blocks
     * @param queryId     The Query ID header to load (if null/blank, first query is used)
     * @param config      FrameworkConfig with default schema and parser settings
     */
    public static RelNode loadRelFromFile(String sqlFilePath, String queryId, FrameworkConfig config)
            throws IOException, SqlParseException, ValidationException, RelConversionException {
        String sql = loadQueryFromFile(sqlFilePath, queryId);
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("No SQL found for Query ID: " + String.valueOf(queryId));
        }
        return toRelNode(sql, config);
    }

    /**
     * Convenience: Load a query by ID from the default ORIGINAL_SQL_FILE and convert to RelNode.
     */
    public static RelNode loadRelFromDefault(String queryId, FrameworkConfig config)
            throws IOException, SqlParseException, ValidationException, RelConversionException {
        return loadRelFromFile(ORIGINAL_SQL_FILE.toString(), queryId, config);
    }

    /**
     * Build a FrameworkConfig backed by a JDBC schema.
     *
     * Why: Calcite must know your tables/columns to validate and convert SqlNode -> RelNode.
     * This helper creates a simple root schema with a sub-schema named "db" that wraps a JDBC
     * DataSource. Your SQL should reference objects available in that database.
     *
     * Notes:
     * - The parser config mirrors loadQuerySQL (double quotes, lower-case unquoted, case-insensitive).
     * - No explicit programs/rules are installed here; use applyRelTransformations to add rules.
     */
    public static FrameworkConfig buildJdbcFrameworkConfig(String jdbcUrl,
                               String jdbcDriver,
                               String user,
                               String pass) {
    // Create a root schema and add a JDBC sub-schema named "db".
    SchemaPlus root = Frameworks.createRootSchema(true);
    DataSource ds = JdbcSchema.dataSource(jdbcUrl, jdbcDriver, user, pass);
    JdbcSchema jdbc = JdbcSchema.create(root, "db", ds, null, null);
    root.add("db", jdbc);

    // Reuse our parser settings so parsing behavior is consistent across APIs.
    SqlParser.Config parserCfg = SqlParser.config()
        .withLex(Lex.ORACLE)
        .withQuoting(Quoting.DOUBLE_QUOTE)
        .withUnquotedCasing(Casing.TO_LOWER)
        .withCaseSensitive(false)
        .withIdentifierMaxLength(128);

    return Frameworks.newConfigBuilder()
        .defaultSchema(root.getSubSchema("db"))
        .parserConfig(parserCfg)
        .build();
    }

    /**
     * Convenience builder for PostgreSQL-backed FrameworkConfig. This composes the JDBC URL
     * and driver for you, e.g.,
     *   buildPostgresFrameworkConfig("localhost", 5432, "tpch", "postgres", "123")
     */
    public static FrameworkConfig buildPostgresFrameworkConfig(String host, int port, String database,
                                                               String user, String pass) {
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        return buildJdbcFrameworkConfig(url, "org.postgresql.Driver", user, pass);
    }

    /**
     * Compare two SQL strings for logical equivalence by converting them to RelNodes using
     * a provided schema (FrameworkConfig) and then normalizing with a small set of common
    * rules. The comparison uses RelNode.toString() on the final RelNodes.
     *
     * Caveats:
     * - Equivalence via stringified plans is a heuristic; for rigorous proofs you may need
     *   canonicalization strategies tailored to your workload.
     */
    public static boolean equivalent(String sqlA, String sqlB, FrameworkConfig config) throws Exception {
    // Parse
    SqlNode a = SQLtoSqlNode(sqlA);
    SqlNode b = SQLtoSqlNode(sqlB);

    // Convert to RelNodes
    RelNode relA = toRelNode(a, config);
    RelNode relB = toRelNode(b, config);

    // Optionally normalize both plans with a small deterministic rule set
    String[] norm = new String[] {
        "ProjectMergeRule",
        "FilterMergeRule",
        "JoinCommuteRule",
        "AggregateRemoveRule",
        "SortRemoveRule",
        "UnionMergeRule"
    };
    relA = applyRelTransformations(relA, norm);
    relB = applyRelTransformations(relB, norm);

    // Compare plan strings (stringified logical plans)
    String sA = String.valueOf(relA);
    String sB = String.valueOf(relB);
    return sA.equals(sB);
    }

    /**
     * Apply a sequence of named CoreRules (RelOptRules) to a RelNode using a HepPlanner.
     * Unknown or unavailable rule names are logged (via System.err) and skipped.
     *
     * @param root      input logical plan
     * @param ruleNames names like "ProjectMergeRule", "FilterMergeRule", etc.
     * @return the best expression found by the HepPlanner (possibly the same instance)
     */
    public static RelNode applyRelTransformations(RelNode root, String[] ruleNames) {
        if (root == null || ruleNames == null || ruleNames.length == 0) return root;

        HepProgramBuilder pb = new HepProgramBuilder();
        for (String n : ruleNames) {
            List<RelOptRule> rules = resolveCoreRules(n);
            if (rules.isEmpty()) {
                // Try adapter rule sets via reflection if requested
                if ("EnumerableRules".equalsIgnoreCase(n)) {
                    rules = resolveAdapterRules("org.apache.calcite.adapter.enumerable.EnumerableRules",
                            name -> name.contains("PROJECT") || name.contains("FILTER") || name.contains("SORT"));
                } else if ("JdbcRules".equalsIgnoreCase(n) || "JDBCRules".equalsIgnoreCase(n)) {
                    rules = resolveAdapterRules("org.apache.calcite.adapter.jdbc.JdbcRules",
                            name -> name.contains("PROJECT") || name.contains("FILTER") || name.contains("SORT"));
                }
            }
            if (rules.isEmpty()) {
                System.err.println("[Calcite][WARN] Unknown/unsupported transformation name: " + n +
                        ". No rules added for this entry. Ensure you are using CoreRules-friendly names or available adapter sets.");
            } else {
                for (RelOptRule r : rules) pb.addRuleInstance(r);
            }
        }

        HepProgram program = pb.build();
        HepPlanner hep = new HepPlanner(program);
        hep.setRoot(root);
        return hep.findBestExp();
    }

    /**
     * Map friendly rule names to Calcite CoreRules. If a name is not recognized in the
     * current Calcite version, this returns null and the caller should skip it.
     */
    private static List<RelOptRule> resolveCoreRules(String name) {
        List<RelOptRule> out = new ArrayList<>();

        // Projection Transformations
        if ("ProjectMergeRule".equals(name)) out.add(CoreRules.PROJECT_MERGE);
        else if ("ProjectRemoveRule".equals(name)) out.add(CoreRules.PROJECT_REMOVE);
        else if ("ProjectJoinTransposeRule".equals(name)) out.add(CoreRules.PROJECT_JOIN_TRANSPOSE);
        else if ("ProjectFilterTransposeRule".equals(name)) out.add(CoreRules.PROJECT_FILTER_TRANSPOSE);
        else if ("ProjectSetOpTransposeRule".equals(name)) out.add(CoreRules.PROJECT_SET_OP_TRANSPOSE);
        else if ("ProjectTableScanRule".equals(name)) {
            // Version-dependent: look for any CoreRules constant containing PROJECT + TABLE + SCAN
            out.addAll(findCoreRulesMatching(s -> s.contains("PROJECT") && s.contains("TABLE") && s.contains("SCAN")));
        }

        // Filter Transformations
        else if ("FilterMergeRule".equals(name)) out.add(CoreRules.FILTER_MERGE);
        else if ("FilterProjectTransposeRule".equals(name)) out.add(CoreRules.FILTER_PROJECT_TRANSPOSE);
        else if ("FilterJoinRule".equals(name)) out.add(CoreRules.FILTER_INTO_JOIN);
        else if ("FilterAggregateTransposeRule".equals(name)) out.add(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        else if ("FilterWindowTransposeRule".equals(name)) out.add(CoreRules.FILTER_WINDOW_TRANSPOSE);
        else if ("FilterTableScanRule".equals(name)) {
            // Version/adapter-dependent
            out.addAll(findCoreRulesMatching(s -> s.contains("FILTER") && s.contains("TABLE") && s.contains("SCAN")));
        }

        // Join Transformations
        else if ("JoinCommuteRule".equals(name)) out.add(CoreRules.JOIN_COMMUTE);
        else if ("JoinAssociateRule".equals(name)) out.add(CoreRules.JOIN_ASSOCIATE);
        else if ("JoinPushExpressionsRule".equals(name)) out.add(CoreRules.JOIN_PUSH_EXPRESSIONS);
        else if ("JoinConditionPushRule".equals(name)) out.add(CoreRules.JOIN_CONDITION_PUSH);
        else if ("SemiJoinRule".equals(name)) {
            // Version-dependent; attempt to find any SEMI_JOIN related rule
            out.addAll(findCoreRulesMatching(s -> s.contains("SEMI") && s.contains("JOIN")));
        }

        // Aggregate Transformations
        else if ("AggregateProjectPullUpConstantsRule".equals(name)) out.add(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
        else if ("AggregateRemoveRule".equals(name)) out.add(CoreRules.AGGREGATE_REMOVE);
        else if ("AggregateJoinTransposeRule".equals(name)) out.add(CoreRules.AGGREGATE_JOIN_TRANSPOSE);
        else if ("AggregateUnionTransposeRule".equals(name)) out.add(CoreRules.AGGREGATE_UNION_TRANSPOSE);
        else if ("AggregateProjectMergeRule".equals(name)) out.add(CoreRules.AGGREGATE_PROJECT_MERGE);
        else if ("AggregateCaseToFilterRule".equals(name)) out.add(CoreRules.AGGREGATE_CASE_TO_FILTER);

        // Sort & Limit Transformations
        else if ("SortRemoveRule".equals(name)) out.add(CoreRules.SORT_REMOVE);
        else if ("SortUnionTransposeRule".equals(name)) out.add(CoreRules.SORT_UNION_TRANSPOSE);
        else if ("SortProjectTransposeRule".equals(name)) out.add(CoreRules.SORT_PROJECT_TRANSPOSE);
        else if ("SortJoinTransposeRule".equals(name)) out.add(CoreRules.SORT_JOIN_TRANSPOSE);

        // Set Operations
        else if ("UnionMergeRule".equals(name)) out.add(CoreRules.UNION_MERGE);
        else if ("UnionPullUpConstantsRule".equals(name)) out.add(CoreRules.UNION_PULL_UP_CONSTANTS);
        else if ("IntersectToDistinctRule".equals(name)) out.add(CoreRules.INTERSECT_TO_DISTINCT);
        else if ("MinusToDistinctRule".equals(name)) out.add(CoreRules.MINUS_TO_DISTINCT);

        // Window Transformations
        else if ("ProjectWindowTransposeRule".equals(name)) out.add(CoreRules.PROJECT_WINDOW_TRANSPOSE);
        else if ("FilterWindowTransposeRule".equals(name)) out.add(CoreRules.FILTER_WINDOW_TRANSPOSE);

        // Other Rules
        else if ("ValuesReduceRule".equals(name)) {
            // Version-dependent; include any VALUES + REDUCE rule(s)
            out.addAll(findCoreRulesMatching(s -> s.contains("VALUES") && s.contains("REDUCE")));
        }
        else if ("ReduceExpressionsRule".equals(name)) {
            // Add common reduce-expressions rules present across Calcite versions
            out.addAll(findCoreRulesMatching(s -> s.contains("REDUCE") && s.contains("EXPR")));
        }
        else if ("PruneEmptyRules".equals(name)) {
            // Collect all PRUNE_EMPTY* rules available in this version
            out.addAll(findCoreRulesMatching(s -> s.startsWith("PRUNE_EMPTY")));
        }

        return out;
    }

    /** Find all CoreRules public static RelOptRule fields whose names match a predicate. */
    private static List<RelOptRule> findCoreRulesMatching(Predicate<String> namePredicate) {
        List<RelOptRule> out = new ArrayList<>();
        try {
            for (java.lang.reflect.Field f : CoreRules.class.getFields()) {
                if (!java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
                if (!RelOptRule.class.isAssignableFrom(f.getType())) continue;
                String fname = f.getName();
                if (namePredicate.test(fname)) {
                    Object val = f.get(null);
                    if (val instanceof RelOptRule) out.add((RelOptRule) val);
                }
            }
        } catch (Throwable ignore) {
            // Reflection failures: ignore and return what we collected
        }
        return out;
    }

    /**
     * Resolve adapter rule sets via reflection (EnumerableRules / JdbcRules) without hard
     * compile-time dependencies. Only adds rules whose field names satisfy the provided filter.
     */
    private static List<RelOptRule> resolveAdapterRules(String adapterClassName,
                                                                  Predicate<String> nameFilter) {
        List<RelOptRule> out = new ArrayList<>();
        try {
            Class<?> cls = Class.forName(adapterClassName);
            for (java.lang.reflect.Field f : cls.getFields()) {
                if (!java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
                if (!RelOptRule.class.isAssignableFrom(f.getType())) continue;
                String fname = f.getName();
                if (nameFilter.test(fname)) {
                    Object val = f.get(null);
                    if (val instanceof RelOptRule) out.add((RelOptRule) val);
                }
            }
        } catch (ClassNotFoundException e) {
            System.err.println("[Calcite] Adapter rules not available: " + adapterClassName);
        } catch (Throwable t) {
            System.err.println("[Calcite] Failed loading adapter rules from: " + adapterClassName + ": " + t.getMessage());
        }
        return out;
    }

    /**
     * TESTING / DEMO
     * Minimal demo entry-point:
     *  - Loads either the first query in ORIGINAL_SQL_FILE or a specific ID provided via args[0].
     *  - Parses the SQL string into a Calcite SqlNode and prints it.
     *
     * Try:
     *  mvn -q -f .\plan_equivalence\pom.xml -DskipTests exec:java \
     *      -Dexec.mainClass=com.ac.iisc.Calcite
     *
     * Or to parse a specific ID:
     *  mvn -q -f .\plan_equivalence\pom.xml -DskipTests exec:java \
     *      -Dexec.mainClass=com.ac.iisc.Calcite -Dexec.args="TPCH_Q11"
     */
    public static void main(String[] args) {
        try {
            String id = (args != null && args.length > 0) ? args[0] : null;
            String sql = loadQueryFromFile(ORIGINAL_SQL_FILE.toString(), id);
            if (sql == null) {
                System.err.println("No query found" + (id != null ? (" for ID: " + id) : " in file"));
                return;
            }

            SqlNode node = SQLtoSqlNode(sql); // Parse to Calcite AST (SqlNode)

            System.out.println("SQL:\n" + sql);
            System.out.println("\nParsed SqlNode (toString):\n" + node.toString());
        } catch (IOException e) {
            System.err.println("Failed to read SQL file: " + e.getMessage());
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            System.err.println("Parse error: " + e.getMessage());
        }
    }
}
