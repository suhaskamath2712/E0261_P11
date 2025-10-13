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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

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
     */
    /**
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
     */
    /**
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
     */
    /**
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
    public static SqlNode loadQuerySQL(String sql) throws SqlParseException {
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
                java.util.List<SqlNode> list = new java.util.ArrayList<>();
                flatten(call, SqlKind.AND, list);
                list.sort(java.util.Comparator.comparing(Object::toString));
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
                        java.util.List<SqlNode> ops = new java.util.ArrayList<>();
                        flatten(innerCall, innerCall.getKind(), ops);
                        java.util.List<SqlNode> nots = new java.util.ArrayList<>(ops.size());
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
                java.util.List<SqlNode> ops = new java.util.ArrayList<>();
                flatten(call, call.getKind(), ops);

                boolean isAnd = call.getKind() == SqlKind.AND;
                java.util.List<SqlNode> kept = new java.util.ArrayList<>();
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
    private static void flatten(SqlCall call, SqlKind kind, java.util.List<SqlNode> out) {
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
    private static SqlNode buildNary(org.apache.calcite.sql.SqlOperator op, java.util.List<SqlNode> ops) {
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

            SqlNode node = loadQuerySQL(sql); // Parse to Calcite AST (SqlNode)

            System.out.println("SQL:\n" + sql);
            System.out.println("\nParsed SqlNode (toString):\n" + node.toString());
        } catch (IOException e) {
            System.err.println("Failed to read SQL file: " + e.getMessage());
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            System.err.println("Parse error: " + e.getMessage());
        }
    }
}
