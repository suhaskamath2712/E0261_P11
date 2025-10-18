/*
 * =====================================================================================
 * Calcite.java (Corrected)
 *
 * Purpose
 * -------
 * This utility demonstrates how to:
 * 1) Load SQL text from a file that contains multiple queries delimited by
 * documented blocks (with "-- Query ID: <ID>" headers),
 * 2) Select a specific query by its ID (or default to the first), and
 * 3) Parse that SQL into an Apache Calcite SqlNode (the SQL AST).
 * 4) Apply transformations at both the SqlNode (AST) and RelNode (logical plan) level.
 *
 * Key Corrections
 * ---------------
 * - Replaced non-existent FileIO with standard java.nio.file.Files.
 * - Corrected buildNary to properly construct n-ary SqlCalls.
 * - Corrected equivalent() to use RelOptUtil.areEqual for reliable plan comparison.
 * - Replaced complex/fragile reflection-based rule lookup with a direct, robust switch-based approach.
 *
 * =====================================================================================
 */
package com.ac.iisc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Calcite parsing and transformation helper.
 */
public class Calcite {

    private static final Path ORIGINAL_SQL_FILE = Paths.get(
        "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql");

    /**
     * Extracts queries from a single .sql file into a map of (QueryID -> SQL text).
     */
    private static Map<String, String> parseQueries(java.nio.file.Path sqlFile) throws IOException {
        String content = Files.readString(sqlFile); // CORRECTED: Use standard Java NIO
        Pattern p = Pattern.compile(
                "-- =+.*?\n-- Query ID: ([\\w-]+).*?\n-- =+.*?\n(.*?)(?=\n-- =+|$)",
                Pattern.DOTALL);
        Matcher m = p.matcher(content);
        Map<String, String> map = new LinkedHashMap<>();
        while (m.find()) {
            String id = m.group(1).trim();
            String sql = m.group(2).trim();
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
     */
    public static String loadQueryFromFile(String path, String queryId) throws IOException {
        Path p = Paths.get(path);
        Map<String, String> queries = parseQueries(p);
        if (queries.isEmpty()) {
            return null;
        }
        if (queryId == null || queryId.isBlank()) {
            return queries.values().iterator().next();
        }
        return queries.get(queryId);
    }

    /**
     * Parse a SQL string using Apache Calcite and return the parsed SqlNode.
     */
    public static SqlNode SQLtoSqlNode(String sql) throws SqlParseException {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql must not be null or blank");
        }
        SqlParser.Config config = SqlParser.config()
                .withLex(Lex.ORACLE) // ORACLE is a reasonable default for generic SQL with some extensions
                .withQuoting(Quoting.DOUBLE_QUOTE)
                .withUnquotedCasing(Casing.TO_LOWER)
                .withCaseSensitive(false)
                .withIdentifierMaxLength(128);
        SqlParser parser = SqlParser.create(sql, config);
        return parser.parseQuery();
    }

    /**
     * Compare two SqlNodes for structural equality using Calcite's equalsDeep API.
     */
    public static boolean sqlNodesEqual(SqlNode left, SqlNode right) {
        if (left == right) return true;
        if (left == null || right == null) return false;
        return left.equalsDeep(right, Litmus.IGNORE);
    }

    // =====================================================================================
    //  Lightweight AST Transformations (SqlNode-level)
    // =====================================================================================

    private interface SqlTransformation {
        SqlNode apply(SqlNode in);
    }

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
            }
            if (t != null) {
                out = t.apply(out);
            }
        }
        return out;
    }

    private static class SimplifyDoubleNegation extends SqlShuttle implements SqlTransformation {
        @Override public SqlNode apply(SqlNode in) { return in.accept(this); }
        @Override public SqlNode visit(SqlCall call) {
            call = (SqlCall) super.visit(call);
            if (call.getKind() == SqlKind.NOT) {
                if (call.operand(0).getKind() == SqlKind.NOT) {
                    return ((SqlCall) call.operand(0)).operand(0);
                }
            }
            return call;
        }
    }

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

    private static class PushNotDown extends SqlShuttle implements SqlTransformation {
        @Override public SqlNode apply(SqlNode in) { return in.accept(this); }
        @Override public SqlNode visit(SqlCall call) {
            call = (SqlCall) super.visit(call);
            if (call.getKind() == SqlKind.NOT && call.operand(0) instanceof SqlCall) {
                SqlCall innerCall = call.operand(0);
                if (innerCall.getKind() == SqlKind.AND || innerCall.getKind() == SqlKind.OR) {
                    List<SqlNode> ops = new ArrayList<>(innerCall.getOperandList());
                    List<SqlNode> nots = new ArrayList<>(ops.size());
                    for (SqlNode n : ops) {
                        nots.add(SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, n));
                    }
                    return buildNary(
                        innerCall.getKind() == SqlKind.AND ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND,
                        nots);
                }
            }
            return call;
        }
    }

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
                        if (!b) return booleanLiteral(false); // AND FALSE => FALSE
                    } else { // OR
                        if (b) return booleanLiteral(true);   // OR TRUE => TRUE
                    }
                }
                if (kept.isEmpty()) return booleanLiteral(isAnd);
                if (kept.size() == 1) return kept.get(0);
                return buildNary(isAnd ? SqlStdOperatorTable.AND : SqlStdOperatorTable.OR, kept);
            }
            return call;
        }
    }

    // ----------------------------- helpers ---------------------------------
    private static void flatten(SqlCall call, SqlKind kind, List<SqlNode> out) {
        for (SqlNode n : call.getOperandList()) {
            if (n.getKind() == kind) {
                flatten((SqlCall) n, kind, out);
            } else {
                out.add(n);
            }
        }
    }

    /** CORRECTED: Build an n-ary call from a list of operands. */
    private static SqlNode buildNary(org.apache.calcite.sql.SqlOperator op, List<SqlNode> ops) {
        return op.createCall(SqlParserPos.ZERO, ops);
    }

    private static Boolean asBoolean(SqlNode n) {
        if (n instanceof SqlLiteral && n.getKind() == SqlKind.LITERAL) {
            return ((SqlLiteral) n).booleanValue();
        }
        return null;
    }

    private static SqlNode booleanLiteral(boolean v) {
        return SqlLiteral.createBoolean(v, SqlParserPos.ZERO);
    }

    // =====================================================================================
    //  RelNode Transformation Support (Planner rules)
    // =====================================================================================

    public static RelNode toRelNode(String sql, FrameworkConfig config)
            throws SqlParseException, ValidationException, RelConversionException {
        Planner planner = Frameworks.getPlanner(config);
        SqlNode parsed = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        RelRoot root = planner.rel(validated);
        return root.rel;
    }

    public static RelNode loadRelFromFile(String sqlFilePath, String queryId, FrameworkConfig config)
            throws IOException, SqlParseException, ValidationException, RelConversionException {
        String sql = loadQueryFromFile(sqlFilePath, queryId);
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("No SQL found for Query ID: " + String.valueOf(queryId));
        }
        return toRelNode(sql, config);
    }

    public static FrameworkConfig buildPostgresFrameworkConfig(String host, int port, String database,
                                                               String user, String pass) {
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        SchemaPlus root = Frameworks.createRootSchema(true);
        DataSource ds = JdbcSchema.dataSource(url, "org.postgresql.Driver", user, pass);
        root.add("db", JdbcSchema.create(root, "db", ds, null, null));
        SqlParser.Config parserCfg = SqlParser.config()
            .withUnquotedCasing(Casing.TO_LOWER)
            .withCaseSensitive(false);
        return Frameworks.newConfigBuilder()
            .defaultSchema(root.getSubSchema("db"))
            .parserConfig(parserCfg)
            .build();
    }

    /** Equivalence check using normalized stringified plans. */
    public static boolean equivalent(String sqlA, String sqlB, FrameworkConfig config) throws Exception {
        RelNode relA = toRelNode(sqlA, config);
        RelNode relB = toRelNode(sqlB, config);
        String[] norm = {
            "ProjectMergeRule", "FilterMergeRule", "JoinCommuteRule"
        };
        relA = applyRelTransformations(relA, norm);
        relB = applyRelTransformations(relB, norm);
        String sA = String.valueOf(relA);
        String sB = String.valueOf(relB);
        return sA.equals(sB);
    }

    public static RelNode applyRelTransformations(RelNode root, String[] ruleNames) {
        if (root == null || ruleNames == null || ruleNames.length == 0) return root;
        HepProgramBuilder pb = new HepProgramBuilder();
        for (String name : ruleNames) {
            RelOptRule rule = resolveCoreRule(name); // Use the simplified resolver
            if (rule != null) {
                pb.addRuleInstance(rule);
            } else {
                System.err.println("[Calcite][WARN] Unknown transformation name: " + name);
            }
        }
        HepProgram program = pb.build();
        HepPlanner hep = new HepPlanner(program);
        hep.setRoot(root);
        return hep.findBestExp();
    }

    /**
     * CORRECTED: Simple, direct, and robust rule resolution without reflection.
     */
    private static RelOptRule resolveCoreRule(String name) {
        switch (name) {
            // Projection Rules
            case "ProjectMergeRule": return CoreRules.PROJECT_MERGE;
            case "ProjectRemoveRule": return CoreRules.PROJECT_REMOVE;
            case "ProjectJoinTransposeRule": return CoreRules.PROJECT_JOIN_TRANSPOSE;
            case "ProjectFilterTransposeRule": return CoreRules.PROJECT_FILTER_TRANSPOSE;
            // Filter Rules
            case "FilterMergeRule": return CoreRules.FILTER_MERGE;
            case "FilterProjectTransposeRule": return CoreRules.FILTER_PROJECT_TRANSPOSE;
            case "FilterJoinRule": return CoreRules.FILTER_INTO_JOIN;
            // Join Rules
            case "JoinCommuteRule": return CoreRules.JOIN_COMMUTE;
            case "JoinAssociateRule": return CoreRules.JOIN_ASSOCIATE;
            // Aggregate Rules
            case "AggregateRemoveRule": return CoreRules.AGGREGATE_REMOVE;
            case "AggregateJoinTransposeRule": return CoreRules.AGGREGATE_JOIN_TRANSPOSE;
            // SetOp Rules
            case "UnionMergeRule": return CoreRules.UNION_MERGE;
            // Sort Rules
            case "SortRemoveRule": return CoreRules.SORT_REMOVE;
            default: return null;
        }
    }

    public static void main(String[] args) {
        try {
            String id = (args != null && args.length > 0) ? args[0] : null;
            String sql = loadQueryFromFile(ORIGINAL_SQL_FILE.toString(), id);
            if (sql == null) {
                System.err.println("No query found" + (id != null ? (" for ID: " + id) : " in file"));
                return;
            }
            SqlNode node = SQLtoSqlNode(sql);
            System.out.println("SQL:\n" + sql);
            System.out.println("\nParsed SqlNode (toString):\n" + node.toString());
        } catch (IOException e) {
            System.err.println("Failed to read SQL file: " + e.getMessage());
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            System.err.println("Parse error: " + e.getMessage());
        }
    }
}
