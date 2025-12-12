package com.ac.iisc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Miscellaneous Calcite utility helpers that are not directly tied to
 * query-equivalence comparison logic.
 */
public final class CalciteUtil {

    // --- 1. PostgreSQL Connection Configuration (UPDATE THESE) ---
    // Driver and JDBC settings for connecting Calcite's JdbcSchema to PostgreSQL.
    // These are used only to expose the Postgres catalog (tables/columns) to Calcite's planner.
    private static final String PG_DRIVER = "org.postgresql.Driver";
    private static final String PG_URL = FileIO.getPgUrl(); 
    private static final String PG_USER = FileIO.getPgUser();
    private static final String PG_PASSWORD = FileIO.getPgPassword();
    // Schema in PostgreSQL containing the TPC-H tables (e.g., 'public')
    private static final String PG_SCHEMA = FileIO.getPgSchema();

    /**
     * Build a Calcite {@link FrameworkConfig} that exposes a PostgreSQL schema via JDBC.
     *
     * This is factored out of {@link Calcite} so other helpers can obtain a
     * framework without depending on Calcite's comparison logic.
     */
    public static FrameworkConfig getFrameworkConfig() {
        try {
            // 1. Ensure the PostgreSQL Driver is loaded so DataSource/DriverManager can find it
            Class.forName(PG_DRIVER);

            // 2. Establish a Calcite-managed connection (used to get the Calcite root schema container)
            java.util.Properties info = new java.util.Properties();

            // parsing behavior of the Calcite connection itself; use JAVA
            // here (Calcite 1.36 does not define a POSTGRESQL lex enum).
            info.setProperty("lex", "JAVA");
            // Expose extended SQL function libraries so Calcite understands
            // dialect-specific operators such as LEAST / GREATEST that appear
            // in our TPC-H style queries. We include STANDARD plus
            // PostgreSQL and MySQL libraries since LEAST/GREATEST are
            // provided via these dialect packs.
            info.setProperty("fun", "standard,postgresql,mysql");

            // open Calcite
            Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:", info);

            // get Calcite API
            CalciteConnection unwrapCalciteConnection = calciteConnection.unwrap(CalciteConnection.class);

            // 3. Create a DataSource for PostgreSQL (Calcite will use this for metadata)
            PGSimpleDataSource dataSource = new PGSimpleDataSource();
            dataSource.setUrl(PG_URL);
            dataSource.setUser(PG_USER);
            dataSource.setPassword(PG_PASSWORD);

            // 4. Wrap the PostgreSQL connection details into a Calcite schema (JdbcSchema)
            SchemaPlus rootSchema = unwrapCalciteConnection.getRootSchema();

            // Use the standard factory method with the DataSource
            JdbcSchema pgJdbcSchema = JdbcSchema.create(rootSchema, PG_SCHEMA, dataSource, PG_SCHEMA, null);
            rootSchema.add(PG_SCHEMA, pgJdbcSchema);

            // Parser config: use MySQL lex, which is close to PostgreSQL for
            // identifier folding and quoting in this workload.
            SqlParser.Config parserConfig = SqlParser.config().withLex(Lex.MYSQL);

            // Operator table: include standard SQL operators plus PostgreSQL- and
            // MySQL-specific functions such as LEAST/GREATEST so that validation
            // matches PostgreSQL behavior more closely.
            SqlOperatorTable operatorTable = SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(SqlLibrary.STANDARD, SqlLibrary.POSTGRESQL, SqlLibrary.MYSQL);

            // 5. Build the Calcite Framework configuration
            return Frameworks.newConfigBuilder()
                // Use the Postgres schema as default
                .defaultSchema(rootSchema.getSubSchema(PG_SCHEMA))
                // Use the PostgreSQL-aware parser config
                .parserConfig(parserConfig)
                // Enable PostgreSQL function/operator library (e.g., LEAST, GREATEST)
                .operatorTable(operatorTable)
                // Allow more permissive SQL semantics (e.g., GROUP BY alias such as o_year)
                .sqlValidatorConfig(SqlValidator.Config.DEFAULT.withConformance(SqlConformanceEnum.BABEL))
                .build();

        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to initialize Calcite framework with PostgreSQL connection. Check driver and connection details.", e);
        }
    }

    /**
     * Rewrite dialect-specific LEAST/GREATEST function calls into standard SQL
     * CASE expressions. This is used as a defensive pre-processing step before
     * handing SQL to Calcite's parser so that queries continue to work even if
     * a particular Calcite version does not expose these functions via the
     * configured operator table.
     */
    public static String rewriteLeastGreatest(String sql) {
        if (sql == null) {
            return null;
        }
        String rewritten = rewriteTwoArgExtremaFunction(sql, "LEAST", "<=");
        rewritten = rewriteTwoArgExtremaFunction(rewritten, "GREATEST", ">=");
        return rewritten;
    }

    /**
     * Generic helper that rewrites calls of the form
     *   FUNCTION(arg1, arg2)
     * into
     *   (CASE WHEN arg1 comparator arg2 THEN arg1 ELSE arg2 END)
     * where {@code comparator} is "<=" for LEAST and ">=" for GREATEST.
     *
     * The implementation is conservative: it only rewrites when it can find a
     * well-formed parenthesized argument list that splits cleanly into exactly
     * two top-level arguments. Otherwise it leaves the original text intact.
     */
    private static String rewriteTwoArgExtremaFunction(String sql, String functionName, String comparator) {
        String upperSql = sql.toUpperCase();
        String fnUpper = functionName.toUpperCase();

        StringBuilder out = new StringBuilder(sql.length());
        int idx = 0;

        while (true) {
            int pos = upperSql.indexOf(fnUpper, idx);
            if (pos < 0) {
                // no more occurrences
                out.append(sql.substring(idx));
                break;
            }

            // Ensure we matched a standalone function name, not a suffix of
            // a longer identifier.
            if (pos > 0) {
                char prev = upperSql.charAt(pos - 1);
                if (Character.isLetterOrDigit(prev) || prev == '_') {
                    out.append(sql, idx, pos + fnUpper.length());
                    idx = pos + fnUpper.length();
                    continue;
                }
            }

            int parenStart = pos + fnUpper.length();
            // Skip whitespace between the function name and '('
            while (parenStart < sql.length() && Character.isWhitespace(sql.charAt(parenStart))) {
                parenStart++;
            }
            if (parenStart >= sql.length() || sql.charAt(parenStart) != '(') {
                // Not a function call we understand; copy text and continue.
                out.append(sql, idx, pos + fnUpper.length());
                idx = pos + fnUpper.length();
                continue;
            }

            // Find the matching closing parenthesis, tracking nested parens to
            // be robust against arguments like f(LEAST(a, b), c).
            int level = 0;
            int i = parenStart;
            for (; i < sql.length(); i++) {
                char c = sql.charAt(i);
                if (c == '(') {
                    level++;
                } else if (c == ')') {
                    level--;
                    if (level == 0) {
                        break;
                    }
                }
            }
            if (level != 0) {
                // Unbalanced parentheses; give up and copy the rest verbatim.
                out.append(sql.substring(idx));
                break;
            }

            String argsRegion = sql.substring(parenStart + 1, i);
            List<String> args = splitTopLevelArgs(argsRegion);
            if (args.size() != 2) {
                // Only handle the common 2-argument case; copy text as-is.
                out.append(sql, idx, i + 1);
                idx = i + 1;
                continue;
            }

            String a1 = args.get(0).trim();
            String a2 = args.get(1).trim();
            String caseExpr = "(CASE WHEN " + a1 + " " + comparator + " " + a2
                    + " THEN " + a1 + " ELSE " + a2 + " END)";

            // Append everything before the function name, then our rewritten CASE.
            out.append(sql, idx, pos);
            out.append(caseExpr);
            idx = i + 1;
        }

        return out.toString();
    }

    /**
     * Split a comma-separated argument list into top-level arguments, ignoring
     * commas that occur inside nested parentheses.
     */
    private static List<String> splitTopLevelArgs(String region) {
        List<String> parts = new ArrayList<>();
        int level = 0;
        int start = 0;
        for (int i = 0; i < region.length(); i++) {
            char c = region.charAt(i);
            if (c == '(') {
                level++;
            } else if (c == ')') {
                level--;
            } else if (c == ',' && level == 0) {
                parts.add(region.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(region.substring(start));
        return parts;
    }

    /**
     * Construct a {@link RelNode} from a JSON query plan.
     *
     * <p>This implementation is designed for the cleaned PostgreSQL EXPLAIN
     * (FORMAT JSON) plans produced by {@link GetQueryPlans}. It performs a
     * <em>structural</em> mapping only:</p>
     * <ul>
     *   <li>Scan-like nodes (e.g., "Seq Scan", "Index Scan") become table
     *       scans on the referenced relation.</li>
     *   <li>Join-like nodes (e.g., "Hash Join", "Merge Join", "Nested Loop")
     *       become logical joins with the appropriate {@link JoinRelType}, but
     *       without re-creating join conditions (joined as TRUE).</li>
     *   <li>Other nodes with a single child (e.g., "Sort", "Aggregate") are
     *       structurally collapsed and mapped to their child plan.</li>
     * </ul>
     *
     * <p>The resulting RelNode is suitable for coarse-grained structural
     * comparison (e.g., which tables are joined and in what tree shape), but it
     * intentionally ignores predicates, projections, and aggregate details.</p>
     *
     * @param jsonPlan JSON string representing a cleaned PostgreSQL plan.
     * @return a RelNode approximating the logical structure of the plan
     * @throws IllegalArgumentException if the JSON text is syntactically invalid
     */
    public static RelNode jsonPlanToRelNode(String jsonPlan) {
        if (jsonPlan == null || jsonPlan.isBlank()) {
            throw new IllegalArgumentException("jsonPlan must be non-null and non-blank");
        }

        JSONObject root = new JSONObject(jsonPlan);
        FrameworkConfig config = getFrameworkConfig();
        return buildRelFromJsonPlan(root, config);
    }

    /**
     * Recursively build a RelNode tree from a PostgreSQL-style JSON plan.
     * This focuses on Scan and Join structure; other node types are
     * collapsed onto their primary child.
     */
    private static RelNode buildRelFromJsonPlan(JSONObject node, FrameworkConfig config) {
        if (node == null) return null;

        String nodeType = node.optString("Node Type", "");
        String normalizedType = nodeType == null ? "" : nodeType.toLowerCase(Locale.ROOT);

        // Child plans (e.g., for joins, sorts, aggregates)
        JSONArray children = node.optJSONArray("Plans");

        // 1) Scan-like nodes → table scan
        if (normalizedType.contains("scan")) {
            String table = node.optString("Relation Name", null);
            if (table == null || table.isBlank()) {
                throw new IllegalArgumentException("JSON plan scan node is missing 'Relation Name': " + node.toString());
            }
            RelBuilder b = RelBuilder.create(config);
            // Default schema of the FrameworkConfig is our Postgres schema,
            // so a single-name scan is sufficient.
            b.scan(table);
            return b.build();
        }

        // 2) Join-like nodes → logical join of recursively built children
        if (normalizedType.contains("join")) {
            if (children == null || children.length() < 2) {
                throw new IllegalArgumentException("Join node must have at least two child plans: " + node.toString());
            }

            JSONObject leftJson = children.getJSONObject(0);
            JSONObject rightJson = children.getJSONObject(1);

            RelNode left = buildRelFromJsonPlan(leftJson, config);
            RelNode right = buildRelFromJsonPlan(rightJson, config);

            RelBuilder b = RelBuilder.create(config);
            b.push(left).push(right);

            String joinTypeStr = node.optString("Join Type", "Inner");
            JoinRelType joinType;
            switch (joinTypeStr == null ? "inner" : joinTypeStr.toLowerCase(Locale.ROOT)) {
                case "left", "left join", "left outer" -> joinType = JoinRelType.LEFT;
                case "right", "right join", "right outer" -> joinType = JoinRelType.RIGHT;
                case "full", "full join", "full outer" -> joinType = JoinRelType.FULL;
                case "semi" -> joinType = JoinRelType.SEMI;
                case "anti" -> joinType = JoinRelType.ANTI;
                default -> joinType = JoinRelType.INNER;
            }

            // Structural only: join on TRUE, we don’t reconstruct conditions.
            b.join(joinType);
            return b.build();
        }

        // 3) Unary structural nodes (Sort, Aggregate, Limit, etc.) → collapse to child plan
        if (children != null && children.length() >= 1) {
            JSONObject child = children.getJSONObject(0);
            return buildRelFromJsonPlan(child, config);
        }

        // 4) Fallback: unsupported leaf
        throw new IllegalArgumentException(
            "Unsupported or leaf JSON plan node without scan info: " + node.toString());
    }

    /**
     * Print the RelTreeNode tree for a given SQL query (for debugging/visualization).
     */
    public static void printRelTrees(String sql1, String sql2) {
        try {
            FrameworkConfig config = getFrameworkConfig();
            Planner planner = Frameworks.getPlanner(config);
            RelNode rel = Calcite.getOptimizedRelNode(planner, sql1);
            System.out.println("RelTreeNode tree1: \n" + Calcite.buildRelTree(rel).toString());
        } catch (Exception e) {
            System.err.println("[CalciteUtil.printRelTrees] Planning error: " + e.getMessage());
        }
    }
}
