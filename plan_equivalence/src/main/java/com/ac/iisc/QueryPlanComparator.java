package com.ac.iisc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Demonstrates how to use Apache Calcite to compare two SQL queries for semantic equivalence
 * by normalizing and comparing their internal RelNode (Relational Expression) representations.
 *
 * This version connects to an external PostgreSQL database using the JDBC adapter.
 * The connection details must be configured below.
 */
public class QueryPlanComparator {

    // --- 1. PostgreSQL Connection Configuration (UPDATE THESE) ---
    private static final String PG_DRIVER = "org.postgresql.Driver";
    private static final String PG_URL = "jdbc:postgresql://localhost:5432/tpch"; 
    private static final String PG_USER = "postgres";
    private static final String PG_PASSWORD = "123";
    // Schema in PostgreSQL containing the TPC-H tables (e.g., 'public')
    private static final String PG_SCHEMA = "public";


    // --- 2. Setup Framework and Planner ---

    /**
     * Configures the Calcite Framework to use the PostgreSQL database as a schema source.
     */
    private static FrameworkConfig getFrameworkConfig() {
        try {
            // 1. Ensure the PostgreSQL Driver is loaded
            Class.forName(PG_DRIVER);

            // 2. Establish a Calcite-managed connection (needed to access the root schema)
            Properties info = new Properties();
            info.setProperty("lex", "JAVA");
            Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:", info);
            CalciteConnection unwrapCalciteConnection = calciteConnection.unwrap(CalciteConnection.class);

            // 3. Create a DataSource for PostgreSQL
            PGSimpleDataSource dataSource = new PGSimpleDataSource();
            dataSource.setUrl(PG_URL);
            dataSource.setUser(PG_USER);
            dataSource.setPassword(PG_PASSWORD);

            // 4. Wrap the PostgreSQL connection details into a Calcite schema (JdbcSchema)
            SchemaPlus rootSchema = unwrapCalciteConnection.getRootSchema();
            // Use the standard factory method with the DataSource
            JdbcSchema pgJdbcSchema = JdbcSchema.create(rootSchema, PG_SCHEMA, dataSource, PG_SCHEMA, null);
            rootSchema.add(PG_SCHEMA, pgJdbcSchema);

        // Parser config: fold unquoted identifiers to lower-case, case-insensitive
        // Use Lex.MYSQL for broad compatibility (works well with PostgreSQL catalogs)
        SqlParser.Config parserConfig = SqlParser.config()
            .withLex(Lex.MYSQL);


            // 5. Build the Calcite Framework configuration
            return Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema.getSubSchema(PG_SCHEMA)) // Use the Postgres schema as default
                    .parserConfig(parserConfig) // Use the PostgreSQL-aware parser config
                    .build();

        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to initialize Calcite framework with PostgreSQL connection. Check driver and connection details.", e);
        }
    }

    /**
     * Parses, validates, and optimizes a SQL query to produce a canonical RelNode.
     * @param planner The Calcite Planner instance.
     * @param sql The SQL query string.
     * @return The optimized RelNode.
     * @throws Exception if parsing or planning fails.
     */
    private static RelNode getOptimizedRelNode(Planner planner, String sql) throws Exception {
        System.out.println("Processing SQL: " + sql);

        // 1. Sanitize input: Calcite parser doesn't accept trailing ';'
        String sqlForParse = sql == null ? null : sql.trim();
        if (sqlForParse != null) {
            // Remove a single trailing semicolon if present
            while (sqlForParse.endsWith(";")) {
                sqlForParse = sqlForParse.substring(0, sqlForParse.length() - 1).trim();
            }
        }

        // 2. Parse the SQL string into an AST
        SqlNode sqlNode = planner.parse(sqlForParse);

    // 3. Validate the AST
        SqlNode validatedSqlNode = planner.validate(sqlNode);

    // 4. Convert the validated AST to RelNode (Logical Plan)
        RelNode logicalPlan = planner.rel(validatedSqlNode).rel;

    // 5. Optimize in phases to avoid oscillations and collapse redundant projections
        // Phase 1: basic simplification
        HepProgramBuilder p1 = new HepProgramBuilder();
        p1.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        p1.addRuleInstance(CoreRules.PROJECT_MERGE);
        p1.addRuleInstance(CoreRules.PROJECT_REMOVE);

        // Phase 2: normalize inner join structure safely
        HepProgramBuilder p2 = new HepProgramBuilder();
        p2.addMatchOrder(HepMatchOrder.TOP_DOWN);
        p2.addMatchLimit(200);
        p2.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        p2.addRuleInstance(CoreRules.JOIN_ASSOCIATE);
        p2.addRuleInstance(CoreRules.JOIN_COMMUTE);

        // Phase 3: collapse projects introduced by rewrites and re-simplify
        HepProgramBuilder p3 = new HepProgramBuilder();
        p3.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
        p3.addRuleInstance(CoreRules.PROJECT_MERGE);
        p3.addRuleInstance(CoreRules.PROJECT_REMOVE);
        p3.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);

        HepProgramBuilder pb = new HepProgramBuilder();
        pb.addSubprogram(p1.build());
        pb.addSubprogram(p2.build());
        pb.addSubprogram(p3.build());
        HepProgram hepProgram = pb.build();
        HepPlanner hepPlanner = new HepPlanner(hepProgram);

        hepPlanner.setRoot(logicalPlan);
        RelNode optimizedPlan = hepPlanner.findBestExp();

        System.out.println("  -> Optimized RelNode (Logical Plan):");
        System.out.println(RelOptUtil.toString(optimizedPlan));
        return optimizedPlan;
    }

    /**
     * Compares two SQL queries for semantic equivalence.
     * @param sql1 First query.
     * @param sql2 Second query.
     * @return true if the optimized RelNode trees are equal, false otherwise.
     */
    public static boolean compareQueries(String sql1, String sql2) {
        FrameworkConfig config = getFrameworkConfig();
        Planner planner = Frameworks.getPlanner(config);

        try {
            // Get the optimized RelNode for the first query
            RelNode rel1 = getOptimizedRelNode(planner, sql1);
            // Reset the planner for the second query (Calcite's Planner is stateful)
            planner.close();
            planner = Frameworks.getPlanner(config);

            // Get the optimized RelNode for the second query
            RelNode rel2 = getOptimizedRelNode(planner, sql2);

            // Fast path: structural digests
            String d1 = RelOptUtil.toString(rel1, SqlExplainLevel.DIGEST_ATTRIBUTES);
            String d2 = RelOptUtil.toString(rel2, SqlExplainLevel.DIGEST_ATTRIBUTES);

            if (d1.equals(d2)) return true;
            // Fallback 1: neutralize input indexes
            String nd1 = normalizeDigest(d1);
            String nd2 = normalizeDigest(d2);

            if (nd1.equals(nd2)) return true;
            // Fallback 2: canonical digest that treats inner-join children as unordered
            String c1 = canonicalDigest(rel1);
            String c2 = canonicalDigest(rel2);

            //System.out.println("c1: " + c1);
            //System.out.println("c2: " + c2);
            return c1.equals(c2);

        } catch (Exception e)
        {
            e.printStackTrace();
            // Swallow detailed error output to satisfy lint rules; return non-equivalence on error
            return false;
        } finally {
            planner.close();
        }
    }

    /**
     * Compares two SQL queries for semantic equivalence.
     * @param sql1 First query.
     * @param sql2 Second query.
     * @param transformations Array of transformation names to apply before comparison.
     * @return true if the optimized RelNode trees are equal, false otherwise.
     */
    public static boolean compareQueries(String sql1, String sql2, List<String> transformations) {
        FrameworkConfig config = getFrameworkConfig();
        Planner planner = Frameworks.getPlanner(config);

        try {
            // Get the optimized RelNode for the first query
            RelNode rel1 = getOptimizedRelNode(planner, sql1);
            RelNode transformedRel1 = applyTransformations(rel1, transformations);
            System.out.println("After applying transformations, RelNode 1:");
            System.out.println(RelOptUtil.toString(transformedRel1));
            // Reset the planner for the second query (Calcite's Planner is stateful)
            planner.close();
            planner = Frameworks.getPlanner(config);

            // Get the optimized RelNode for the second query
            RelNode rel2 = getOptimizedRelNode(planner, sql2);

            // Fast path: structural digests
            String d1 = RelOptUtil.toString(transformedRel1, SqlExplainLevel.DIGEST_ATTRIBUTES);
            String d2 = RelOptUtil.toString(rel2, SqlExplainLevel.DIGEST_ATTRIBUTES);

            if (d1.equals(d2)) return true;
            // Fallback 1: neutralize input indexes
            String nd1 = normalizeDigest(d1);
            String nd2 = normalizeDigest(d2);

            if (nd1.equals(nd2)) return true;
            // Fallback 2: canonical digest that treats inner-join children as unordered
            String c1 = canonicalDigest(rel1);
            String c2 = canonicalDigest(rel2);

            //System.out.println("c1: " + c1);
            //System.out.println("c2: " + c2);
            return c1.equals(c2);

        } catch (Exception e)
        {
            e.printStackTrace();
            // Swallow detailed error output to satisfy lint rules; return non-equivalence on error
            return false;
        } finally {
            planner.close();
        }
    }

    private static String normalizeDigest(String digest) {
        if (digest == null) return null;
        // Replace input refs like $0, $12 with a placeholder to make join-child order less significant
        String s = digest.replaceAll("\\$\\d+", "\\$x");
        // Collapse multiple spaces to onxe for stability
        s = s.replaceAll("[ ]+", " ");
        return s.trim();
    }

    // Build a canonical digest for a plan where inner-join children are treated as an unordered set
    private static String canonicalDigest(RelNode rel) {
        if (rel instanceof LogicalProject p) {
            StringBuilder sb = new StringBuilder();
            sb.append("Project[");
            for (int i = 0; i < p.getProjects().size(); i++) {
                RexNode rex = p.getProjects().get(i);
                if (i > 0) sb.append(",");
                sb.append(normalizeDigest(rex.toString()));
            }
            sb.append("]->");
            sb.append(canonicalDigest(p.getInput()));
            return sb.toString();
        }
        if (rel instanceof LogicalFilter f) {
            return "Filter(" + normalizeDigest(f.getCondition().toString()) + ")->" + canonicalDigest(f.getInput());
        }
        if (rel instanceof LogicalJoin j) {
            String cond = normalizeDigest(j.getCondition().toString());
            String left = canonicalDigest(j.getLeft());
            String right = canonicalDigest(j.getRight());
            String a = left;
            String b = right;
            // For inner joins, make children order-insensitive by sorting their digests
            if (j.getJoinType() == JoinRelType.INNER) {
                if (a.compareTo(b) > 0) {
                    String t = a; a = b; b = t;
                }
            }
            return "Join(" + j.getJoinType() + "," + cond + "){" + a + "|" + b + "}";
        }
        if (rel instanceof TableScan) {
            TableScan ts = (TableScan) rel;
            return "Scan(" + String.join(".", ts.getTable().getQualifiedName()) + ")";
        }
        // Default: normalized string of this node with placeholders, plus canonical children
        String head = normalizeDigest(rel.getRelTypeName());
        StringBuilder sb = new StringBuilder(head);
        sb.append("[");
        boolean first = true;
        for (RelNode in : rel.getInputs()) {
            if (!first) sb.append("|");
            sb.append(canonicalDigest(in));
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }

    private static RelNode applyTransformations(RelNode rel, List<String> transformations)
    {
        HepProgramBuilder pb = new HepProgramBuilder();
        for (String transform : transformations)
        {
            switch (transform.toLowerCase())
            {
                //Projection Rules
                case "projectmergerule" -> pb.addRuleInstance(CoreRules.PROJECT_MERGE);
                case "projectremoverule" -> pb.addRuleInstance(CoreRules.PROJECT_REMOVE);
                case "projectjointransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
                case "projectfiltertransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE);
                case "projectsetoptransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE);
                case "projecttablescanrule" -> pb.addRuleInstance(CoreRules.PROJECT_TABLE_SCAN);
                
                //Filter Rules
                case "filtermergerule" -> pb.addRuleInstance(CoreRules.FILTER_MERGE);
                case "filterprojecttransposerule" -> pb.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
                case "filterjoinrule" -> pb.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
                case "filteraggregatetransposerule" -> pb.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
                case "filterwindowtransposerule" -> pb.addRuleInstance(CoreRules.FILTER_WINDOW_TRANSPOSE);
                
                //Join Rules
                case "joincommuterule" -> pb.addRuleInstance(CoreRules.JOIN_COMMUTE);
                case "joinassociaterule" -> pb.addRuleInstance(CoreRules.JOIN_ASSOCIATE);
                case "joinpushexpressionsrule" -> pb.addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS);
                case "joinconditionpushrule" -> pb.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
                
                //Aggregate Rules
                case "aggregateprojectpullupconstantsrule" -> pb.addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
                case "aggregateremoverule" -> pb.addRuleInstance(CoreRules.AGGREGATE_REMOVE);
                case "aggregatejointransposerule" -> pb.addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE);
                case "aggregateuniontransposerule" -> pb.addRuleInstance(CoreRules.AGGREGATE_UNION_TRANSPOSE);
                case "aggregateprojectmergerule" -> pb.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
                case "aggregatecasetofilterrule" -> pb.addRuleInstance(CoreRules.AGGREGATE_CASE_TO_FILTER);

                //Sort & limit rules
                case "sortremoverule" -> pb.addRuleInstance(CoreRules.SORT_REMOVE);
                case "sortuniontransposerule" -> pb.addRuleInstance(CoreRules.SORT_UNION_TRANSPOSE);
                case "sortprojecttransposerule" -> pb.addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE);
                case "sortjointransposerule" -> pb.addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE);

                //Set operation rules
                case "unionmergerule" -> pb.addRuleInstance(CoreRules.UNION_MERGE);
                case "unionpullupconstantsrule" -> pb.addRuleInstance(CoreRules.UNION_PULL_UP_CONSTANTS);
                case "intersecttodistinctrule" -> pb.addRuleInstance(CoreRules.INTERSECT_TO_DISTINCT);
                case "minustodistinctrule" -> pb.addRuleInstance(CoreRules.MINUS_TO_DISTINCT);

                //Window rules
                case "projectwindowtransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_WINDOW_TRANSPOSE);

                default -> System.out.println("Unknown transformation: " + transform);
            }
        }
        HepPlanner planner = new HepPlanner(pb.build());
        planner.setRoot(rel);
        RelNode newRel = planner.findBestExp();
        return newRel;
    }

    public static void main(String[] args) {
        // --- Examples using TPC-H tables (assuming 'nation' and 'region' exist) ---

        System.out.println("--- Scenario 1: Semantically Equivalent Queries (Different Syntax) ---");
        // Query A: Standard join order (NATION JOIN REGION) and standard filter
        String sqlA = "(SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'GERMANY' order by s_suppkey desc, s_name limit 12)UNION ALL (SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '1-URGENT' order by c_custkey, c_name desc limit 10);";
        // Query B: Same semantics with reversed join inputs; normalization should make them equivalent
        String sqlB = """
                      (SELECT C.c_name, C.c_custkey
                        FROM customer AS C, orders AS O
                        WHERE O.o_custkey = C.c_custkey AND O.o_orderpriority = '1-URGENT'
                        ORDER BY C.c_custkey, C.c_name DESC
                        LIMIT 10)
                        UNION ALL
                        (SELECT S.s_name, S.s_suppkey
                        FROM supplier AS S, nation AS N
                        WHERE S.s_nationkey = N.n_nationkey AND N.n_name = 'GERMANY'
                        ORDER BY S.s_suppkey DESC, S.s_name
                        LIMIT 12);""";

        List<String> transformations = List.of("unionmergerule");
        boolean result1 = compareQueries(sqlA, sqlB, transformations);
        System.out.println("\nComparison Result (A vs B): " + result1);
        System.out.println("------------------------------------------------------------------\n");
    }
}
