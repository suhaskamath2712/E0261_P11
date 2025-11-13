package com.ac.iisc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Calcite parsing and transformation helper.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Build a Calcite {@link FrameworkConfig} backed by a PostgreSQL schema (via JDBC).</li>
 *   <li>Parse/validate SQL to {@link RelNode} and optionally optimize it using HepPlanner phases.</li>
 *   <li>Provide utilities to compare two queries for structural equivalence using digests
 *       and a canonical formatter that is insensitive to inner-join child order, commutative/symmetric expressions, and harmless CASTs.</li>
 *   <li>Apply specific CoreRules by name to a {@link RelNode} for controlled normalization.</li>
 * </ul>
 *
 * <p>Notes:</p>
 * <ul>
 *   <li>Unquoted identifiers are folded to lower-case to align with PostgreSQL catalogs.</li>
 *   <li>Trailing semicolons are stripped before parsing for robustness.</li>
 *   <li>Canonical digest normalizes inner-join child ordering, commutative/symmetric expressions, and ignores harmless CASTs; projection order is preserved.</li>
 *   <li>Sort/Limit nodes are normalized to ignore sort keys but preserve row limits.</li>
 * </ul>
 */
public class Calcite {

    // --- 1. PostgreSQL Connection Configuration (UPDATE THESE) ---
    // Driver and JDBC settings for connecting Calcite's JdbcSchema to PostgreSQL.
    // These are used only to expose the Postgres catalog (tables/columns) to Calcite's planner.
    private static final String PG_DRIVER = "org.postgresql.Driver";
    private static final String PG_URL = "jdbc:postgresql://localhost:5432/tpch"; 
    private static final String PG_USER = "postgres";
    private static final String PG_PASSWORD = "123";
    // Schema in PostgreSQL containing the TPC-H tables (e.g., 'public')
    private static final String PG_SCHEMA = "public";


    // --- 2. Setup Framework and Planner ---

    /**
     * Build a Calcite {@link FrameworkConfig} that exposes a PostgreSQL schema via JDBC.
     *
     * <p>Implementation details:</p>
     * <ul>
     *   <li>Creates an in-memory Calcite connection to obtain the root schema container.</li>
     *   <li>Wraps a {@link PGSimpleDataSource} in a Calcite {@link JdbcSchema} named {@code PG_SCHEMA}.</li>
     *   <li>Configures the parser with {@link Lex#MYSQL} so unquoted identifiers fold to
     *       lower-case, matching PostgreSQL default behavior and easing name resolution.</li>
     * </ul>
     *
     * @return FrameworkConfig for Calcite planners using the PostgreSQL schema
     * @throws RuntimeException if the driver or connection fails
     */
    public static FrameworkConfig getFrameworkConfig() {
        try {
            // 1. Ensure the PostgreSQL Driver is loaded so DataSource/DriverManager can find it
            Class.forName(PG_DRIVER);

            // 2. Establish a Calcite-managed connection (used to get the Calcite root schema container)
            Properties info = new Properties();
            info.setProperty("lex", "JAVA"); // parsing behavior of the Calcite connection itself
            Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:", info); // open Calcite
            CalciteConnection unwrapCalciteConnection = calciteConnection.unwrap(CalciteConnection.class); // get Calcite API

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

            // Parser config: fold unquoted identifiers to lower-case, case-insensitive.
            // Using Lex.MYSQL yields a behavior similar to PostgreSQL name resolution.
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
     * Parse, validate, and normalize a SQL query into an optimized {@link RelNode}.
     *
     * <p>Steps:</p>
     * <ol>
     *   <li>Sanitize input by removing trailing semicolons (Calcite's parser rejects them).</li>
     *   <li>Parse SQL string into a {@link SqlNode} (AST).</li>
     *   <li>Validate the AST against the JDBC-backed schema.</li>
     *   <li>Convert the validated AST to a logical plan (RelNode).</li>
     *   <li>Apply phased HepPlanner programs to normalize and stabilize the plan.</li>
     * </ol>
     *
     * <p>The returned RelNode is suitable for structural comparison and transformation.</p>
     *
     * @param planner The Calcite planner instance
     * @param sql The SQL query string
     * @return Optimized RelNode
     * @throws Exception on parse/validation/optimization errors
     */
    public static RelNode getOptimizedRelNode(Planner planner, String sql) throws Exception
    {
        //System.out.println("Processing SQL: " + sql);

        // 1. Sanitize input: Calcite parser doesn't accept trailing ';'
        String sqlForParse = sql == null ? null : sql.trim();
        if (sqlForParse != null) {
            // Remove one or more trailing semicolons if present (robustness for copy/paste SQL)
            while (sqlForParse.endsWith(";")) {
                sqlForParse = sqlForParse.substring(0, sqlForParse.length() - 1).trim();
            }
            // Normalize non-standard inequality operator '!=' to standard '<>' for Calcite parser
            sqlForParse = sqlForParse.replace("!=", "<>");
        }

        // 2. Parse the SQL string into an AST (SqlNode)
        SqlNode sqlNode = planner.parse(sqlForParse);

        // 3. Validate the AST: resolves names/types against the configured schema
        SqlNode validatedSqlNode = planner.validate(sqlNode);

        // 4. Convert the validated AST to RelNode (Logical Plan)
        RelNode logicalPlan = planner.rel(validatedSqlNode).rel;

        // 5. Optimize in phases to avoid oscillations and collapse redundant projections
        // Phase 1: basic simplification
        HepProgramBuilder p1 = new HepProgramBuilder();
        p1.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS); // fold constants, simplify predicates
        p1.addRuleInstance(CoreRules.PROJECT_MERGE);            // collapse stacked projects
        p1.addRuleInstance(CoreRules.PROJECT_REMOVE);           // drop identity projections

        // Phase 2: normalize inner join structure safely
        HepProgramBuilder p2 = new HepProgramBuilder();
        p2.addMatchOrder(HepMatchOrder.TOP_DOWN);               // predictable traversal to minimize oscillation
        p2.addMatchLimit(200);                            // guard against infinite transforms
        p2.addRuleInstance(CoreRules.FILTER_INTO_JOIN);         // push filters into joins
        p2.addRuleInstance(CoreRules.JOIN_ASSOCIATE);           // re-associate joins
        p2.addRuleInstance(CoreRules.JOIN_COMMUTE);             // commute join inputs

        // Phase 3: collapse projects introduced by rewrites and re-simplify
        HepProgramBuilder p3 = new HepProgramBuilder();
        p3.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);   // move projects through joins
        p3.addRuleInstance(CoreRules.PROJECT_MERGE);            // merge adjacent projects again
        p3.addRuleInstance(CoreRules.PROJECT_REMOVE);           // drop identities introduced
        p3.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);// re-simplify predicates

        HepProgramBuilder pb = new HepProgramBuilder();
        pb.addSubprogram(p1.build()); // add phase 1
        pb.addSubprogram(p2.build()); // add phase 2
        pb.addSubprogram(p3.build()); // add phase 3
        HepProgram hepProgram = pb.build();                     // compile the program
        HepPlanner hepPlanner = new HepPlanner(hepProgram);     // initialize planner

        hepPlanner.setRoot(logicalPlan);                        // set input plan
        RelNode optimizedPlan = hepPlanner.findBestExp();       // execute optimization

        return optimizedPlan;
    }

    /**
     * Parse and validate a SQL query into a logical (unoptimized) {@link RelNode}.
     *
     * <p>Removes trailing semicolons for parser compatibility, parses to AST, validates, and converts to RelNode.</p>
     *
     * @param planner The Calcite planner instance
     * @param sql The SQL query string
     * @return Logical RelNode (not optimized)
     * @throws Exception on parse/validation errors
     */
    public static RelNode getRelNode(Planner planner, String sql) throws Exception {
        // Sanitize input: Calcite parser doesn't accept trailing ';'
        String sqlForParse = sql == null ? null : sql.trim();
        if (sqlForParse != null) {
            while (sqlForParse.endsWith(";")) {
                sqlForParse = sqlForParse.substring(0, sqlForParse.length() - 1).trim();
            }
            sqlForParse = sqlForParse.replace("!=", "<>");
        }
        // Parse, validate, and convert to RelNode
        SqlNode sqlNode = planner.parse(sqlForParse);
        SqlNode validatedSqlNode = planner.validate(sqlNode);
        RelNode logicalPlan = planner.rel(validatedSqlNode).rel;
        return logicalPlan;
    }

    /**
     * Compare two SQL queries for semantic equivalence, optionally applying transformations
     * to the first query's RelNode before comparison.
     *
     * <p>Strategy (in order):</p>
     * <ol>
     *   <li>Compare Calcite structural digests (order-sensitive, precise on structure).</li>
     *   <li>If different, compare normalized digests that ignore input-ref indices (e.g. $0 → $x),
     *       which neutralizes differences due to field index shifts (common when join inputs swap).</li>
     *   <li>If still different, compare a canonical digest where inner-join children are treated as
     *       an unordered set (child digests sorted), commutative/symmetric expressions are normalized, and harmless CASTs are ignored. Projection order is preserved.</li>
     *   <li>If still different, compare tree structure ignoring child order.</li>
    *   <li>Final fallback: object equality or deepEquals (rarely helpful).</li>
     * </ol>
     *
     * @param sql1 First query string
     * @param sql2 Second query string
     * @param transformations Optional list of CoreRule names to apply to the first plan
     * @return true if any of the above comparisons match; false otherwise or on planning error
     */
    public static boolean compareQueries(String sql1, String sql2, List<String> transformations)
    {
        //Check if SQL strings are equal, if so, return true directly
        if (sql1.equals(sql2)) return true;

        FrameworkConfig config = getFrameworkConfig();
        Planner planner = Frameworks.getPlanner(config); // create a planner tied to this config

        try {
            // Get the optimized RelNode for the first query
            RelNode rel1 = getOptimizedRelNode(planner, sql1);

            if (transformations != null && !transformations.isEmpty()) 
                rel1 = applyTransformations(rel1, transformations); // apply rules as given by LLM

            planner.close();                          // planner cannot be reused across parse/validate cycles reliably
            planner = Frameworks.getPlanner(config);  // create a fresh planner for the second query

            // Get the optimized RelNode for the second query
            RelNode rel2 = getOptimizedRelNode(planner, sql2);

            // Fast path: structural digests (order-sensitive and precise on structure).
            String d1 = RelOptUtil.toString(rel1, SqlExplainLevel.DIGEST_ATTRIBUTES);
            String d2 = RelOptUtil.toString(rel2, SqlExplainLevel.DIGEST_ATTRIBUTES);

            if (d1.equals(d2)) return true;
            // Fallback 1: neutralize input indexes (e.g., $0 → $x) to reduce false negatives
            String nd1 = normalizeDigest(d1);
            String nd2 = normalizeDigest(d2);

            if (nd1.equals(nd2)) return true;
            
            // Fallback 2: canonical digest that treats inner-join children as unordered
            String c1 = canonicalDigest(rel1);
            String c2 = canonicalDigest(rel2);

            if (c1.equals(c2)) return true;

            // Fallback 3: tree comparison ignoring child order
            RelTreeNode tree1 = buildRelTree(rel1);
            RelTreeNode tree2 = buildRelTree(rel2);

            if (tree1.equalsIgnoreChildOrder(tree2)) return true;

            if (rel1.equals(rel2))  return true;

            return rel1.deepEquals(rel2); // final fallback: deepEquals (rarely helpful)

        } catch (Exception e)
        {
            // Planning/parsing/validation error: treat as non-equivalent.
            //System.err.println("[Calcite.compareQueries] Planning error: " + e.getMessage());
            return false;
        } finally {
            planner.close(); // ensure planner resources are released
        }
    }

    /**
     * Debug version of compareQueries that prints intermediate digests when queries are not deemed equivalent.
     * Tag can be a query id to aid tracing.
     */
    public static boolean compareQueriesDebug(String sql1, String sql2, List<String> transformations, String tag)
    {
        if (sql1.equals(sql2)) return true;

        FrameworkConfig config = getFrameworkConfig();
        Planner planner = Frameworks.getPlanner(config);
        try {
            RelNode rel1 = getOptimizedRelNode(planner, sql1);
            if (transformations != null && !transformations.isEmpty()) {
                rel1 = applyTransformations(rel1, transformations);
            }
            planner.close();
            planner = Frameworks.getPlanner(config);
            RelNode rel2 = getOptimizedRelNode(planner, sql2);

            String d1 = RelOptUtil.toString(rel1, SqlExplainLevel.DIGEST_ATTRIBUTES);
            String d2 = RelOptUtil.toString(rel2, SqlExplainLevel.DIGEST_ATTRIBUTES);
            if (d1.equals(d2)) return true;

            String nd1 = normalizeDigest(d1);
            String nd2 = normalizeDigest(d2);
            if (nd1.equals(nd2)) return true;

            String c1 = canonicalDigest(rel1);
            String c2 = canonicalDigest(rel2);
            if (c1.equals(c2)) return true;

            RelTreeNode t1 = buildRelTree(rel1);
            RelTreeNode t2 = buildRelTree(rel2);
            String ct1 = t1 == null ? "null" : t1.canonicalDigest();
            String ct2 = t2 == null ? "null" : t2.canonicalDigest();
            if (ct1.equals(ct2)) return true;

            // Final object equality fallback: include deepEquals like compareQueries
            boolean eq = rel1.equals(rel2) || rel1.deepEquals(rel2);
            if (!eq) {
                System.out.println("[DEBUG compareQueries tag=" + tag + "] NOT EQUAL");
                System.out.println("  Structural digest A:\n" + d1);
                System.out.println("  Structural digest B:\n" + d2);
                System.out.println("  Normalized digest A:\n" + nd1);
                System.out.println("  Normalized digest B:\n" + nd2);
                System.out.println("  Canonical digest A:\n" + c1);
                System.out.println("  Canonical digest B:\n" + c2);
                System.out.println("  Tree canonical A:\n" + ct1);
                System.out.println("  Tree canonical B:\n" + ct2);
            }
            return eq;
        } catch (Exception e) {
            System.out.println("[DEBUG compareQueries tag=" + tag + "] Planning error: " + e.getMessage());
            return false;
        } finally {
            planner.close();
        }
    }

    /**
     * Normalize a digest string by replacing input references like $0, $12 with a
     * placeholder ($x) and collapsing repeated spaces. This reduces sensitivity to
     * field index positions that shift when join inputs are swapped.
     *
     * @param digest The input digest string
     * @return Normalized digest string
     */
    public static String normalizeDigest(String digest) {
        if (digest == null) return null;
        // Replace input refs like $0, $12 with a placeholder to make join-child order less significant
        String s = digest.replaceAll("\\$\\d+", "\\$x");
        // Collapse multiple spaces for stability so cosmetic spacing doesn't differ
        s = s.replaceAll("[ ]+", " ");
        return s.trim();
    }

    /**
     * Build a canonical digest for a plan where:
     * <ul>
     *   <li>Inner-join children are treated as an unordered set (child digests sorted)</li>
     *   <li>All CASTs are recursively stripped from RexNode expressions before digesting</li>
     *   <li>Commutative and symmetric expressions (AND, OR, EQUALS, etc.) are normalized</li>
     *   <li>Filter conditions and expression strings are normalized via {@link #normalizeDigest(String)}</li>
     * </ul>
     * Projection order is preserved. This makes the digest insensitive to inner-join child order, commutative/symmetric expressions, and harmless CASTs, while keeping column order significant.
     *
     * @param rel The input RelNode
     * @return Canonical digest string
     */
    // ...existing code...
    public static String canonicalDigest(RelNode rel) {
        if (rel == null) return "null"; // explicit null marker for consistency
        if (rel instanceof LogicalProject p) { // Project: keep output order significant
            StringBuilder sb = new StringBuilder();
            sb.append("Project[");
            for (int i = 0; i < p.getProjects().size(); i++) {
                RexNode rex = p.getProjects().get(i);
                if (i > 0) sb.append(",");
                sb.append(canonicalizeRex(rex));
            }
            sb.append("]->");
            sb.append(canonicalDigest(p.getInput()));
            return sb.toString();
        }
        if (rel instanceof LogicalFilter f) { // Filter: normalize predicate and recurse
            return "Filter(" + canonicalizeRex(f.getCondition()) + ")->" + canonicalDigest(f.getInput());
        }
        if (rel instanceof LogicalJoin j) { // Join: sort children for INNER join type
            String condStr = canonicalizeRex(j.getCondition());
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
            return "Join(" + j.getJoinType() + "," + condStr + "){" + a + "|" + b + "}";
        }
        // Set operations: normalize UNION/UNION ALL by flattening nested unions and
        // sorting child digests (associative + commutative under multiset semantics).
        if (rel instanceof Union u) {
            boolean all = u.all;
            List<RelNode> flatChildren = new ArrayList<>();
            flattenUnionInputs(u, all, flatChildren);
            List<String> childDigests = new ArrayList<>();
            for (RelNode in : flatChildren) childDigests.add(canonicalDigest(in));
            childDigests.sort(String::compareTo);
            return "Union(" + (all ? "ALL" : "DISTINCT") + ")" + childDigests.toString();
        }
        if (rel instanceof TableScan ts) { // Table scan: include fully qualified name
            return "Scan(" + String.join(".", ts.getTable().getQualifiedName()) + ")";
        }
        if (rel instanceof LogicalSort s) { // Sort/Limit: ignore sort keys, keep fetch/offset presence
            String fetch = s.fetch == null ? "" : ("fetch=" + canonicalizeRex(s.fetch));
            String offset = s.offset == null ? "" : ("offset=" + canonicalizeRex(s.offset));
            String meta = (fetch + (fetch.isEmpty() || offset.isEmpty() ? "" : ",") + offset).trim();
            String head = meta.isEmpty() ? "Sort" : ("Sort(" + meta + ")");
            return head + "->" + canonicalDigest(s.getInput());
        }
        // Default: normalized string of this node with placeholders, plus canonical children
        String typeName = rel.getRelTypeName();
        if (typeName == null) typeName = "UnknownRel";
        String head = normalizeDigest(typeName);
        StringBuilder sb = new StringBuilder(head);
        sb.append("[");
        boolean first = true;
        for (RelNode in : rel.getInputs()) {
            if (!first) sb.append("|"); // separate child digests
            sb.append(canonicalDigest(in));
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }
    
    /**
     * Canonicalize a RexNode expression to a stable string for robust plan comparison.
     * <ul>
     *   <li>Recursively strips CASTs</li>
     *   <li>Sorts operands for commutative operators (AND, OR, PLUS, TIMES)</li>
     *   <li>Treats EQUALS/NOT_EQUALS as symmetric (order-insensitive)</li>
     *   <li>Applies normalizeDigest to reduce noise from input refs and spacing</li>
     * </ul>
     *
     * @param node The RexNode expression
     * @return Canonicalized string representation
     */
    private static String canonicalizeRex(RexNode node) {
        if (node == null) return "null";
        RexNode n = stripAllCasts(node);
        if (n instanceof RexCall call && call.getOperator() != null) {
            SqlKind kind = call.getOperator().getKind();
            List<String> parts = new ArrayList<>();
            for (RexNode op : call.getOperands()) parts.add(canonicalizeRex(op));
            switch (kind) {
                case AND, OR, PLUS, TIMES -> {
                    parts.sort(String::compareTo);
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
                case EQUALS, NOT_EQUALS -> {
                    parts.sort(String::compareTo);
                    String opName = kind == SqlKind.EQUALS ? "=" : "<>";
                    return normalizeDigest(opName + "(" + String.join(",", parts) + ")");
                }
                case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    // Normalize > to < with operands swapped; >= to <= with operands swapped.
                    boolean flipToLess = (kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL);
                    String left = parts.size() > 0 ? parts.get(0) : "?";
                    String right = parts.size() > 1 ? parts.get(1) : "?";
                    String finalOp;
                    if (flipToLess) {
                        // swap operands and map operator
                        String tmp = left; left = right; right = tmp;
                        finalOp = (kind == SqlKind.GREATER_THAN) ? "<" : "<=";
                    } else {
                        finalOp = (kind == SqlKind.LESS_THAN) ? "<" : "<=";
                    }
                    // Canonical orientation: ensure left <= right lexicographically for deterministic form
                    int cmp = left.compareTo(right);
                    if (cmp > 0) {
                        // Swap orientation by converting < / <= into > / >= then normalize again by flipping
                        // Instead of introducing >, just swap strings so smaller is first; semantics preserved for equality checks.
                        String tmp = left; left = right; right = tmp;
                    }
                    return normalizeDigest(finalOp + "(" + left + "," + right + ")");
                }
                case CAST -> {
                    // Should be stripped already, but handle gracefully
                    if (!call.getOperands().isEmpty()) return canonicalizeRex(call.getOperands().get(0));
                    return "CAST(?)";
                }
                default -> {
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
            }
        }
        return normalizeDigest(n.toString());
    }
    /**
     * Recursively strip all CASTs from a RexNode tree.
     *
     * @param node The RexNode expression
     * @return RexNode with all CASTs removed
     */
    private static RexNode stripAllCasts(RexNode node) {
        if (node instanceof RexCall call && call.getOperator() != null && call.getOperator().getKind() == SqlKind.CAST) {
            if (call.getOperands() != null && !call.getOperands().isEmpty()) {
                return stripAllCasts(call.getOperands().get(0));
            }
        }
        if (node instanceof RexCall call) {
            List<RexNode> newOps = new ArrayList<>();
            for (RexNode op : call.getOperands()) {
                newOps.add(stripAllCasts(op));
            }
            // If any operand changed, rebuild the call
            boolean changed = false;
            List<RexNode> origOps = call.getOperands();
            for (int i = 0; i < origOps.size(); i++) {
                if (origOps.get(i) != newOps.get(i)) { changed = true; break; }
            }
            if (changed) {
                // Rebuild the call with new operands (using the same operator/type)
                return call.clone(call.getType(), newOps);
            }
        }
        return node;
    }

    /**
     * Recursively collect inputs of nested Union nodes that share the same ALL/DISTINCT property.
     * Flattens nested unions for normalization.
     *
     * @param rel The RelNode (possibly a Union)
     * @param targetAll Whether to flatten ALL or DISTINCT unions
     * @param out Output list to collect flattened inputs
     */
    private static void flattenUnionInputs(RelNode rel, boolean targetAll, List<RelNode> out) {
        if (rel instanceof Union u && u.all == targetAll) {
            for (RelNode in : u.getInputs()) flattenUnionInputs(in, targetAll, out);
        } else {
            out.add(rel);
        }
    }

    /**
     * Print the RelTreeNode tree for a given SQL query (for debugging/visualization).
     *
     * @param sql1 The SQL query string
     * @param sql2 (Unused) Second SQL query string (for future extension)
     */
    public static void printRelTrees (String sql1, String sql2) {
        RelNode rel;
        try {
            rel = getOptimizedRelNode(Frameworks.getPlanner(getFrameworkConfig()), sql1);
            System.out.println("RelTreeNode tree1: \n" + buildRelTree(rel).toString());
        } catch (Exception e) {
            System.err.println("[Calcite.printRelTrees] Planning error: " + e.getMessage());
        }
    }

    /**
     * Convert a Calcite RelNode graph into a simple tree of {@link RelTreeNode}
     * using a RelVisitor. Children are kept in input order.
     */
    public static RelTreeNode buildRelTree(RelNode rel) {
        if (rel == null) return null;

        final Map<RelNode, RelTreeNode> built = new IdentityHashMap<>(); // map physical nodes to constructed tree nodes
        final RelTreeNode[] rootHolder = new RelTreeNode[1];             // simple holder for root reference

        RelVisitor builder = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                RelTreeNode cur = new RelTreeNode(summarizeNode(node)); // create tree node label for this RelNode
                built.put(node, cur);                                    // remember mapping
                if (parent == null) {
                    rootHolder[0] = cur;                                 // this is the root
                } else {
                    RelTreeNode p = built.get(parent);                   // find parent's tree node
                    if (p == null) {
                        // In rare cases, if the parent hasn't been recorded yet, create and link it.
                        p = new RelTreeNode(summarizeNode(parent));
                        built.put(parent, p);
                        if (rootHolder[0] == null) rootHolder[0] = p;    // initialize root if needed
                    }
                    p.addChild(cur);                                     // link child in input order
                }
                super.visit(node, ordinal, parent);                       // recurse on children
            }
        };

        builder.go(rel);                                                  // perform traversal/build
        return rootHolder[0];                                            // return root of constructed tree
    }

    /**
     * Create a concise per-node label suitable for a tree display.
     *
     * @param rel The RelNode
     * @return String label for the node
     */
    private static String summarizeNode(RelNode rel) {
        if (rel == null) return "(null)";
        if (rel instanceof LogicalProject p) { // show project expressions in order
            List<String> exprs = new ArrayList<>();
            for (RexNode rex : p.getProjects()) {
                exprs.add(canonicalizeRex(rex));
            }
            return "Project[" + String.join(",", exprs) + "]";
        }
        if (rel instanceof LogicalFilter f) { // show normalized filter predicate
            return "Filter(" + canonicalizeRex(f.getCondition()) + ")";
        }
        if (rel instanceof LogicalJoin j) { // include join type and normalized condition
            return "Join(" + j.getJoinType() + "," + canonicalizeRex(j.getCondition()) + ")";
        }
        if (rel instanceof LogicalSort s) { // compact label for sort/limit in tree view
            String fetch = s.fetch == null ? "" : ("fetch=" + canonicalizeRex(s.fetch));
            String offset = s.offset == null ? "" : ("offset=" + canonicalizeRex(s.offset));
            String meta = (fetch + (fetch.isEmpty() || offset.isEmpty() ? "" : ",") + offset).trim();
            return meta.isEmpty() ? "Sort" : ("Sort(" + meta + ")");
        }
        if (rel instanceof TableScan ts) { // fully qualified table name
            return "Scan(" + String.join(".", ts.getTable().getQualifiedName()) + ")";
        }
        return rel.getRelTypeName(); // fallback: node type
    }

    /**
     * Compare two {@link RelTreeNode} trees for structural equality.
     * If {@code ignoreChildOrder} is true, children at each node are treated as an
     * unordered multiset via canonical digests.
     *
     * @param a First RelTreeNode
     * @param b Second RelTreeNode
     * @param ignoreChildOrder Whether to ignore child order at each node
     * @return true if trees are equivalent (order-sensitive or insensitive)
     */
    public static boolean compareRelTrees(RelTreeNode a, RelTreeNode b, boolean ignoreChildOrder) {
        if (!ignoreChildOrder) {
            if (a == b) return true;
            if (a == null || b == null) return false;
            return a.equals(b); // strict order-sensitive equality
        }
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.canonicalDigest().equals(b.canonicalDigest()); // order-insensitive via canonical form
    }

    /**
     * Build trees from two RelNodes and compare them using {@link #compareRelTrees}.
     * This is a convenience for order-sensitive or order-insensitive plan tree equality.
     *
     * @param r1 First RelNode
     * @param r2 Second RelNode
     * @param ignoreChildOrder Whether to ignore child order at each node
     * @return true if trees are equivalent (order-sensitive or insensitive)
     */
    public static boolean compareRelNodes(RelNode r1, RelNode r2, boolean ignoreChildOrder) {
        RelTreeNode t1 = buildRelTree(r1);
        RelTreeNode t2 = buildRelTree(r2);
        return compareRelTrees(t1, t2, ignoreChildOrder);
    }

    /**
     * Get a canonical, order-insensitive digest string for a RelNode's tree.
     *
     * @param rel The RelNode
     * @return Canonical digest string for the tree
     */
    public static String relTreeCanonicalDigest(RelNode rel) {
        RelTreeNode t = buildRelTree(rel);
        return t == null ? "null" : t.canonicalDigest();
    }

    // (Removed) stripTopLevelCasts: superseded by stripAllCasts which handles recursive CAST removal

    /**
     * Apply an explicit list of HepPlanner CoreRules (by name) to a plan, producing a
     * new {@link RelNode}. This is useful for experimenting with how particular rules
     * affect normalization and equivalence.
     *
     * <p>Supported rule name keys map to Calcite's {@link CoreRules}. Unknown names are ignored.</p>
     *
     * @param rel The input RelNode
     * @param transformations List of rule names to apply (case-insensitive)
     * @return Transformed RelNode
     */
    public static RelNode applyTransformations(RelNode rel, List<String> transformations)
    {
        RelNode newRel = rel;

    //System.out.println("RelTree before transformations: \n" + buildRelTree(newRel).toString());

        for (String transform : transformations)
        {
            String key = transform == null ? "" : transform.toLowerCase();
            HepProgramBuilder pb = new HepProgramBuilder();
            boolean recognized = true;
            switch (key)
            {
                case "projectmergerule" -> pb.addRuleInstance(CoreRules.PROJECT_MERGE);
                case "projectremoverule" -> pb.addRuleInstance(CoreRules.PROJECT_REMOVE);
                case "projectjointransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
                case "projectfiltertransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE);
                case "projectsetoptransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE);
                case "projecttablescanrule" -> pb.addRuleInstance(CoreRules.PROJECT_TABLE_SCAN);
                case "filtermergerule" -> pb.addRuleInstance(CoreRules.FILTER_MERGE);
                case "filterprojecttransposerule" -> pb.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
                case "filterjoinrule" -> pb.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
                case "filteraggregatetransposerule" -> pb.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
                case "filterwindowtransposerule" -> pb.addRuleInstance(CoreRules.FILTER_WINDOW_TRANSPOSE);
                case "joincommuterule" -> pb.addRuleInstance(CoreRules.JOIN_COMMUTE);
                case "joinassociaterule" -> pb.addRuleInstance(CoreRules.JOIN_ASSOCIATE);
                case "joinpushexpressionsrule" -> pb.addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS);
                case "joinconditionpushrule" -> pb.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
                case "aggregateprojectpullupconstantsrule" -> pb.addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
                case "aggregateremoverule" -> pb.addRuleInstance(CoreRules.AGGREGATE_REMOVE);
                case "aggregatejointransposerule" -> pb.addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE);
                case "aggregateuniontransposerule" -> pb.addRuleInstance(CoreRules.AGGREGATE_UNION_TRANSPOSE);
                case "aggregateprojectmergerule" -> pb.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
                case "aggregatecasetofilterrule" -> pb.addRuleInstance(CoreRules.AGGREGATE_CASE_TO_FILTER);
                case "sortremoverule" -> pb.addRuleInstance(CoreRules.SORT_REMOVE);
                case "sortuniontransposerule" -> pb.addRuleInstance(CoreRules.SORT_UNION_TRANSPOSE);
                case "sortprojecttransposerule" -> pb.addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE);
                case "sortjointransposerule" -> pb.addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE);
                case "unionmergerule" -> pb.addRuleInstance(CoreRules.UNION_MERGE);
                case "unionpullupconstantsrule" -> pb.addRuleInstance(CoreRules.UNION_PULL_UP_CONSTANTS);
                case "intersecttodistinctrule" -> pb.addRuleInstance(CoreRules.INTERSECT_TO_DISTINCT);
                case "minustodistinctrule" -> pb.addRuleInstance(CoreRules.MINUS_TO_DISTINCT);
                case "projectwindowtransposerule" -> pb.addRuleInstance(CoreRules.PROJECT_WINDOW_TRANSPOSE);
                default -> recognized = false;
            }

            if (!recognized) continue;

            HepPlanner planner = new HepPlanner(pb.build()); // one-off planner for this rule set
            planner.setRoot(newRel);                          // apply to the latest plan
            RelNode result = planner.findBestExp();           // run and get transformed plan
            newRel = result;
        }

    //System.out.println("RelTree after transformations: \n" + buildRelTree(newRel).toString());

        return newRel;
    }
}
