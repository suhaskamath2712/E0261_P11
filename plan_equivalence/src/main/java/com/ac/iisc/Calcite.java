package com.ac.iisc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

/**
 * Calcite parsing and transformation helper.
 *
 * Responsibilities:
 * - Build a Calcite {@link FrameworkConfig} backed by a PostgreSQL schema (via JDBC).
 * - Parse/validate SQL to {@link RelNode} and optionally optimize it using phased HepPlanner programs.
 * - Compare queries via layered digests to judge equivalence:
 *   - Structural digest: {@code RelOptUtil.toString(..., DIGEST_ATTRIBUTES)} — order‑sensitive.
 *   - Normalized digest: {@link #normalizeDigest(String)} — replaces input refs like {@code $0}→{@code $x}, collapses spacing.
 *   - Canonical digest: {@link #canonicalDigest(RelNode)} — inner‑join children flattened and sorted; expressions canonicalized; CASTs stripped; aggregates ordered.
 *   - Tree canonical digest: {@link RelTreeNode#canonicalDigest()} — ignores child order across the tree.
 * - Apply specific {@link org.apache.calcite.rel.rules.CoreRules} by name to a {@link RelNode}
 *   using a registry‑driven approach (`RULE_MAP`) and a composite Hep program to reduce oscillation.
 * - Normalize scalar subqueries (convert to correlates) and decorrelate to join+aggregate forms where possible, symmetrically for both sides.
 *
 * Notes:
 * - Unquoted identifiers fold to lower‑case (PostgreSQL‑like).
 * - Trailing semicolons are stripped; non‑standard {@code !=} is normalized to {@code <>}.
 * - Canonical digest preserves meaningful order (projection, non‑INNER joins) but removes noise (commutativity, CASTs, input‑ref positions).
 * - A recursion‑path set makes canonicalization safe on DAGs/shared subgraphs.
 */
public class Calcite {
    /**
     * Build a Calcite {@link FrameworkConfig} backed by the configured
     * PostgreSQL schema. This is a convenience delegator that forwards to
     * {@link CalciteUtil#getFrameworkConfig()} so callers can continue to use
     * the historical {@code Calcite.getFrameworkConfig()} entry point while
     * the implementation lives in {@code CalciteUtil}.
     *
     * @return FrameworkConfig bound to the PostgreSQL schema configured via {@link FileIO}
     * @throws RuntimeException if the PostgreSQL driver or JDBC connection cannot be initialized
     */
    public static FrameworkConfig getFrameworkConfig() {
        return CalciteUtil.getFrameworkConfig();
    }

    /**
    * Parse, validate, and normalize a SQL query into an optimized {@link RelNode}.
    *
    * Steps:
    * 1. Sanitize input by removing trailing semicolons (Calcite's parser rejects them).
    * 2. Parse SQL string into a {@link SqlNode} (AST).
    * 3. Validate the AST against the JDBC-backed schema.
    * 4. Convert the validated AST to a logical plan (RelNode).
    * 5. Apply phased HepPlanner programs to normalize and stabilize the plan.
    *
    * The returned RelNode is suitable for structural comparison and transformation.
    *
    * @param planner The Calcite planner instance
    * @param sql The SQL query string
    * @return Optimized RelNode
    * @throws Exception on parse/validation/optimization errors
    */
    public static RelNode getOptimizedRelNode(Planner planner, String sql) throws Exception
    {
        // 1. Perform string operations on SQL string to sanitise input
        // Sanitize input: Calcite parser doesn't accept trailing ';' or trailing whitespace
        String sqlForParse = sql == null ? null : sql.trim();

        if (sqlForParse != null) {
            // Remove trailing semicolons if present 
            while (sqlForParse.endsWith(";"))
                sqlForParse = sqlForParse.substring(0, sqlForParse.length() - 1).trim();
            
            // Normalize non-standard inequality operator '!=' to standard '<>' for Calcite parser
            sqlForParse = sqlForParse.replace("!=", "<>");

            // Rewrite dialect-specific LEAST/GREATEST calls into standard SQL
            // CASE expressions as a defensive fallback. This ensures queries
            // that use these functions can still be planned even if the
            // operator table configuration for a given Calcite version does
            // not register them as built-ins.
            sqlForParse = CalciteUtil.rewriteLeastGreatest(sqlForParse);
        }

        // 2. Parse the SQL string into an AST (SqlNode)
        // 3. Validate the AST: resolves names/types against the configured schema
        // 4. Convert the validated AST to RelNode (Logical Plan)
        RelNode logicalPlan = planner.rel(planner.validate(planner.parse(sqlForParse))).rel;

        // 5. Optimize in phases to avoid oscillations and collapse redundant projections
        // Phase 1: basic simplification
        HepProgramBuilder p1 = new HepProgramBuilder();
        // fold constants, simplify predicates
        p1.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        // collapse stacked projects
        p1.addRuleInstance(CoreRules.PROJECT_MERGE);
        // drop identity projections
        p1.addRuleInstance(CoreRules.PROJECT_REMOVE);

        // Phase 2: normalize inner join structure safely
        HepProgramBuilder p2 = new HepProgramBuilder();
        // predictable traversal to minimize oscillation
        p2.addMatchOrder(HepMatchOrder.TOP_DOWN);
        // guard against infinite transforms
        p2.addMatchLimit(200);
        // push filters into joins
        p2.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        // re-associate joins
        p2.addRuleInstance(CoreRules.JOIN_ASSOCIATE);
        // commute join inputs
        p2.addRuleInstance(CoreRules.JOIN_COMMUTE);

        // Phase 3: collapse projects introduced by rewrites and re-simplify
        HepProgramBuilder p3 = new HepProgramBuilder();
        // move projects through joins
        p3.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
        // merge adjacent projects again
        p3.addRuleInstance(CoreRules.PROJECT_MERGE);
        // drop identities introduced
        p3.addRuleInstance(CoreRules.PROJECT_REMOVE);
        // re-simplify predicates
        p3.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);

        HepProgramBuilder pb = new HepProgramBuilder();
        // add phase 1
        pb.addSubprogram(p1.build());
        // add phase 2 
        pb.addSubprogram(p2.build());
        // add phase 3
        pb.addSubprogram(p3.build());

        // compile the program & initialize planner
        HepPlanner hepPlanner = new HepPlanner(pb.build());
        
        // set input plan
        hepPlanner.setRoot(logicalPlan);                             

        // execute optimization
        return hepPlanner.findBestExp();
    }

    /**
     * Construct a {@link RelNode} from a JSON query plan by delegating to
     * {@link CalciteUtil#jsonPlanToRelNode(String)}.
     */
    public static RelNode jsonPlanToRelNode(String jsonPlan) {
        return CalciteUtil.jsonPlanToRelNode(jsonPlan);
    }
    /**
     * Compare two SQL queries for semantic equivalence, optionally applying transformations
     * to the first query's RelNode before comparison.
     *
    * Strategy (in order):
    * 1. Compare Calcite structural digests (order-sensitive, precise on structure).
    * 2. If different, compare normalized digests that ignore input-ref indices (e.g. $0 → $x),
    *    which neutralizes differences due to field index shifts (common when join inputs swap).
    * 3. If still different, compare a canonical digest where inner-join children are treated as
    *    an unordered set (child digests sorted), commutative/symmetric expressions are normalized,
    *    and harmless CASTs are ignored. Projection order is preserved.
    * 4. If still different, compare tree structure ignoring child order.
    * 5. Final fallback: object equality or deepEquals (rarely helpful).
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

        // create a planner tied to this config
        Planner planner = Frameworks.getPlanner(config); 

        try {
            // Note: Planning produces Calcite logical operators (e.g., LogicalJoin).
            // Physical variants like HashJoin/NestedLoop do not appear here, so join
            // type names are effectively normalized to logical forms by design.
            // Get the optimized RelNode for the first query
            RelNode rel1 = getOptimizedRelNode(planner, sql1);

            // planner cannot be reused across parse/validate cycles reliably
            planner.close();
            
            // create a fresh planner for the second query
            planner = Frameworks.getPlanner(config);  

            // Get the optimized RelNode for the second query
            RelNode rel2 = getOptimizedRelNode(planner, sql2);

            // Delegate to the RelNode-based equivalence checker so the core
            // comparison logic is shared between SQL-string and RelNode APIs.
            return compareRelNodesForEquivalence(rel1, rel2, transformations);
        } catch (Exception e)
        {
            // Planning/parsing/validation error: treat as non-equivalent.
            System.err.println("[Calcite.compareQueries] Planning error: " + e.getMessage());
            return false;
        } finally {
            // ensure planner resources are released
            planner.close();
        }
    }

    /**
     * Compare two pre-built RelNodes for semantic equivalence using the same
     * strategy as {@link #compareQueries(String, String, List)}.
     *
     * This is useful when callers already have RelNodes (for example, obtained
     * from a planner elsewhere) and want to re-use the equivalence engine
     * without re-parsing SQL strings.
     */
    public static boolean compareQueries(RelNode rel1, RelNode rel2, List<String> transformations) {
        return compareRelNodesForEquivalence(rel1, rel2, transformations);
    }

    /**
     * Core equivalence logic over RelNodes shared by both compareQueries
     * overloads.
     */
    private static boolean compareRelNodesForEquivalence(RelNode rel1, RelNode rel2, List<String> transformations) {
        if (rel1 == null || rel2 == null) {
            return false;
        }
        try {
            // apply rules as given by LLM to the first plan
            if (transformations != null && !transformations.isEmpty()) {
                rel1 = applyTransformations(rel1, transformations);
            }

            // Normalize sub-queries and decorrelate to align scalar subquery vs join forms
            rel1 = normalizeSubqueriesAndDecorrelate(rel1);
            // Normalize sub-queries and decorrelate symmetrically for the second plan as well
            rel2 = normalizeSubqueriesAndDecorrelate(rel2);

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

            // Note: canonicalDigest flattens INNER joins and sorts child digests,
            // providing deterministic ordering to remove commutativity differences
            // (i.e., join child order is normalized before comparison).
            if (c1.equals(c2)) return true;

            // Fallback 3: tree comparison ignoring child order
            RelTreeNode tree1 = buildRelTree(rel1);
            RelTreeNode tree2 = buildRelTree(rel2);
            // equalsIgnoreChildOrder also compares via a canonical form that ignores
            // child ordering, further neutralizing INNER-join commutativity.
            if (tree1.equalsIgnoreChildOrder(tree2))  return true;

            // Fallback 4: compare cleaned PostgreSQL execution plans as a last resort.
            // If both queries yield the same physical plan on the target database,
            // treat them as equivalent even if Calcite's logical digests differ.
            String p1 = convertRelNodetoJSONQueryPlan(rel1);
            String p2 = convertRelNodetoJSONQueryPlan(rel2);
            if (p1 != null && p1.equals(p2)) return true;

            if (transformations != null) {
                System.out.println("\n[Calcite.compareQueries] NOT EQUIVALENT\n\n");
                System.out.println("Transformed Rel1: \n" + RelOptUtil.toString(rel1, SqlExplainLevel.DIGEST_ATTRIBUTES) + "\n");
                System.out.println("Rel2: \n" + RelOptUtil.toString(rel2, SqlExplainLevel.DIGEST_ATTRIBUTES) + "\n");
            }

            return false;
        } catch (Exception e) {
            System.err.println("[Calcite.compareRelNodesForEquivalence] Error: " + e.getMessage());
            return false;
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
    * - Inner-join children are treated as an unordered set (child digests sorted)
    * - All CASTs are recursively stripped from RexNode expressions before digesting
    * - Commutative and symmetric expressions (AND, OR, EQUALS, etc.) are normalized
    * - Filter conditions and expression strings are normalized via {@link #normalizeDigest(String)}
    * - Aggregates are normalized: groupSet sorted and aggregate calls sorted
    *   deterministically (by function name + input index, with DISTINCT tagged)
    * Projection order is preserved.
    *
    * Step-by-step summary:
    * 1) Safely traverse the plan using a path-set to avoid cycles.
    * 2) For Project: list output expressions in order, then recurse.
    * 3) For Filter: canonicalize the predicate, then recurse.
    * 4) For INNER Join: flatten nested inner joins, sort child digests, split/normalize/deduplicate/sort conjuncts, then build an order-insensitive join string.
    * 5) For non-INNER Join: keep left/right order and include normalized condition.
    * 6) For Union: flatten matching ALL/DISTINCT unions, sort child digests, build a stable representation.
    * 7) For Sort: ignore sort keys; include fetch/offset presence; then recurse.
    * 8) For Aggregate: sort group keys; format/sort aggregate calls deterministically; then recurse.
    * 9) For other nodes: emit normalized type plus canonicalized children.
    * 10) Expressions are canonicalized by stripping CASTs, sorting operands for commutative ops, making equality symmetric, normalizing inequality orientation, and replacing input refs with placeholders.
    *
    * @param rel The input RelNode
    * @return Canonical digest string
    */
    public static String canonicalDigest(RelNode rel) {
        // explicit null marker for consistency
        if (rel == null) return "null";
        Set<RelNode> path = Collections.newSetFromMap(new IdentityHashMap<>());
        return canonicalDigestInternal(rel, path);
    }

    private static String canonicalDigestInternal(RelNode rel, Set<RelNode> path) {
        if (rel == null) return "null";
        if (path.contains(rel)) {
            String typeName = rel.getRelTypeName();
            if (typeName == null) typeName = "UnknownRel";
            return typeName + "[...cycle...]";
        }
        path.add(rel);
        // Handle Project nodes: keep projection (output) expression order significant
        // because projection order affects result column positions and semantics.
        // Project: keep output order significant
        if (rel instanceof LogicalProject p) {
            StringBuilder sb = new StringBuilder();
            sb.append("Project[");
            for (int i = 0; i < p.getProjects().size(); i++) {
                RexNode rex = p.getProjects().get(i);
                if (i > 0) sb.append(",");
                sb.append(canonicalizeRex(rex));
            }
            sb.append("]->");
            sb.append(canonicalDigestInternal(p.getInput(), path));
            path.remove(rel);
            return sb.toString();
        }
        // Handle Filter: canonicalize the predicate expression, then recurse into input
        // Filter: normalize predicate and recurse
        if (rel instanceof LogicalFilter f) {
            String result = "Filter(" + canonicalizeRex(f.getCondition()) + ")->" + canonicalDigestInternal(f.getInput(), path);
            path.remove(rel);
            return result;
        }
        // Handle Join nodes. For INNER joins we treat children as an unordered
        // multiset (flatten nested inner joins, canonicalize child digests and conjuncts)
        // so that commutative re-orderings do not change the digest.
        if (rel instanceof LogicalJoin j) {
            if (j.getJoinType() == JoinRelType.INNER) {
                // Flatten nested inner joins into factors (leaf inputs) and collect conjunctive conditions
                List<RelNode> factors = new ArrayList<>();
                List<RexNode> conds = new ArrayList<>();
                collectInnerJoinFactors(j, factors, conds);

                // Canonicalize each factor and sort them for deterministic, order-insensitive representation
                List<String> factorDigests = new ArrayList<>();
                for (RelNode f : factors) {
                    factorDigests.add(canonicalDigestInternal(f, path));
                }
                factorDigests.sort(String::compareTo);

                // Canonicalize and normalize join conditions by decomposing ANDs, canonicalizing, deduplicating, and sorting
                List<String> condDigests = new ArrayList<>();
                for (RexNode c : conds) {
                    decomposeConj(c, condDigests);
                }
                // Remove duplicate conjuncts while preserving deterministic order
                condDigests = new ArrayList<>(new java.util.LinkedHashSet<>(condDigests));
                condDigests.sort(String::compareTo);
                String condPart = condDigests.isEmpty() ? "true" : String.join("&", condDigests);
                String result = "Join(INNER," + condPart + "){" + String.join("|", factorDigests) + "}";
                path.remove(rel);
                return result;
            } else {
                // For outer/semi/anti joins keep child ordering significant because semantics depend on side
                String condStr = canonicalizeRex(j.getCondition());
                String left = canonicalDigestInternal(j.getLeft(), path);
                String right = canonicalDigestInternal(j.getRight(), path);
                String result = "Join(" + j.getJoinType() + "," + condStr + "){" + left + "|" + right + "}";
                path.remove(rel);
                return result;
            }
        }
        // Set operations: normalize UNION/UNION ALL by flattening nested unions and
        // sorting child digests (associative + commutative under multiset semantics).
        if (rel instanceof Union u) {
            boolean all = u.all;
            List<RelNode> flatChildren = new ArrayList<>();
            flattenUnionInputs(u, all, flatChildren);
            List<String> childDigests = new ArrayList<>();
            for (RelNode in : flatChildren) childDigests.add(canonicalDigestInternal(in, path));
            childDigests.sort(String::compareTo);
            String result = "Union(" + (all ? "ALL" : "DISTINCT") + ")" + childDigests.toString();
            path.remove(rel);
            return result;
        }
        // Table scan: include fully qualified name
        if (rel instanceof TableScan ts) {
            String result = "Scan(" + String.join(".", ts.getTable().getQualifiedName()) + ")";
            path.remove(rel);
            return result;
        }
        // Sort/Limit: ignore sort keys, keep fetch/offset presence
        if (rel instanceof LogicalSort s) {
            String fetch = s.fetch == null ? "" : ("fetch=" + canonicalizeRex(s.fetch));
            String offset = s.offset == null ? "" : ("offset=" + canonicalizeRex(s.offset));
            String meta = (fetch + (fetch.isEmpty() || offset.isEmpty() ? "" : ",") + offset).trim();
            String head = meta.isEmpty() ? "Sort" : ("Sort(" + meta + ")");
            String result = head + "->" + canonicalDigestInternal(s.getInput(), path);
            path.remove(rel);
            return result;
        }
        // Aggregate: normalize groupSet and aggregate calls ordering
        if (rel instanceof LogicalAggregate agg) {
            java.util.List<Integer> groups = new java.util.ArrayList<>(agg.getGroupSet().asList());
            java.util.Collections.sort(groups);
            java.util.List<String> calls = new java.util.ArrayList<>();
            agg.getAggCallList().forEach(call -> {
                String func = call.getAggregation() == null ? "agg" : call.getAggregation().getName();
                int idx = -1;
                if (call.getArgList() != null && !call.getArgList().isEmpty()) {
                    idx = call.getArgList().get(0);
                }
                calls.add(func + "@" + idx + (call.isDistinct() ? ":DISTINCT" : ""));
            });
            java.util.Collections.sort(calls);
            String head = "Aggregate(groups=" + groups.toString() + ", calls=" + calls.toString() + ")";
            String result = head + "->" + canonicalDigestInternal(agg.getInput(), path);
            path.remove(rel);
            return result;
        }
        // Default: normalized string of this node with placeholders, plus canonical children
        String typeName = rel.getRelTypeName();
        if (typeName == null) typeName = "UnknownRel";
        String head = normalizeDigest(typeName);
        StringBuilder sb = new StringBuilder(head);
        sb.append("[");
        boolean first = true;
        for (RelNode in : rel.getInputs()) {
            // separate child digests
            if (!first) sb.append("|");
            sb.append(canonicalDigestInternal(in, path));
            first = false;
        }
        sb.append("]");
        String result = sb.toString();
        path.remove(rel);
        return result;
    }
    
    /**
    * Canonicalize a RexNode expression to a stable string for robust plan comparison.
    * - Recursively strips CASTs
    * - Sorts operands for commutative operators (AND, OR, PLUS, TIMES)
    * - Treats EQUALS/NOT_EQUALS as symmetric (order-insensitive)
    * - Applies normalizeDigest to reduce noise from input refs and spacing
     *
     * @param node The RexNode expression
     * @return Canonicalized string representation
     */
    private static String canonicalizeRex(RexNode node) {
        if (node == null) return "null";
        RexNode n = stripAllCasts(node);
        // Predicate normalization: for commutative boolean ops (AND/OR), we sort
        // operands during canonicalization so A AND B ≡ B AND A. This ensures
        // filter predicate order does not affect equivalence.
        // If this is a function/operator call, canonicalize based on operator kind
        if (n instanceof RexCall call && call.getOperator() != null) {
            SqlKind kind = call.getOperator().getKind();
            // Recursively canonicalize operands first
            List<String> parts = new ArrayList<>();
            for (RexNode op : call.getOperands()) parts.add(canonicalizeRex(op));

            switch (kind) {
                // Commutative containers: sort operands so order does not matter
                case AND, OR, PLUS, TIMES -> {
                    parts.sort(String::compareTo);
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }

                // Equality / inequality: treat as symmetric (order-insensitive)
                case EQUALS, NOT_EQUALS -> {
                    parts.sort(String::compareTo);
                    String opName = kind == SqlKind.EQUALS ? "=" : "<>";
                    return normalizeDigest(opName + "(" + String.join(",", parts) + ")");
                }

                // Ordering comparisons: normalize orientation so representation is stable
                case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    // Prefer '<' or '<=' form with the lexicographically smaller operand first
                    boolean flipToLess = (kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL);
                    String left = !parts.isEmpty() ? parts.get(0) : "?";
                    String right = parts.size() > 1 ? parts.get(1) : "?";
                    String finalOp;
                    if (flipToLess) {
                        // Map '>' to '<' by swapping operands
                        String tmp = left; left = right; right = tmp;
                        finalOp = (kind == SqlKind.GREATER_THAN) ? "<" : "<=";
                    } else {
                        finalOp = (kind == SqlKind.LESS_THAN) ? "<" : "<=";
                    }
                    // Ensure deterministic ordering by lexicographic comparison; swap if needed
                    int cmp = left.compareTo(right);
                    if (cmp > 0) {
                        String tmp = left; left = right; right = tmp;
                    }
                    return normalizeDigest(finalOp + "(" + left + "," + right + ")");
                }

                case CAST -> {
                    // CASTs are expected to be stripped earlier; if present, return the operand's canonical form
                    if (!call.getOperands().isEmpty()) return canonicalizeRex(call.getOperands().get(0));
                    return "CAST(?)";
                }

                default -> {
                    // Generic operator/function: include operator kind and canonicalized operands
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
            }
        }
        return normalizeDigest(n.toString());
    }

    // Helper: collect factors and conditions from a nested INNER join tree
    private static void collectInnerJoinFactors(RelNode node, List<RelNode> factors, List<RexNode> conds) {
        // Unwrap trivial projection (all input refs) to reach a deeper join
        if (node instanceof LogicalProject p) {
            boolean onlyRefs = true;
            for (RexNode e : p.getProjects()) {
                if (!(e instanceof RexInputRef)) { onlyRefs = false; break; }
            }
            if (onlyRefs) {
                collectInnerJoinFactors(p.getInput(), factors, conds);
                return;
            }
        }
        if (node instanceof LogicalJoin j && j.getJoinType() == JoinRelType.INNER) {
            if (j.getCondition() != null) {
                conds.add(j.getCondition());
            }
            collectInnerJoinFactors(j.getLeft(), factors, conds);
            collectInnerJoinFactors(j.getRight(), factors, conds);
        } else {
            factors.add(node);
        }
    }

    // Helper: decompose a condition into conjuncts, canonicalize each, and add to output
    private static void decomposeConj(RexNode cond, List<String> out) {
        if (cond == null) return;
        RexNode n = stripAllCasts(cond);
        if (n instanceof RexCall call && call.getOperator() != null && call.getOperator().getKind() == SqlKind.AND) {
            for (RexNode op : call.getOperands()) decomposeConj(op, out);
        } else {
            String s = canonicalizeRex(n);
            // drop no-op conjuncts
            if ("true".equalsIgnoreCase(s)) return;
            out.add(s);
        }
    }
    /**
     * Recursively strip all CASTs from a RexNode tree.
     *
     * @param node The RexNode expression
     * @return RexNode with all CASTs removed
     */
    private static RexNode stripAllCasts(RexNode node) {
        // Recursively remove any CAST operations to avoid artificial differences in types
        // that do not affect logical equivalence (e.g., CAST(INT AS BIGINT) in harmless contexts).
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
                // If operands changed (e.g., CAST stripped from subexpressions), rebuild the call node
                // with the same operator and the new operand list so canonicalization is consistent.
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
    public static void printRelTrees(String sql1, String sql2) {
        CalciteUtil.printRelTrees(sql1, sql2);
    }

    /**
     * Convert a Calcite RelNode graph into a simple tree of {@link RelTreeNode}
     * using a RelVisitor. Children are kept in input order.
     */
    public static RelTreeNode buildRelTree(RelNode rel) {
        if (rel == null) return null;

        // map physical nodes to constructed tree nodes
        final Map<RelNode, RelTreeNode> built = new IdentityHashMap<>();
        // simple holder for root reference
        final RelTreeNode[] rootHolder = new RelTreeNode[1];

        RelVisitor builder = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                // create tree node label for this RelNode
                RelTreeNode cur = new RelTreeNode(summarizeNode(node));
                // remember mapping
                built.put(node, cur);
                if (parent == null) {
                    // this is the root
                    rootHolder[0] = cur;
                } else {
                    // find parent's tree node
                    RelTreeNode p = built.get(parent);
                    if (p == null) {
                        // In rare cases, if the parent hasn't been recorded yet, create and link it.
                        p = new RelTreeNode(summarizeNode(parent));
                        built.put(parent, p);
                        // initialize root if needed
                        if (rootHolder[0] == null) rootHolder[0] = p;
                    }
                    // link child in input order
                    p.addChild(cur);
                }
                // recurse on children
                super.visit(node, ordinal, parent);
            }
        };

        // perform traversal/build
        builder.go(rel);
        // return root of constructed tree
        return rootHolder[0];
    }

    /**
     * Create a concise per-node label suitable for a tree display.
     *
     * @param rel The RelNode
     * @return String label for the node
     */
    private static String summarizeNode(RelNode rel) {
        if (rel == null) return "(null)";
        // show project expressions in order
        if (rel instanceof LogicalProject p) {
            List<String> exprs = new ArrayList<>();
            for (RexNode rex : p.getProjects()) {
                exprs.add(canonicalizeRex(rex));
            }
            return "Project[" + String.join(",", exprs) + "]";
        }
        // show normalized filter predicate
        if (rel instanceof LogicalFilter f) {
            return "Filter(" + canonicalizeRex(f.getCondition()) + ")";
        }
        // include join type and normalized condition
        if (rel instanceof LogicalJoin j) {
            return "Join(" + j.getJoinType() + "," + canonicalizeRex(j.getCondition()) + ")";
        }
        // compact label for sort/limit in tree view
        if (rel instanceof LogicalSort s) {
            String fetch = s.fetch == null ? "" : ("fetch=" + canonicalizeRex(s.fetch));
            String offset = s.offset == null ? "" : ("offset=" + canonicalizeRex(s.offset));
            String meta = (fetch + (fetch.isEmpty() || offset.isEmpty() ? "" : ",") + offset).trim();
            return meta.isEmpty() ? "Sort" : ("Sort(" + meta + ")");
        }
        // fully qualified table name
        if (rel instanceof TableScan ts) {
            return "Scan(" + String.join(".", ts.getTable().getQualifiedName()) + ")";
        }
        // fallback: node type
        return rel.getRelTypeName();
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
            // strict order-sensitive equality
            return a.equals(b);
        }
        if (a == b) return true;
        if (a == null || b == null) return false;
        // order-insensitive via canonical form
        return a.canonicalDigest().equals(b.canonicalDigest());
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

    // Registry mapping lowercase transformation keys to rule-adder consumers.
    private static final Map<String, Consumer<HepProgramBuilder>> RULE_MAP = new HashMap<>();
    static {
        RULE_MAP.put("aggregateexpanddistinctaggregatesrule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES));
        RULE_MAP.put("aggregateextractprojectrule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES));
        RULE_MAP.put("aggregatefiltertocaserule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_CASE_TO_FILTER));
        RULE_MAP.put("aggregatefiltertransposerule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_FILTER_TRANSPOSE));
        RULE_MAP.put("aggregatejoinjoinremoverule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_JOIN_JOIN_REMOVE));
        RULE_MAP.put("aggregatejoinremoverule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_JOIN_REMOVE));
        RULE_MAP.put("aggregatejointransposerule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE));
        RULE_MAP.put("aggregatemergerule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_MERGE));
        RULE_MAP.put("aggregateprojectmergerule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE));
        RULE_MAP.put("aggregateprojectpullupconstantsrule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS));
        RULE_MAP.put("aggregateprojectstartablerule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_PROJECT_STAR_TABLE));
        RULE_MAP.put("aggregatereducefunctionsrule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS));
        RULE_MAP.put("aggregateremoverule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_REMOVE));
        RULE_MAP.put("aggregatestartablerule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_STAR_TABLE));
        RULE_MAP.put("aggregateunionaggregaterule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_UNION_AGGREGATE));
        RULE_MAP.put("aggregateuniontransposerule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_UNION_TRANSPOSE));
        RULE_MAP.put("aggregatevaluesrule", pb -> pb.addRuleInstance(CoreRules.AGGREGATE_VALUES));
        RULE_MAP.put("calcmergerule", pb -> pb.addRuleInstance(CoreRules.CALC_MERGE));
        RULE_MAP.put("calcremoverule", pb -> pb.addRuleInstance(CoreRules.CALC_REMOVE));
        RULE_MAP.put("calcsplitrule", pb -> pb.addRuleInstance(CoreRules.CALC_SPLIT));
        RULE_MAP.put("filteraggregatetransposerule", pb -> pb.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE));
        RULE_MAP.put("filtercalcmergerule", pb -> pb.addRuleInstance(CoreRules.FILTER_CALC_MERGE));
        RULE_MAP.put("filtercorrelaterule", pb -> pb.addRuleInstance(CoreRules.FILTER_CORRELATE));
        RULE_MAP.put("filterjoinrule.filterintojoinrule", pb -> pb.addRuleInstance(CoreRules.FILTER_INTO_JOIN));
        RULE_MAP.put("filterjoinrule.joinconditionpushrule", pb -> pb.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH));
        RULE_MAP.put("filtermergerule", pb -> pb.addRuleInstance(CoreRules.FILTER_MERGE));
        RULE_MAP.put("filtermultijoinmergerule", pb -> pb.addRuleInstance(CoreRules.FILTER_MULTI_JOIN_MERGE));
        RULE_MAP.put("filterprojecttransposerule", pb -> pb.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE));
        RULE_MAP.put("filtersampletransposerule", pb -> pb.addRuleInstance(CoreRules.FILTER_SAMPLE_TRANSPOSE));
        RULE_MAP.put("filtersetoptransposerule", pb -> pb.addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE));
        RULE_MAP.put("filtertablefunctiontransposerule", pb -> pb.addRuleInstance(CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE));
        RULE_MAP.put("filtertocalcrule", pb -> pb.addRuleInstance(CoreRules.FILTER_TO_CALC));
        RULE_MAP.put("filterwindowtransposerule", pb -> pb.addRuleInstance(CoreRules.FILTER_WINDOW_TRANSPOSE));
        RULE_MAP.put("joinaddredundantsemijoinrule", pb -> pb.addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN));
        RULE_MAP.put("joinassociaterule", pb -> pb.addRuleInstance(CoreRules.JOIN_ASSOCIATE));
        RULE_MAP.put("joincommuterule", pb -> pb.addRuleInstance(CoreRules.JOIN_COMMUTE));
        RULE_MAP.put("joinderiveisnotnullfilterrule", pb -> pb.addRuleInstance(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE));
        RULE_MAP.put("joinextractfilterrule", pb -> pb.addRuleInstance(CoreRules.JOIN_EXTRACT_FILTER));
        RULE_MAP.put("joinprojectbothtransposerule", pb -> pb.addRuleInstance(CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE));
        RULE_MAP.put("joinprojectlefttransposerule", pb -> pb.addRuleInstance(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE));
        RULE_MAP.put("joinprojectrighttransposerule", pb -> pb.addRuleInstance(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE));
        RULE_MAP.put("joinpushexpressionsrule", pb -> pb.addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS));
        RULE_MAP.put("joinpushtransitivepredicatesrule", pb -> pb.addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES));
        RULE_MAP.put("jointocorrelaterule", pb -> pb.addRuleInstance(CoreRules.JOIN_TO_CORRELATE));
        RULE_MAP.put("jointomultijoinrule", pb -> pb.addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN));
        RULE_MAP.put("joinleftuniontransposerule", pb -> pb.addRuleInstance(CoreRules.JOIN_LEFT_UNION_TRANSPOSE));
        RULE_MAP.put("joinrightuniontransposerule", pb -> pb.addRuleInstance(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE));
        RULE_MAP.put("minusmergerule", pb -> pb.addRuleInstance(CoreRules.MINUS_MERGE));
        RULE_MAP.put("minustodistinctrule", pb -> pb.addRuleInstance(CoreRules.MINUS_TO_DISTINCT));
        RULE_MAP.put("projectaggregatemergerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_AGGREGATE_MERGE));
        RULE_MAP.put("projectcalcmergerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_CALC_MERGE));
        RULE_MAP.put("projectcorrelatetransposerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_CORRELATE_TRANSPOSE));
        RULE_MAP.put("projectfiltertransposerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE));
        RULE_MAP.put("projectjoinjoinremoverule", pb -> pb.addRuleInstance(CoreRules.PROJECT_JOIN_JOIN_REMOVE));
        RULE_MAP.put("projectjoinremoverule", pb -> pb.addRuleInstance(CoreRules.PROJECT_JOIN_REMOVE));
        RULE_MAP.put("projectjointransposerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE));
        RULE_MAP.put("projectmergerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_MERGE));
        RULE_MAP.put("projectmultijoinmergerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_MULTI_JOIN_MERGE));
        RULE_MAP.put("projectremoverule", pb -> pb.addRuleInstance(CoreRules.PROJECT_REMOVE));
        RULE_MAP.put("projectsetoptransposerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE));
        RULE_MAP.put("projecttocalcrule", pb -> pb.addRuleInstance(CoreRules.PROJECT_TO_CALC));
        RULE_MAP.put("projecttowindowrule", pb -> pb.addRuleInstance(CoreRules.PROJECT_WINDOW_TRANSPOSE));
        RULE_MAP.put("projecttowindowrule.calctowindowrule", pb -> pb.addRuleInstance(CoreRules.CALC_TO_WINDOW));
        RULE_MAP.put("projecttowindowrule.projecttologicalprojectandwindowrule", pb -> pb.addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW));
        RULE_MAP.put("projectwindowtransposerule", pb -> pb.addRuleInstance(CoreRules.PROJECT_WINDOW_TRANSPOSE));
        RULE_MAP.put("reducedecimalsrule", pb -> pb.addRuleInstance(CoreRules.CALC_REDUCE_DECIMALS));
        RULE_MAP.put("reduceexpressionsrule.calcreduceexpressionsrule", pb -> pb.addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS));
        RULE_MAP.put("reduceexpressionsrule.filterreduceexpressionsrule", pb -> pb.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS));
        RULE_MAP.put("reduceexpressionsrule.joinreduceexpressionsrule", pb -> pb.addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS));
        RULE_MAP.put("reduceexpressionsrule.projectreduceexpressionsrule", pb -> pb.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS));
        RULE_MAP.put("reduceexpressionsrule.windowreduceexpressionsrule", pb -> pb.addRuleInstance(CoreRules.WINDOW_REDUCE_EXPRESSIONS));
        RULE_MAP.put("semijoinfiltertransposerule", pb -> pb.addRuleInstance(CoreRules.SEMI_JOIN_FILTER_TRANSPOSE));
        RULE_MAP.put("semijoinjointransposerule", pb -> pb.addRuleInstance(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE));
        RULE_MAP.put("semijoinprojecttransposerule", pb -> pb.addRuleInstance(CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE));
        RULE_MAP.put("semijoinremoverule", pb -> pb.addRuleInstance(CoreRules.SEMI_JOIN_REMOVE));
        RULE_MAP.put("semijoinrule.joinonuniquetosemijoinrule", pb -> pb.addRuleInstance(CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN));
        RULE_MAP.put("semijoinrule.jointosemijoinrule", pb -> pb.addRuleInstance(CoreRules.JOIN_TO_SEMI_JOIN));
        RULE_MAP.put("semijoinrule.projecttosemijoinrule", pb -> pb.addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN));
        RULE_MAP.put("sortjoincopyrule", pb -> pb.addRuleInstance(CoreRules.SORT_JOIN_COPY));
        RULE_MAP.put("sortjointransposerule", pb -> pb.addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE));
        RULE_MAP.put("sortprojecttransposerule", pb -> pb.addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE));
        RULE_MAP.put("sortremoveconstantkeysrule", pb -> pb.addRuleInstance(CoreRules.SORT_REMOVE_CONSTANT_KEYS));
        RULE_MAP.put("sortremoveredundantrule", pb -> pb.addRuleInstance(CoreRules.SORT_REMOVE_REDUNDANT));
        RULE_MAP.put("sortremoverule", pb -> pb.addRuleInstance(CoreRules.SORT_REMOVE));
        RULE_MAP.put("sortuniontransposerule", pb -> pb.addRuleInstance(CoreRules.SORT_UNION_TRANSPOSE));
        RULE_MAP.put("unionmergerule", pb -> pb.addRuleInstance(CoreRules.UNION_MERGE));
        RULE_MAP.put("unionpullupconstantsrule", pb -> pb.addRuleInstance(CoreRules.UNION_PULL_UP_CONSTANTS));
        RULE_MAP.put("uniontodistrictrule", pb -> pb.addRuleInstance(CoreRules.UNION_TO_DISTINCT));
        RULE_MAP.put("coerceinputsrule", pb -> pb.addRuleInstance(CoreRules.COERCE_INPUTS));
        RULE_MAP.put("exchangeremoveconstantkeysrule", pb -> pb.addRuleInstance(CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS));
        RULE_MAP.put("intersecttodistrictrule", pb -> pb.addRuleInstance(CoreRules.INTERSECT_TO_DISTINCT));
        RULE_MAP.put("matchrule", pb -> pb.addRuleInstance(CoreRules.MATCH));
        RULE_MAP.put("multijoinoptimizebushyrule", pb -> pb.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY));
        RULE_MAP.put("sampletofilterrule", pb -> pb.addRuleInstance(CoreRules.SAMPLE_TO_FILTER));
        RULE_MAP.put("tablescanrule", pb -> pb.addRuleInstance(CoreRules.PROJECT_TABLE_SCAN));
    }

    // (Removed) stripTopLevelCasts: superseded by stripAllCasts which handles recursive CAST removal

    /**
     * Apply an explicit list of HepPlanner CoreRules (by name) to a plan, producing a
     * new {@link RelNode}. This is useful for experimenting with how particular rules
     * affect normalization and equivalence.
     *
    * Supported rule name keys map to Calcite's {@link CoreRules}. Unknown names are ignored.
     *
     * @param rel The input RelNode
     * @param transformations List of rule names to apply (case-insensitive)
     * @return Transformed RelNode
     */
    public static RelNode applyTransformations(RelNode rel, List<String> transformations)
    {
        RelNode newRel = rel;

        // Build a composite Hep program containing all requested rules and
        // apply them together to reach a better fixpoint than one-by-one.
        List<Consumer<HepProgramBuilder>> ruleAdders = new ArrayList<>();
        for (String transform : transformations) {
            String key = transform == null ? "" : transform.trim().toLowerCase();
            Consumer<HepProgramBuilder> adder = RULE_MAP.get(key);
            if (adder != null) ruleAdders.add(adder);
        }
        if (!ruleAdders.isEmpty()) {
            // Optional: iterate a few times to ensure convergence if rules enable each other
            for (int pass = 0; pass < 3; pass++) {
                String before = RelOptUtil.toString(newRel, SqlExplainLevel.DIGEST_ATTRIBUTES);
                HepProgramBuilder pb = new HepProgramBuilder();
                pb.addMatchOrder(HepMatchOrder.TOP_DOWN);
                pb.addMatchLimit(1000);
                for (Consumer<HepProgramBuilder> adder : ruleAdders) adder.accept(pb);
                HepPlanner planner = new HepPlanner(pb.build());
                planner.setRoot(newRel);
                RelNode result = planner.findBestExp();
                String after = RelOptUtil.toString(result, SqlExplainLevel.DIGEST_ATTRIBUTES);
                newRel = result;
                // reached fixpoint
                if (before.equals(after)) break;
            }
        }

        //System.out.println("RelTree after transformations: \n" + buildRelTree(newRel).toString());

        return newRel;
    }

    /**
     * Remove scalar/sub-query expressions and decorrelate correlates to normalized joins/aggregates.
     * This helps align logically equivalent forms where one side uses SCALAR_QUERY with correlation and
     * the other side uses a join + aggregate after decorrelation.
        *
        * Notes:
        * - This method is best-effort: decorrelation can fail or be inapplicable for some plans, in which case
        *   the original plan is returned unchanged.
        * - It is invoked symmetrically on both sides prior to comparison to improve chance of alignment
        *   between scalar-subquery and join+aggregate representations.
     */
    private static RelNode normalizeSubqueriesAndDecorrelate(RelNode rel) {
        if (rel == null) return null;

        RelNode cur = rel;
        // First, apply sub-query removal rules to convert RexSubQuery into relational operators (often Correlate/Join+Agg)
        HepProgramBuilder subq = new HepProgramBuilder();
        subq.addMatchOrder(HepMatchOrder.TOP_DOWN);
        subq.addMatchLimit(1000);
        // Convert RexSubQuery to correlates/joins
        subq.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        subq.addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
        subq.addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        // Clean up after rewrites
        subq.addRuleInstance(CoreRules.PROJECT_MERGE);
        subq.addRuleInstance(CoreRules.PROJECT_REMOVE);
        subq.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        HepPlanner hp = new HepPlanner(subq.build());
        hp.setRoot(cur);
        cur = hp.findBestExp();

        // Then, decorrelate to transform LogicalCorrelate into joins when possible
        try {
            cur = RelDecorrelator.decorrelateQuery(cur);
        } catch (Throwable t) {
            // Best-effort: if decorrelation not applicable, keep the current plan
        }

        // Optional additional cleanup (harmless if not needed)
        HepProgramBuilder cleanup = new HepProgramBuilder();
        cleanup.addMatchOrder(HepMatchOrder.TOP_DOWN);
        cleanup.addMatchLimit(1000);
        cleanup.addRuleInstance(CoreRules.FILTER_CORRELATE);
        cleanup.addRuleInstance(CoreRules.PROJECT_CORRELATE_TRANSPOSE);
        cleanup.addRuleInstance(CoreRules.PROJECT_MERGE);
        cleanup.addRuleInstance(CoreRules.PROJECT_REMOVE);
        cleanup.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        HepPlanner hp2 = new HepPlanner(cleanup.build());
        hp2.setRoot(cur);
        cur = hp2.findBestExp();

        return cur;
    }

    /**
     * Convert a {@link RelNode} to a SQL string and retrieve its cleaned PostgreSQL
     * execution plan as pretty‑printed JSON.
     *
     * Implementation details:
     * - Uses Calcite's {@link RelToSqlConverter} with {@link PostgresqlSqlDialect}
     *   to render a SQL statement from the relational plan.
     * - Delegates to {@link GetQueryPlans#getCleanedQueryPlanJSONasString(String)}
     *   to run EXPLAIN (FORMAT JSON, BUFFERS) and strip execution‑specific fields.
     *
     * Error handling:
     * - Any exception during plan retrieval is caught; a brief message is written
     *   to {@code System.err} and this method returns {@code null} so callers can
     *   degrade gracefully.
     *
     * @param rel The relational plan to describe
     * @return Cleaned, pretty‑printed JSON plan string, or {@code null} on failure
     */
    public static String convertRelNodetoJSONQueryPlan(RelNode rel)
    {
        if (rel == null) return "null";
        RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
        RelToSqlConverter.Result res = converter.visitRoot(rel);
        String sql = res.asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
        // Get query plan: protect external call with try-catch to avoid bubbling
        try {
            return GetQueryPlans.getCleanedQueryPlanJSONasString(sql);
        } catch (SQLException  e) {
            System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] Error while obtaining plan: " + e.getMessage());
            return null;
        }
    }
}
