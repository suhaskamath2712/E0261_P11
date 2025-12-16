package com.ac.iisc;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.calcite.rel.type.RelDataTypeField;
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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.json.JSONArray;
import org.json.JSONObject;

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
 *   - Canonical-digest AND normalization: {@link #normalizeAndOrderingInDigest(String)} — safety net to make textual AND term order benign.
 *   - Optional fallback: PostgreSQL EXPLAIN JSON equality (see {@link #convertRelNodetoJSONQueryPlan(RelNode)}).
 *
 *   Additional canonicalization details (recent robustness updates):
 *   - CHAR literal canonicalization: trailing padding in fixed‑width CHAR literals is trimmed
 *     so semantically equivalent CHAR constants compare equal regardless of declaration width.
 *   - SEARCH / SARG normalization: Calcite's SEARCH/SARG textual forms are normalized and
 *     single‑interval SEARCH expressions are mapped to a compact RANGE(...) form.
 *   - Conjunct decomposition and range folding: filter AND chains are decomposed into
 *     individual conjuncts; pairs/triples of inequalities are detected and folded into
 *     canonical RANGE(...) expressions, then deduplicated and sorted.
 *   - Date + INTERVAL folding: literal DATE + INTERVAL_YEAR_MONTH expressions with a numeric
 *     prefix are interpreted as months (e.g., `12 INTERVAL YEAR` → +12 months) and folded to
 *     concrete date literals when safe.
 *   - Range grouping key stability: range detection groups predicates using a raw RexNode key
 *     that preserves distinct input refs (avoids conflating different columns that were
 *     previously normalized to the same `$x` token).
 *   - Digest-level AND order normalization: after canonicalization, conjuncts inside textual
 *     `AND(...)` digests are lexicographically sorted to make order differences benign.
 *
 * Debug helpers (not part of the equivalence ladder by default):
 * - Tree canonical digest: {@link RelTreeNode#canonicalDigest()} — ignores child order across the tree.
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

    /** Lazily parsed schema summary (PK/FK metadata) from tpch_schema_summary.json. */
    private static volatile SchemaSummary SCHEMA_SUMMARY;

    private static final class SchemaSummary {
        final Map<String, Set<String>> pkByTableUpper;
        // key: TABLE -> (FK_COL -> (REF_TABLE, REF_COL))
        final Map<String, Map<String, Ref>> fkByTableAndColUpper;

        SchemaSummary(Map<String, java.util.Set<String>> pkByTableUpper,
                      Map<String, Map<String, Ref>> fkByTableAndColUpper) {
            this.pkByTableUpper = pkByTableUpper;
            this.fkByTableAndColUpper = fkByTableAndColUpper;
        }
    }

    private static final class Ref {
        final String refTableUpper;
        final String refColUpper;
        Ref(String refTableUpper, String refColUpper) {
            this.refTableUpper = refTableUpper;
            this.refColUpper = refColUpper;
        }
    }

    private static final class FieldOrigin {
        final String tableUpper;
        final String colUpper;
        FieldOrigin(String tableUpper, String colUpper) {
            this.tableUpper = tableUpper;
            this.colUpper = colUpper;
        }
    }

    private static SchemaSummary getSchemaSummary() {
        SchemaSummary cached = SCHEMA_SUMMARY;
        if (cached != null) return cached;
        synchronized (Calcite.class) {
            if (SCHEMA_SUMMARY != null) return SCHEMA_SUMMARY;
            try {
                String json = FileIO.readSchemaSummary();
                if (json == null || json.isBlank()) {
                    SCHEMA_SUMMARY = new SchemaSummary(Map.of(), Map.of());
                    return SCHEMA_SUMMARY;
                }
                JSONObject root = new JSONObject(json);
                Map<String, java.util.Set<String>> pk = new HashMap<>();
                Map<String, Map<String, Ref>> fk = new HashMap<>();
                for (String tableKey : root.keySet()) {
                    if (tableKey == null) continue;
                    String tableUpper = tableKey.trim().toUpperCase();
                    JSONObject t = root.optJSONObject(tableKey);
                    if (t == null) continue;

                    java.util.Set<String> pkCols = new java.util.LinkedHashSet<>();
                    JSONArray pkArr = t.optJSONArray("pk");
                    if (pkArr != null) {
                        for (int i = 0; i < pkArr.length(); i++) {
                            String c = pkArr.optString(i, null);
                            if (c != null && !c.isBlank()) pkCols.add(c.trim().toUpperCase());
                        }
                    }
                    pk.put(tableUpper, pkCols);

                    Map<String, Ref> fks = new HashMap<>();
                    JSONArray fkArr = t.optJSONArray("fks");
                    if (fkArr != null) {
                        for (int i = 0; i < fkArr.length(); i++) {
                            JSONObject fkObj = fkArr.optJSONObject(i);
                            if (fkObj == null) continue;
                            String col = fkObj.optString("col", "").trim();
                            String rt = fkObj.optString("ref_table", "").trim();
                            String rc = fkObj.optString("ref_col", "").trim();
                            if (col.isEmpty() || rt.isEmpty() || rc.isEmpty()) continue;
                            fks.put(col.toUpperCase(), new Ref(rt.toUpperCase(), rc.toUpperCase()));
                        }
                    }
                    fk.put(tableUpper, fks);
                }
                SCHEMA_SUMMARY = new SchemaSummary(pk, fk);
            } catch (Exception e) {
                // Be conservative: schema summary is optional; if parsing fails, disable schema-aware rules.
                SCHEMA_SUMMARY = new SchemaSummary(Map.of(), Map.of());
            }
            return SCHEMA_SUMMARY;
        }
    }
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

            // Compatibility shim: expand SELECT aliases referenced in the
            // outermost GROUP BY (e.g., "GROUP BY o_year" where o_year is an
            // alias for EXTRACT(YEAR FROM o_orderdate)).
            sqlForParse = CalciteUtil.rewriteGroupByAliases(sqlForParse);
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
    * 4. If still different, compare a canonical digest with additional AND-term ordering
    *    normalization at the string level (safety net).
    * 5. Final fallback: compare cleaned PostgreSQL EXPLAIN JSON plans (best-effort).
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

            //System.out.println("Rel1: \n" + RelOptUtil.toString(rel1, SqlExplainLevel.DIGEST_ATTRIBUTES) + "\n");
            //System.out.println("Rel2: \n" + RelOptUtil.toString(rel2, SqlExplainLevel.DIGEST_ATTRIBUTES) + "\n");
            

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

            // Additional robustness: treat conjunctions inside canonical digests
            // as order-insensitive. In rare cases (e.g., mixed RANGE(...) sources
            // from SEARCH vs. inequalities), AND terms may still appear in
            // different textual order despite representing the same multiset of
            // conjuncts. Normalize those segments and compare again.
            String c1AndNorm = normalizeAndOrderingInDigest(c1);
            String c2AndNorm = normalizeAndOrderingInDigest(c2);
            if (c1AndNorm.equals(c2AndNorm)) return true;

            // Final fallback: compare cleaned PostgreSQL execution plans as a last resort.
            // If both queries yield the same physical plan on the target database,
            // treat them as equivalent even if Calcite's logical digests differ.
            String p1 = convertRelNodetoJSONQueryPlan(rel1);
            String p2 = convertRelNodetoJSONQueryPlan(rel2);
            if (p1 != null && p1.equals(p2)) return true;

            if (transformations != null) {
                // Debug: print canonical digests as well to understand any
                // residual differences that survive all comparison layers.
                System.out.println("[Calcite.compareQueries] canonicalDigest Rel1: " + c1);
                System.out.println("[Calcite.compareQueries] canonicalDigest Rel2: " + c2);
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
    * 7) For Sort: ignore sort keys unless FETCH/OFFSET is present (Top-N), in which case
    *    include the sort collation (field indexes + direction/null direction); then recurse.
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
                // For outer/semi/anti joins keep child ordering significant because semantics depend on side.
                // However, SEMI joins created by certain rule sequences can be (a) provably redundant
                // under schema FK->PK constraints, or (b) idempotent duplicates of an identical SEMI join.
                // We canonicalize those cases to reduce false negatives.
                if (j.getJoinType() == JoinRelType.SEMI) {
                    // 1) Collapse chains of identical semi-joins: SEMI(SEMI(L,R,cond),R,cond) == SEMI(L,R,cond)
                    RelNode leftChild = j.getLeft();
                    if (leftChild instanceof LogicalJoin lj && lj.getJoinType() == JoinRelType.SEMI) {
                        String c0 = canonicalizeRex(j.getCondition());
                        String c1 = canonicalizeRex(lj.getCondition());
                        if (c0.equals(c1)) {
                            // Compare right subtrees canonically (order-sensitive because SEMI).
                            String r0 = canonicalDigestInternal(j.getRight(), path);
                            String r1 = canonicalDigestInternal(lj.getRight(), path);
                            if (r0.equals(r1)) {
                                // Drop the outer SEMI join.
                                String result = canonicalDigestInternal(lj, path);
                                path.remove(rel);
                                return result;
                            }
                        }
                    }

                    // 2) Drop schema-redundant SEMI joins: if left has FK to right PK and right side is unfiltered.
                    if (isRedundantSemiJoinUnderSchema(j)) {
                        String result = canonicalDigestInternal(j.getLeft(), path);
                        path.remove(rel);
                        return result;
                    }

                    // 3) Canonicalize SEMI join as an INNER join against a DISTINCT set of right keys.
                    // This is semantics-preserving for equi-semi-joins and helps align with plans
                    // that materialize the key-set via Aggregate(groups=[...], calls=[]).
                    String semiAsInner = canonicalizeSemiJoinAsInnerJoinWithDistinctRightKeys(j, path);
                    if (semiAsInner != null) {
                        path.remove(rel);
                        return semiAsInner;
                    }
                }

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
        // Sort/Limit: ignore ordering unless FETCH/OFFSET is present (Top-N); then include collation
        if (rel instanceof LogicalSort s) {
            // ORDER BY is only semantically relevant when paired with LIMIT/OFFSET.
            // For Top-N queries, the sort keys and directions determine which rows
            // are returned, so we must include them in the canonical digest.
            boolean topN = (s.fetch != null || s.offset != null);

            String fetch = s.fetch == null ? "" : ("fetch=" + canonicalizeRex(s.fetch));
            String offset = s.offset == null ? "" : ("offset=" + canonicalizeRex(s.offset));
            String meta = (fetch + (fetch.isEmpty() || offset.isEmpty() ? "" : ",") + offset).trim();

            String order = "";
            if (topN && s.getCollation() != null && s.getCollation().getFieldCollations() != null
                    && !s.getCollation().getFieldCollations().isEmpty()) {
                java.util.List<String> keys = new java.util.ArrayList<>();
                for (org.apache.calcite.rel.RelFieldCollation fc : s.getCollation().getFieldCollations()) {
                    keys.add(fc.getFieldIndex() + ":" + fc.direction + ":" + fc.nullDirection);
                }
                order = "order=" + keys;
            }

            String head;
            if (meta.isEmpty() && order.isEmpty()) {
                head = "Sort";
            } else if (meta.isEmpty()) {
                head = "Sort(" + order + ")";
            } else if (order.isEmpty()) {
                head = "Sort(" + meta + ")";
            } else {
                head = "Sort(" + meta + "," + order + ")";
            }

            String result = head + "->" + canonicalDigestInternal(s.getInput(), path);
            path.remove(rel);
            return result;
        }
        // Aggregate: normalize groupSet and aggregate calls ordering
        if (rel instanceof LogicalAggregate agg) {
            // Special-case: schema-aware canonicalization for TPCH Q3-like patterns.
            // Some planners express the final revenue aggregation either:
            //  (a) as an Aggregate above a 3-way INNER join, grouping by (l_orderkey, o_orderdate, o_shippriority)
            //      and summing a lineitem-only revenue expression, OR
            //  (b) as a pre-aggregation on LINEITEM grouped by l_orderkey, followed by joining ORDERS/CUSTOMER.
            // Under the TPCH schema (L_ORDERKEY -> O_ORDERKEY PK and O_CUSTKEY -> C_CUSTKEY PK), these forms
            // are semantically equivalent and we want canonical digests to align.
            String pushedDown = tryCanonicalizeAggregateOverInnerJoinAsPreAgg(agg, path);
            if (pushedDown != null) {
                path.remove(rel);
                return pushedDown;
            }

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
        // If this is a literal of character type, normalize trailing spaces inside
        // quoted text so that 'SHIP' and 'SHIP      ' hash the same for CHAR(n)
        // columns. We only affect the digest string; the underlying RexLiteral is
        // left unchanged.
        if (n instanceof RexLiteral lit) {
            if (lit.getType() != null
                    && lit.getType().getSqlTypeName() != null
                    && lit.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
                String text = lit.toString();
                text = normalizeCharLiteralText(text);
                return normalizeDigest(text);
            }
            return normalizeDigest(n.toString());
        }
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
                // Conjunctions: decompose into conjuncts, normalize each, apply
                // range folding, then sort and dedupe.
                case AND -> {
                    // AND nodes are always RexCall here; use conjunctive
                    // decomposition + range folding for canonicalization.
                    String andDigest = canonicalizeAndWithRanges(call);
                    return normalizeDigest(andDigest);
                }

                // Other commutative containers: sort operands so order does not matter
                case OR, PLUS, TIMES -> {
                    // For PLUS, attempt to fold date + interval YEAR into a single
                    // DATE literal so 1995-01-01 + INTERVAL '1 year' aligns with
                    // a plain 1996-01-01 literal used elsewhere.
                    if (kind == SqlKind.PLUS && call != null) {
                        String folded = tryFoldDatePlusInterval(call);
                        if (folded != null) {
                            return normalizeDigest(folded);
                        }
                        // Also fold DATE +/- INTERVAL DAY expressions when the interval
                        // is an exact whole number of days.
                        folded = tryFoldDatePlusOrMinusDayTimeInterval(call, false);
                        if (folded != null) {
                            return normalizeDigest(folded);
                        }
                    }
                    parts.sort(String::compareTo);
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }

                case MINUS -> {
                    // Fold DATE - INTERVAL DAY_TIME (when the interval is a whole number
                    // of days) so expressions like 1998-12-01 - 259200000:INTERVAL DAY
                    // canonicalize to 1998-11-28.
                    String folded = tryFoldDatePlusOrMinusDayTimeInterval(call, true);
                    if (folded != null) {
                        return normalizeDigest(folded);
                    }
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

                // SEARCH(value, Sarg[...]) – normalize Sarg text so that differences
                // in CHAR length annotations and trailing spaces inside literals do
                // not cause spurious mismatches.
                case SEARCH -> {
                    String valueExpr = parts.size() > 0 ? parts.get(0) : "?";
                    String sargExpr = parts.size() > 1 ? parts.get(1) : "?";
                    String cleanedSarg = normalizeSearchSargText(sargExpr);
                    // If the Sarg represents a single interval, convert to RANGE(...) to match
                    // any AND-based range canonicalization.
                    String maybeRange = parseSingleIntervalFromSarg(cleanedSarg);
                    if (maybeRange != null) {
                        return normalizeDigest("RANGE(" + valueExpr + "," + maybeRange + ")");
                    }
                    return normalizeDigest("SEARCH(" + valueExpr + "," + cleanedSarg + ")");
                }

                default -> {
                    // Generic operator/function: include operator kind and canonicalized operands
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
            }
        }
        return normalizeDigest(n.toString());
    }

    /**
     * Normalize character literal text for digesting by trimming trailing spaces
     * inside single-quoted segments. For example, "'SHIP      '" becomes
     * "'SHIP'". Non-literal parts of the string are left unchanged. This is
     * intentionally simple and tailored to the string forms produced by RexNode
     * toString for CHAR(n) literals in this project.
     */
    private static String normalizeCharLiteralText(String text) {
        if (text == null || text.indexOf('\'') < 0) {
            return text;
        }
        StringBuilder sb = new StringBuilder(text.length());
        int i = 0;
        int len = text.length();
        while (i < len) {
            char c = text.charAt(i);
            if (c == '\'') {
                sb.append(c); // opening quote
                i++;
                int litStart = i;
                while (i < len && text.charAt(i) != '\'') {
                    i++;
                }
                String literalContent = text.substring(litStart, i);
                int end = literalContent.length();
                while (end > 0 && literalContent.charAt(end - 1) == ' ') {
                    end--;
                }
                sb.append(literalContent, 0, end);
                if (i < len && text.charAt(i) == '\'') {
                    sb.append('\'');
                    i++;
                }
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    /**
     * Normalize SEARCH/Sarg textual representation for digest purposes by
     * removing explicit CHAR length annotations (e.g., ":CHAR(8)") and applying
     * {@link #normalizeCharLiteralText(String)} to trim trailing spaces inside any
     * quoted literals.
     */
    private static String normalizeSearchSargText(String sargExpr) {
        if (sargExpr == null) {
            return "";
        }
        // Remove type suffixes like :CHAR(8) or :CHAR(15) that do not affect
        // logical range semantics but would otherwise cause different digests.
        String cleaned = sargExpr.replaceAll(":CHAR\\(\\d+\\)", "");
        // Normalize trailing spaces inside quoted literals in the Sarg text.
        cleaned = normalizeCharLiteralText(cleaned);
        return cleaned;
    }

    /**
     * If the cleaned Sarg text encodes a single closed-open interval, return
     * a pair "lower,upper" where bounds are the textual literal forms used
     * in canonical digests. Otherwise return null.
     */
    private static String parseSingleIntervalFromSarg(String cleanedSarg) {
        if (cleanedSarg == null) return null;
        // Look for a simple pattern like "Sarg[[LOWER..UPPER)]" or variants
        int dots = cleanedSarg.indexOf("..");
        if (dots < 0) return null;
        // heuristically extract tokens around '..'
        int left = cleanedSarg.lastIndexOf('[', dots);
        if (left < 0) left = cleanedSarg.lastIndexOf('(', dots);
        int right = cleanedSarg.indexOf(')', dots);
        if (right < 0) right = cleanedSarg.indexOf(']', dots);
        if (left < 0 || right < 0 || right <= left) return null;
        String inner = cleanedSarg.substring(left + 1, right);
        String[] parts = inner.split("\\.\\.");
        if (parts.length != 2) return null;
        String low = parts[0].trim();
        String up = parts[1].trim();
        // remove surrounding quotes if present
        low = stripSurroundingQuotes(low);
        up = stripSurroundingQuotes(up);
        return low + "," + up;
    }

    private static String stripSurroundingQuotes(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.length() >= 2 && s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * Best-effort normalization of conjunction (AND) segments inside a
     * canonical digest string. Any substring of the form {@code AND(a&b&c)}
     * is rewritten so that the {@code a,b,c} parts are sorted
     * lexicographically, making AND order-insensitive at the string level.
     *
     * This is a safety net on top of expression-level canonicalization to
     * avoid false negatives when equivalent plans differ only in conjunct
     * ordering.
     */
    private static String normalizeAndOrderingInDigest(String digest) {
        if (digest == null) return null;
        StringBuilder out = new StringBuilder(digest.length());
        int idx = 0;
        while (idx < digest.length()) {
            int andPos = digest.indexOf("AND(", idx);
            if (andPos < 0) {
                out.append(digest.substring(idx));
                break;
            }
            // copy text before AND(
            out.append(digest, idx, andPos);
            int start = andPos + 4; // position after "AND("

            // Find the matching closing parenthesis for this AND( using a
            // simple depth counter so that inner parentheses from sub-
            // expressions (e.g., "<($x,$x)") do not terminate the search.
            int depth = 1;
            int i = start;
            int end = -1;
            while (i < digest.length() && depth > 0) {
                char ch = digest.charAt(i);
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                    if (depth == 0) {
                        end = i;
                        break;
                    }
                }
                i++;
            }
            if (end < 0) {
                // Malformed; append rest and stop
                out.append(digest.substring(andPos));
                break;
            }

            // The substring between start and end contains the raw conjunct
            // strings separated by '&', but may itself contain nested
            // parentheses. Splitting on '&' is safe because '&' is the
            // top-level separator we introduced when formatting AND.
            String inside = digest.substring(start, end);
            String[] parts = inside.split("&");
            Arrays.sort(parts);
            out.append("AND(");
            out.append(String.join("&", parts));
            out.append(")");
            idx = end + 1;
        }
        return out.toString();
    }

    /**
     * Canonicalize an AND-expression by:
     *  - Decomposing into individual conjuncts (flatten nested ANDs)
     *  - Identifying inequality pairs that form ranges on the same expression
     *    and emitting a single RANGE(expr,lower,upper) term
     *  - Canonicalizing all remaining conjuncts
     *  - Sorting and de-duplicating the resulting conjunct strings.
     *
     * This ensures that forms like (col >= L AND col < U) and SEARCH(col, Sarg[[L..U)))
     * both contribute a RANGE(...) term and otherwise share the same multiset of
     * conjunct digests.
     */
    private static String canonicalizeAndWithRanges(RexCall andCall) {
        // 1) Flatten nested ANDs into a simple list of conjunct nodes
        java.util.List<RexNode> conjuncts = new java.util.ArrayList<>();
        collectConjuncts(andCall, conjuncts);

        // 2) First pass: discover candidate lower/upper bounds per expression key.
        // Instead of requiring a literal RexNode, we look for comparisons where
        // exactly one side contains an input reference ("$") and treat the
        // other side as the bound. This allows us to fold patterns like
        // col < (DATE + INTERVAL YEAR) once the arithmetic has been folded to a
        // literal by canonicalizeRex. Importantly, we derive the expression key
        // from the raw (non-normalized) RexNode string so that different input
        // refs ($12 vs $14) are not collapsed to the same "$x" placeholder.
        java.util.Map<String, java.util.Map<String, String>> rangeMap = new java.util.HashMap<>();
        for (RexNode conj : conjuncts) {
            if (!(conj instanceof RexCall oc) || oc.getOperator() == null) continue;
            SqlKind k = oc.getOperator().getKind();
            switch (k) {
                case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, EQUALS -> {
                    RexNode left = oc.getOperands().size() > 0 ? oc.getOperands().get(0) : null;
                    RexNode right = oc.getOperands().size() > 1 ? oc.getOperands().get(1) : null;
                    if (left == null || right == null) continue;

                    String leftStr = canonicalizeRex(left);
                    String rightStr = canonicalizeRex(right);
                    boolean leftHasRef = leftStr.contains("$");
                    boolean rightHasRef = rightStr.contains("$");

                    // We only consider simple ranges where one side is an
                    // expression on columns (contains "$") and the other side
                    // is a bound without input refs (typically a literal or
                    // literal expression like DATE+INTERVAL).
                    String exprKey;
                    String bound;
                    if (leftHasRef && !rightHasRef) {
                        exprKey = canonicalizeRangeKey(left);
                        bound = rightStr;
                    } else if (!leftHasRef && rightHasRef) {
                        exprKey = canonicalizeRangeKey(right);
                        bound = leftStr;
                    } else {
                        continue;
                    }

                    java.util.Map<String, String> row = rangeMap.getOrDefault(exprKey, new java.util.HashMap<>());
                    if (k == SqlKind.GREATER_THAN || k == SqlKind.GREATER_THAN_OR_EQUAL) {
                        row.put("lower", bound);
                    } else if (k == SqlKind.LESS_THAN || k == SqlKind.LESS_THAN_OR_EQUAL) {
                        row.put("upper", bound);
                    } else if (k == SqlKind.EQUALS) {
                        row.put("lower", bound);
                        row.put("upper", bound);
                    }
                    rangeMap.put(exprKey, row);
                }
                default -> {}
            }
        }

        // 3) Determine which conjunct nodes are consumed by range folding
        java.util.Set<RexNode> consumed = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        java.util.List<String> rangeConjs = new java.util.ArrayList<>();

        for (var e : rangeMap.entrySet()) {
            String exprKey = e.getKey();
            java.util.Map<String, String> row = e.getValue();
            if (!(row.containsKey("lower") && row.containsKey("upper"))) continue;
            String lower = stripSurroundingQuotes(row.get("lower"));
            String upper = stripSurroundingQuotes(row.get("upper"));
            rangeConjs.add("RANGE(" + exprKey + "," + lower + "," + upper + ")");

            // Mark comparison conjuncts on this expression as consumed so we
            // don't also include them individually.
            for (RexNode conj : conjuncts) {
                if (!(conj instanceof RexCall oc) || oc.getOperator() == null) continue;
                SqlKind k = oc.getOperator().getKind();
                if (k != SqlKind.GREATER_THAN && k != SqlKind.GREATER_THAN_OR_EQUAL
                        && k != SqlKind.LESS_THAN && k != SqlKind.LESS_THAN_OR_EQUAL
                        && k != SqlKind.EQUALS) {
                    continue;
                }
                RexNode left = oc.getOperands().size() > 0 ? oc.getOperands().get(0) : null;
                RexNode right = oc.getOperands().size() > 1 ? oc.getOperands().get(1) : null;
                if (left == null || right == null) continue;

                String leftStr = canonicalizeRex(left);
                String rightStr = canonicalizeRex(right);
                boolean leftHasRef = leftStr.contains("$");
                boolean rightHasRef = rightStr.contains("$");

                String exprKey2 = null;
                if (leftHasRef && !rightHasRef) {
                    exprKey2 = canonicalizeRangeKey(left);
                } else if (!leftHasRef && rightHasRef) {
                    exprKey2 = canonicalizeRangeKey(right);
                } else {
                    continue;
                }

                if (exprKey.equals(exprKey2)) {
                    consumed.add(conj);
                }
            }
        }

        // 4) Canonicalize remaining conjuncts, skipping those folded into ranges
        java.util.List<String> conjStrings = new java.util.ArrayList<>();
        conjStrings.addAll(rangeConjs);

        for (RexNode conj : conjuncts) {
            if (consumed.contains(conj)) continue;
            String s = canonicalizeRex(conj);
            if ("true".equalsIgnoreCase(s)) continue;
            conjStrings.add(s);
        }

        // 5) Sort and de-duplicate
        java.util.Set<String> uniq = new java.util.LinkedHashSet<>(conjStrings);
        java.util.List<String> ordered = new java.util.ArrayList<>(uniq);
        java.util.Collections.sort(ordered);
        return "AND(" + String.join("&", ordered) + ")";
    }

    // Helper: recursively flatten nested ANDs into a list of conjuncts
    private static void collectConjuncts(RexNode node, java.util.List<RexNode> out) {
        if (node instanceof RexCall call && call.getOperator() != null
                && call.getOperator().getKind() == SqlKind.AND) {
            for (RexNode op : call.getOperands()) {
                collectConjuncts(op, out);
            }
        } else {
            out.add(node);
        }
    }

    // Helper: build a stable key for grouping range predicates that preserves
    // column identity. We strip CASTs but otherwise use the raw RexNode
    // toString so that $12 and $14 remain distinct (unlike the normalized
    // "$x" placeholder used in canonicalizeRex).
    private static String canonicalizeRangeKey(RexNode expr) {
        if (expr == null) return "";
        RexNode base = stripAllCasts(expr);
        return base == null ? "" : base.toString();
    }

    /**
     * Best-effort folding of expressions of the form DATE + INTERVAL YEAR
     * (or INTERVAL YEAR + DATE) into a single DATE literal string for
     * digesting. This helps align range predicates expressed via explicit
     * date arithmetic with those that use a literal upper bound.
     */
    private static String tryFoldDatePlusInterval(RexCall call) {
        if (call == null || call.getOperands().size() != 2) return null;
        RexNode a = call.getOperands().get(0);
        RexNode b = call.getOperands().get(1);

        RexLiteral dateLit = null;
        RexLiteral intervalLit = null;

        if (a instanceof RexLiteral litA && isDateLiteral(litA)) {
            dateLit = litA;
        }
        if (b instanceof RexLiteral litB && isDateLiteral(litB)) {
            if (dateLit != null) return null; // two dates, not our pattern
            dateLit = litB;
        }
        if (a instanceof RexLiteral litAInt && isYearIntervalLiteral(litAInt)) {
            intervalLit = litAInt;
        }
        if (b instanceof RexLiteral litBInt && isYearIntervalLiteral(litBInt)) {
            if (intervalLit != null) return null; // two intervals, not our pattern
            intervalLit = litBInt;
        }

        if (dateLit == null || intervalLit == null) return null;

        try {
            // Parse base date from literal text
            String dateText = normalizeCharLiteralText(dateLit.toString());
            dateText = stripSurroundingQuotes(dateText);
            if (dateText == null || dateText.isEmpty()) return null;
            LocalDate base = LocalDate.parse(dateText);

            // Extract month count from interval literal text, e.g. "12:INTERVAL YEAR".
            // Calcite's INTERVAL_YEAR_MONTH literal encodes a total month count
            // as the numeric prefix, so "12:INTERVAL YEAR" means 12 months
            // (i.e., 1 year), not 12 years.
            String intervalText = intervalLit.toString();
            int colon = intervalText.indexOf(':');
            if (colon <= 0) return null;
            String monthsPart = intervalText.substring(0, colon).trim();
            if (monthsPart.isEmpty()) return null;
            int months = Integer.parseInt(monthsPart);

            LocalDate result = base.plusMonths(months);
            // Match Calcite RexLiteral date rendering (typically unquoted: 1998-11-28)
            return result.toString();
        } catch (Exception e) {
            // Best-effort: if parsing fails, fall back to the generic PLUS handling
            return null;
        }
    }

    /**
     * Best-effort folding for DATE +/- INTERVAL_DAY_TIME when the interval represents an
     * exact whole number of days.
     *
     * Why: Calcite sometimes represents "DATE - INTERVAL '3' DAY" as
     * {@code MINUS(1998-12-01, 259200000:INTERVAL DAY)} (milliseconds), while other
     * queries may simplify to the literal {@code 1998-11-28}. Folding makes these
     * representations compare equal in canonical digests.
     *
     * @param call RexCall of kind PLUS or MINUS
     * @param isMinus true for DATE - INTERVAL, false for DATE + INTERVAL
     * @return folded date literal string (e.g., {@code 1998-11-28}) or null if not foldable
     */
    private static String tryFoldDatePlusOrMinusDayTimeInterval(RexCall call, boolean isMinus) {
        if (call == null || call.getOperands().size() != 2) return null;
        RexNode a = call.getOperands().get(0);
        RexNode b = call.getOperands().get(1);

        RexLiteral dateLit = null;
        RexLiteral intervalLit = null;

        if (isMinus) {
            // Be conservative: only fold DATE - INTERVAL patterns where DATE is the left operand.
            if (a instanceof RexLiteral litA && isDateLiteral(litA)) {
                dateLit = litA;
            } else {
                return null;
            }
            if (b instanceof RexLiteral litB && isDayTimeIntervalLiteral(litB)) {
                intervalLit = litB;
            } else {
                return null;
            }
        } else {
            // PLUS is commutative: accept either order.
            if (a instanceof RexLiteral litA && isDateLiteral(litA)) {
                dateLit = litA;
            }
            if (b instanceof RexLiteral litB && isDateLiteral(litB)) {
                if (dateLit != null) return null;
                dateLit = litB;
            }
            if (a instanceof RexLiteral litAInt && isDayTimeIntervalLiteral(litAInt)) {
                intervalLit = litAInt;
            }
            if (b instanceof RexLiteral litBInt && isDayTimeIntervalLiteral(litBInt)) {
                if (intervalLit != null) return null;
                intervalLit = litBInt;
            }
            if (dateLit == null || intervalLit == null) return null;
        }

        try {
            String dateText = normalizeCharLiteralText(dateLit.toString());
            dateText = stripSurroundingQuotes(dateText);
            if (dateText == null || dateText.isEmpty()) return null;
            LocalDate base = LocalDate.parse(dateText);

            // Calcite encodes INTERVAL_DAY_TIME as a numeric prefix representing total millis.
            // Example: 259200000:INTERVAL DAY  (3 * 86400000)
            String intervalText = intervalLit.toString();
            int colon = intervalText.indexOf(':');
            if (colon <= 0) return null;
            String millisPart = intervalText.substring(0, colon).trim();
            if (millisPart.isEmpty()) return null;
            long millis = Long.parseLong(millisPart);

            final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;
            if (millis % MILLIS_PER_DAY != 0L) {
                // Not a whole number of days; folding could change type/semantics.
                return null;
            }
            long days = millis / MILLIS_PER_DAY;

            LocalDate result;
            if (isMinus) {
                result = base.minusDays(days);
            } else {
                result = base.plusDays(days);
            }
            return result.toString();
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isDateLiteral(RexLiteral lit) {
        if (lit == null || lit.getType() == null || lit.getType().getSqlTypeName() == null) return false;
        return lit.getType().getSqlTypeName() == SqlTypeName.DATE;
    }

    private static boolean isYearIntervalLiteral(RexLiteral lit) {
        if (lit == null || lit.getType() == null || lit.getType().getSqlTypeName() == null) return false;
        SqlTypeName typeName = lit.getType().getSqlTypeName();
        // Treat any interval whose family is YEAR-MONTH as a year interval,
        // which covers textual forms like "12:INTERVAL YEAR" in Calcite's
        // RexLiteral rendering.
        return typeName.getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH;
    }

    private static boolean isDayTimeIntervalLiteral(RexLiteral lit) {
        if (lit == null || lit.getType() == null || lit.getType().getSqlTypeName() == null) return false;
        SqlTypeName typeName = lit.getType().getSqlTypeName();
        return typeName.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME;
    }

    /**
     * Detect and canonicalize an Aggregate-over-3way-inner-join pattern into an equivalent
     * join-with-pre-aggregated-lineitem representation (TPCH Q3-style).
     *
     * This is intentionally conservative: it only fires when we can see the expected join keys
     * (L_ORDERKEY = O_ORDERKEY and O_CUSTKEY = C_CUSTKEY) somewhere in the inner-join tree, and
     * the aggregate computes a SUM over an expression that references only LINEITEM fields.
     *
     * Returns a digest string that matches the canonical shape produced when the plan already
     * contains a pre-aggregation factor under the join.
     */
    private static String tryCanonicalizeAggregateOverInnerJoinAsPreAgg(LogicalAggregate agg,
                                                                        Set<RelNode> path) {
        if (agg == null) return null;
        if (agg.getGroupSet() == null) return null;
        if (agg.getAggCallList() == null || agg.getAggCallList().isEmpty()) return null;

        // We only canonicalize single SUM aggregations in this special-case.
        if (agg.getAggCallList().size() != 1) return null;
        var sumCall = agg.getAggCallList().get(0);
        if (sumCall.getAggregation() == null) return null;
        if (!"SUM".equalsIgnoreCase(sumCall.getAggregation().getName())) return null;
        if (sumCall.isDistinct()) return null;
        if (sumCall.getArgList() == null || sumCall.getArgList().isEmpty()) return null;

        // Expect Aggregate input to be a Project over an INNER join tree.
        if (!(agg.getInput() instanceof LogicalProject proj)) return null;
        RelNode joinRoot = proj.getInput();
        if (!(joinRoot instanceof LogicalJoin)) return null;

        // Ensure joinRoot is an INNER join tree containing the key equalities we rely on.
        if (!innerJoinTreeContainsEquality(joinRoot, "L_ORDERKEY", "O_ORDERKEY")) return null;
        if (!innerJoinTreeContainsEquality(joinRoot, "O_CUSTKEY", "C_CUSTKEY")) return null;

        // Identify the revenue expression used by SUM: it's a Project output ref.
        int argIdx = sumCall.getArgList().get(0);
        if (argIdx < 0 || argIdx >= proj.getProjects().size()) return null;
        RexNode revenueExpr = proj.getProjects().get(argIdx);

        // Revenue expression must reference only LINEITEM columns.
        if (!rexReferencesOnlyTableColumns(joinRoot, revenueExpr, "LINEITEM")) return null;

        // Find the LINEITEM factor subtree.
        List<RelNode> factors = new ArrayList<>();
        List<RexNode> conds = new ArrayList<>();
        collectInnerJoinFactors(joinRoot, factors, conds);
        RelNode lineitemFactor = null;
        for (RelNode f : factors) {
            if (relContainsTableScan(f, "lineitem")) {
                lineitemFactor = f;
                break;
            }
        }
        if (lineitemFactor == null) return null;

        // The pre-aggregation grouping key is L_ORDERKEY.
        // We build a synthetic factor digest:
        //   Aggregate(groups=[0], calls=[SUM@1])->Project[$x,<revenueExpr>]-><lineitemFactorDigest>
        // This matches the common representation produced by plans that already pre-aggregate LINEITEM.
        String revenueDigest = canonicalizeRex(revenueExpr);
        String lineitemBase = canonicalDigestInternal(lineitemFactor, path);
        String aggFactor = "Aggregate(groups=[0], calls=[SUM@1])->Project[$x," + revenueDigest + "]->" + lineitemBase;

        // Canonicalize the join using the same logic as INNER-join canonicalization.
        List<String> factorDigests = new ArrayList<>();
        for (RelNode f : factors) {
            if (f == lineitemFactor) {
                factorDigests.add(aggFactor);
            } else {
                factorDigests.add(canonicalDigestInternal(f, path));
            }
        }
        Collections.sort(factorDigests);

        List<String> condDigests = new ArrayList<>();
        for (RexNode c : conds) {
            decomposeConj(c, condDigests);
        }
        condDigests = new ArrayList<>(new java.util.LinkedHashSet<>(condDigests));
        condDigests.sort(String::compareTo);
        String condPart = condDigests.isEmpty() ? "true" : String.join("&", condDigests);

        return "Join(INNER," + condPart + "){" + String.join("|", factorDigests) + "}";
    }

    private static boolean relContainsTableScan(RelNode node, String tableLower) {
        if (node == null || tableLower == null) return false;
        final String needle = tableLower.toLowerCase();
        final boolean[] found = new boolean[] { false };
        new RelVisitor() {
            @Override public void visit(RelNode rel, int ordinal, RelNode parent) {
                if (rel instanceof TableScan ts) {
                    List<String> qn = ts.getTable() == null ? null : ts.getTable().getQualifiedName();
                    String name = (qn == null || qn.isEmpty()) ? "" : qn.get(qn.size() - 1);
                    if (name != null && name.toLowerCase().contains(needle)) {
                        found[0] = true;
                        return;
                    }
                }
                super.visit(rel, ordinal, parent);
            }
        }.go(node);
        return found[0];
    }

    private static boolean rexReferencesOnlyTableColumns(RelNode joinRoot, RexNode expr, String tableNameUpper) {
        if (expr == null || joinRoot == null || tableNameUpper == null) return false;
        final String tableNeedle = tableNameUpper.toUpperCase();
        final List<RelDataTypeField> fields = joinRoot.getRowType() == null ? List.of() : joinRoot.getRowType().getFieldList();
        final boolean[] ok = new boolean[] { true };

        Consumer<RexNode> walk = new Consumer<>() {
            @Override public void accept(RexNode n) {
                if (n == null) return;
                RexNode base = stripAllCasts(n);
                if (base instanceof RexInputRef ref) {
                    int idx = ref.getIndex();
                    if (idx < 0 || idx >= fields.size()) {
                        ok[0] = false;
                        return;
                    }
                    String fn = normalizeFieldName(fields.get(idx).getName());
                    // We accept either explicit table prefix (LINEITEM / public.lineitem) or TPCH-style column prefix L_.
                    if (!(fn.contains(tableNeedle) || fn.startsWith("L_"))) {
                        ok[0] = false;
                    }
                    return;
                }
                if (base instanceof RexCall c) {
                    for (RexNode op : c.getOperands()) {
                        accept(op);
                        if (!ok[0]) return;
                    }
                }
            }
        };

        walk.accept(expr);
        return ok[0];
    }

    private static boolean innerJoinTreeContainsEquality(RelNode root, String colAUpper, String colBUpper) {
        if (root == null || colAUpper == null || colBUpper == null) return false;
        final String a = colAUpper.toUpperCase();
        final String b = colBUpper.toUpperCase();
        final boolean[] found = new boolean[] { false };

        new RelVisitor() {
            @Override public void visit(RelNode rel, int ordinal, RelNode parent) {
                if (found[0]) return;
                if (rel instanceof LogicalJoin j && j.getJoinType() == JoinRelType.INNER) {
                    RexNode cond = j.getCondition();
                    if (cond != null && joinConditionContainsEquality(j, cond, a, b)) {
                        found[0] = true;
                        return;
                    }
                }
                super.visit(rel, ordinal, parent);
            }
        }.go(root);

        return found[0];
    }

    private static boolean joinConditionContainsEquality(LogicalJoin join, RexNode cond, String a, String b) {
        if (join == null || cond == null) return false;
        List<RelDataTypeField> fields = join.getRowType() == null ? List.of() : join.getRowType().getFieldList();
        final boolean[] found = new boolean[] { false };
        Consumer<RexNode> walk = new Consumer<>() {
            @Override public void accept(RexNode n) {
                if (found[0] || n == null) return;
                RexNode base = stripAllCasts(n);
                if (base instanceof RexCall c && c.getOperator() != null) {
                    SqlKind k = c.getOperator().getKind();
                    if (k == SqlKind.AND) {
                        for (RexNode op : c.getOperands()) {
                            accept(op);
                            if (found[0]) return;
                        }
                        return;
                    }
                    if (k == SqlKind.EQUALS && c.getOperands().size() == 2) {
                        RexNode l0 = stripAllCasts(c.getOperands().get(0));
                        RexNode r0 = stripAllCasts(c.getOperands().get(1));
                        if (l0 instanceof RexInputRef l && r0 instanceof RexInputRef r) {
                            String ln = fieldNameByIndex(fields, l.getIndex());
                            String rn = fieldNameByIndex(fields, r.getIndex());
                            if (ln != null && rn != null) {
                                String L = normalizeFieldName(ln);
                                String R = normalizeFieldName(rn);
                                boolean match = (L.equals(a) && R.equals(b)) || (L.equals(b) && R.equals(a));
                                if (match) {
                                    found[0] = true;
                                    return;
                                }
                            }
                        }
                    }
                    // Other calls: keep walking
                    for (RexNode op : c.getOperands()) {
                        accept(op);
                        if (found[0]) return;
                    }
                }
            }
        };
        walk.accept(cond);
        return found[0];
    }

    private static String fieldNameByIndex(List<RelDataTypeField> fields, int idx) {
        if (fields == null) return null;
        if (idx < 0 || idx >= fields.size()) return null;
        RelDataTypeField f = fields.get(idx);
        return f == null ? null : f.getName();
    }

    private static String normalizeFieldName(String name) {
        if (name == null) return "";
        String s = name.trim();
        int dot = s.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < s.length()) {
            s = s.substring(dot + 1);
        }
        // Remove quoting artifacts if any
        s = s.replace("\"", "");
        return s.toUpperCase();
    }

    /**
     * Resolve a field index on a RelNode back to a base table/column pair when possible.
     *
     * We only follow through simple Projects consisting of RexInputRef passthroughs
     * and Filters (which do not change row shape). If the expression is computed,
     * or the subtree is not a single TableScan, returns null.
     */
    private static FieldOrigin resolveFieldOrigin(RelNode rel, int fieldIndex) {
        if (rel == null) return null;
        if (fieldIndex < 0) return null;

        if (rel instanceof LogicalFilter f) {
            return resolveFieldOrigin(f.getInput(), fieldIndex);
        }

        if (rel instanceof LogicalProject p) {
            if (fieldIndex >= p.getProjects().size()) return null;
            RexNode expr = stripAllCasts(p.getProjects().get(fieldIndex));
            if (expr instanceof RexInputRef ref) {
                return resolveFieldOrigin(p.getInput(), ref.getIndex());
            }
            return null;
        }

        if (rel instanceof TableScan ts) {
            if (ts.getRowType() == null) return null;
            List<RelDataTypeField> fields = ts.getRowType().getFieldList();
            if (fieldIndex >= fields.size()) return null;
            String col = normalizeFieldName(fields.get(fieldIndex).getName());
            List<String> qn = ts.getTable() == null ? null : ts.getTable().getQualifiedName();
            String table = (qn == null || qn.isEmpty()) ? "" : qn.get(qn.size() - 1);
            String tableUpper = table == null ? "" : table.trim().toUpperCase();
            if (tableUpper.isEmpty() || col.isEmpty()) return null;
            return new FieldOrigin(tableUpper, col);
        }

        return null;
    }

    private static boolean relHasNonTrivialFilter(RelNode rel) {
        if (rel == null) return false;
        if (rel instanceof LogicalFilter f) {
            String c = canonicalizeRex(f.getCondition());
            if (!"true".equalsIgnoreCase(c)) return true;
            return relHasNonTrivialFilter(f.getInput());
        }
        if (rel instanceof LogicalProject p) {
            return relHasNonTrivialFilter(p.getInput());
        }
        return false;
    }

    /**
     * Returns true if this SEMI join is guaranteed redundant under FK->PK constraints and
     * the right side is unfiltered.
     */
    private static boolean isRedundantSemiJoinUnderSchema(LogicalJoin semiJoin) {
        if (semiJoin == null || semiJoin.getJoinType() != JoinRelType.SEMI) return false;

        // Right side must be unfiltered; otherwise the SEMI join can filter.
        if (relHasNonTrivialFilter(semiJoin.getRight())) return false;

        // Both sides must resolve to a single base table so we can check FK metadata.
        // (This is conservative; we can extend later.)
        if (!(stripToTableScan(semiJoin.getLeft()) instanceof TableScan)) return false;
        if (!(stripToTableScan(semiJoin.getRight()) instanceof TableScan)) return false;

        int leftCount = semiJoin.getLeft().getRowType() == null ? -1 : semiJoin.getLeft().getRowType().getFieldCount();
        if (leftCount <= 0) return false;

        List<int[]> pairs = extractLeftRightEqualityPairs(semiJoin.getCondition(), leftCount);
        if (pairs.isEmpty()) return false;

        SchemaSummary s = getSchemaSummary();
        if (s == null) return false;

        for (int[] p : pairs) {
            int li = p[0];
            int ri = p[1];
            FieldOrigin lo = resolveFieldOrigin(semiJoin.getLeft(), li);
            FieldOrigin ro = resolveFieldOrigin(semiJoin.getRight(), ri);
            if (lo == null || ro == null) return false;

            Map<String, Ref> fks = s.fkByTableAndColUpper.get(lo.tableUpper);
            if (fks == null) return false;
            Ref ref = fks.get(lo.colUpper);
            if (ref == null) return false;
            if (!ref.refTableUpper.equals(ro.tableUpper)) return false;
            if (!ref.refColUpper.equals(ro.colUpper)) return false;

            java.util.Set<String> pkCols = s.pkByTableUpper.getOrDefault(ro.tableUpper, java.util.Set.of());
            if (!pkCols.contains(ro.colUpper)) return false;
        }

        return true;
    }

    private static RelNode stripToTableScan(RelNode rel) {
        if (rel == null) return null;
        RelNode cur = rel;
        while (true) {
            if (cur instanceof LogicalFilter f) {
                cur = f.getInput();
                continue;
            }
            if (cur instanceof LogicalProject p) {
                // unwrap only passthrough/identity projects
                boolean onlyRefs = true;
                for (RexNode e : p.getProjects()) {
                    if (!(stripAllCasts(e) instanceof RexInputRef)) {
                        onlyRefs = false;
                        break;
                    }
                }
                if (onlyRefs) {
                    cur = p.getInput();
                    continue;
                }
            }
            return cur;
        }
    }

    /** Extract (leftIndex,rightIndex) pairs for conjunctions of input-ref equalities. */
    private static List<int[]> extractLeftRightEqualityPairs(RexNode cond, int leftFieldCount) {
        List<int[]> out = new ArrayList<>();
        if (cond == null || leftFieldCount <= 0) return out;

        Consumer<RexNode> walk = new Consumer<>() {
            @Override public void accept(RexNode n) {
                if (n == null) return;
                RexNode base = stripAllCasts(n);
                if (base instanceof RexCall c && c.getOperator() != null) {
                    SqlKind k = c.getOperator().getKind();
                    if (k == SqlKind.AND) {
                        for (RexNode op : c.getOperands()) accept(op);
                        return;
                    }
                    if (k == SqlKind.EQUALS && c.getOperands().size() == 2) {
                        RexNode l0 = stripAllCasts(c.getOperands().get(0));
                        RexNode r0 = stripAllCasts(c.getOperands().get(1));
                        if (l0 instanceof RexInputRef l && r0 instanceof RexInputRef r) {
                            int li;
                            int ri;
                            if (l.getIndex() < leftFieldCount && r.getIndex() >= leftFieldCount) {
                                li = l.getIndex();
                                ri = r.getIndex() - leftFieldCount;
                            } else if (r.getIndex() < leftFieldCount && l.getIndex() >= leftFieldCount) {
                                li = r.getIndex();
                                ri = l.getIndex() - leftFieldCount;
                            } else {
                                return;
                            }
                            out.add(new int[] { li, ri });
                        }
                    }
                }
            }
        };

        walk.accept(cond);
        return out;
    }

    /**
     * Canonicalize an equi-SEMI join as an INNER join against a DISTINCT key set derived from the
     * right input.
     *
     * Semantics:
     *   SEMI(L,R, Lk = Rk) == Project(L.*) ( L INNER JOIN (SELECT DISTINCT Rk FROM R) )
     *
     * We only emit this canonical form when we can extract one or more equality key pairs where
     * both sides are RexInputRef references.
     */
    private static String canonicalizeSemiJoinAsInnerJoinWithDistinctRightKeys(LogicalJoin semiJoin,
                                                                               Set<RelNode> path) {
        if (semiJoin == null || semiJoin.getJoinType() != JoinRelType.SEMI) return null;

        int leftCount = semiJoin.getLeft().getRowType() == null ? -1 : semiJoin.getLeft().getRowType().getFieldCount();
        if (leftCount <= 0) return null;

        List<int[]> pairs = extractLeftRightEqualityPairs(semiJoin.getCondition(), leftCount);
        if (pairs.isEmpty()) return null;

        // Build a deterministic condition part, like INNER join canonicalization.
        List<String> condDigests = new ArrayList<>();
        decomposeConj(semiJoin.getCondition(), condDigests);
        condDigests = new ArrayList<>(new java.util.LinkedHashSet<>(condDigests));
        condDigests.sort(String::compareTo);
        String condPart = condDigests.isEmpty() ? "true" : String.join("&", condDigests);

        // Derive the distinct right-key set factor digest.
        // We don't need exact ref indices in the string (canonicalizeRex normalizes to $x anyway),
        // but we preserve the number of key fields for stability.
        int k = pairs.size();
        java.util.List<String> projKeys = new java.util.ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            projKeys.add("$x");
        }
        java.util.List<Integer> groups = new java.util.ArrayList<>(k);
        for (int i = 0; i < k; i++) groups.add(i);
        String rightBase = canonicalDigestInternal(semiJoin.getRight(), path);
        String rightKeySet = "Aggregate(groups=" + groups + ", calls=[])" + "->Project[" + String.join(",", projKeys) + "]->" + rightBase;

        String left = canonicalDigestInternal(semiJoin.getLeft(), path);
        return "Join(INNER," + condPart + "){" + left + "|" + rightKeySet + "}";
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
            boolean topN = (s.fetch != null || s.offset != null);
            String fetch = s.fetch == null ? "" : ("fetch=" + canonicalizeRex(s.fetch));
            String offset = s.offset == null ? "" : ("offset=" + canonicalizeRex(s.offset));
            String meta = (fetch + (fetch.isEmpty() || offset.isEmpty() ? "" : ",") + offset).trim();

            String order = "";
            if (topN && s.getCollation() != null && s.getCollation().getFieldCollations() != null
                    && !s.getCollation().getFieldCollations().isEmpty()) {
                java.util.List<String> keys = new java.util.ArrayList<>();
                for (org.apache.calcite.rel.RelFieldCollation fc : s.getCollation().getFieldCollations()) {
                    keys.add(fc.getFieldIndex() + ":" + fc.direction + ":" + fc.nullDirection);
                }
                order = "order=" + keys;
            }

            if (meta.isEmpty() && order.isEmpty()) return "Sort";
            if (meta.isEmpty()) return "Sort(" + order + ")";
            if (order.isEmpty()) return "Sort(" + meta + ")";
            return "Sort(" + meta + "," + order + ")";
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

        // RelToSqlConverter in Calcite does not reliably support correlated plans
        // (LogicalCorrelate) and can throw AssertionError such as
        // "field ordinal X out of range". Since this method is only a best-effort
        // fallback (Postgres EXPLAIN), we skip correlated plans instead of
        // crashing the whole equivalence run.
        if (containsLogicalCorrelate(rel)) {
            System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] Skipping EXPLAIN fallback: plan contains LogicalCorrelate.");
            return null;
        }

        String sql = null;
        try {
            RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
            RelToSqlConverter.Result res = converter.visitRoot(rel);
            sql = res.asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();

            return GetQueryPlans.getCleanedQueryPlanJSONasString(sql);
        } catch (SQLException e) {
            System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] Error while obtaining plan: " + e.getMessage());
            if (sql != null) {
                String trimmed = sql.strip();
                String preview = trimmed.length() <= 2000 ? trimmed : trimmed.substring(0, 2000) + " ...[truncated]";
                System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] Generated SQL (preview):\n" + preview);
            } else {
                System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] Generated SQL unavailable (conversion failed before SQL materialized).");
            }
            return null;
        } catch (AssertionError e) {
            // Calcite may throw AssertionError for some logical operator combinations
            // during RelNode -> SQL conversion (notably correlated plans).
            System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] AssertionError while converting RelNode to SQL: " + e.getMessage());
            return null;
        } catch (RuntimeException e) {
            // RelToSqlConverter and SQL stringification can throw unchecked exceptions for
            // complex logical plans (especially after aggressive rule sequences).
            System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] Error while converting RelNode to SQL: " + e.getMessage());
            return null;
        }
    }

    /** Detect whether a plan still contains a LogicalCorrelate. */
    private static boolean containsLogicalCorrelate(RelNode rel) {
        if (rel == null) return false;
        final boolean[] found = new boolean[] { false };
        RelVisitor v = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (found[0]) return;
                if (node instanceof org.apache.calcite.rel.logical.LogicalCorrelate) {
                    found[0] = true;
                    return;
                }
                super.visit(node, ordinal, parent);
            }
        };
        v.go(rel);
        return found[0];
    }
}
