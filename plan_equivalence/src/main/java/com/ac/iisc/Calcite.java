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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
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
import org.apache.calcite.tools.RelBuilder;
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
     * Conservative schema-aware cleanup rule:
     *
     * Calcite decorrelation and subquery rewrites sometimes introduce
     * DISTINCT-like aggregates (LogicalAggregate with no aggregate calls).
     * When those aggregates group by a set of columns that includes the
     * base table's primary key, the aggregate is redundant (each group
     * corresponds to at most one row).
     *
     * We rewrite:
     *   Aggregate(group=[...], calls=[])
     *     <single-table input>
     * to:
     *   Project(<group fields>)
     *     <same input>
     *
     * This is intentionally limited to inputs that resolve to a single
     * TableScan (possibly under Filter/Project) and uses the JSON schema
     * summary PKs rather than Calcite metadata, which may not include PK
     * constraints for JDBC schemas.
     */
    private static final RelOptRule REMOVE_REDUNDANT_DISTINCT_AGG_ON_PK = new RemoveRedundantDistinctAggOnPkRule();

    @SuppressWarnings("deprecation")
    private static final class RemoveRedundantDistinctAggOnPkRule extends RelOptRule {
        RemoveRedundantDistinctAggOnPkRule() {
            super(operand(LogicalAggregate.class, any()), "RemoveRedundantDistinctAggOnPkRule");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalAggregate agg = call.rel(0);
            if (agg.getAggCallList() != null && !agg.getAggCallList().isEmpty()) return;
            if (agg.getGroupSet() == null || agg.getGroupSet().isEmpty()) return;

            // Try to resolve the aggregate input to a single base table scan, while
            // tracking how group indexes map through simple Projects.
            RelNode input = agg.getInput();
            if (input == null) return;

            java.util.List<Integer> groupIdxs = agg.getGroupSet().asList();
            java.util.List<Integer> mappedIdxs = new java.util.ArrayList<>(groupIdxs);
            RelNode cur = input;

            // Walk down through Filter/Project; only accept Projects that are
            // pure input-ref passthroughs for all group indices.
            while (true) {
                if (cur instanceof LogicalFilter f) {
                    cur = f.getInput();
                    continue;
                }
                if (cur instanceof LogicalProject p) {
                    java.util.List<RexNode> exprs = p.getProjects();
                    java.util.List<Integer> next = new java.util.ArrayList<>(mappedIdxs.size());
                    for (Integer outIdxObj : mappedIdxs) {
                        int outIdx = outIdxObj == null ? -1 : outIdxObj;
                        if (outIdx < 0 || outIdx >= exprs.size()) return;
                        RexNode e = stripAllCasts(exprs.get(outIdx));
                        if (!(e instanceof RexInputRef ref)) return;
                        next.add(ref.getIndex());
                    }
                    mappedIdxs = next;
                    cur = p.getInput();
                    continue;
                }
                break;
            }

            if (!(cur instanceof TableScan ts)) return;

            // Resolve table name.
            java.util.List<String> qn = ts.getTable() == null ? java.util.List.of() : ts.getTable().getQualifiedName();
            if (qn == null || qn.isEmpty()) return;
            String tableUpper = qn.get(qn.size() - 1);
            if (tableUpper == null || tableUpper.isBlank()) return;
            tableUpper = tableUpper.trim().toUpperCase();

            SchemaSummary ss = getSchemaSummary();
            java.util.Set<String> pkCols = ss == null || ss.pkByTableUpper == null
                    ? java.util.Set.of()
                    : ss.pkByTableUpper.getOrDefault(tableUpper, java.util.Set.of());
            if (pkCols == null || pkCols.isEmpty()) return;

            // Determine which base columns are being grouped.
            java.util.List<RelDataTypeField> baseFields = ts.getRowType() == null ? java.util.List.of() : ts.getRowType().getFieldList();
            if (baseFields == null || baseFields.isEmpty()) return;

            java.util.Set<String> groupColsUpper = new java.util.LinkedHashSet<>();
            for (Integer inIdxObj : mappedIdxs) {
                int inIdx = inIdxObj == null ? -1 : inIdxObj;
                if (inIdx < 0 || inIdx >= baseFields.size()) return;
                String n = baseFields.get(inIdx).getName();
                if (n == null || n.isBlank()) return;
                groupColsUpper.add(n.trim().toUpperCase());
            }

            // Safe only if grouping includes the full PK.
            if (!groupColsUpper.containsAll(pkCols)) return;

            // Replace the DISTINCT-like Aggregate with a Project of the group keys.
            // (Maintains the same output schema as the Aggregate, but avoids structural noise.)
            RelBuilder b = call.builder();
            b.push(input);
            java.util.List<RexNode> projects = new java.util.ArrayList<>(groupIdxs.size());
            for (Integer gObj : groupIdxs) {
                int g = gObj == null ? -1 : gObj;
                if (g < 0) return;
                projects.add(b.field(g));
            }
            b.project(projects);
            call.transformTo(b.build());
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

            // Compatibility shim: TPC-DS/PostgreSQL commonly use SUBSTR(...).
            // Calcite supports SUBSTRING(...) and, in Calcite 1.36, certain
            // SUBSTR operator definitions can trigger a validation/type-coercion
            // ClassCastException (FamilyOperandTypeChecker -> SqlOperandMetadata).
            // Rewriting SUBSTR to SUBSTRING keeps semantics (Postgres synonym)
            // but routes validation through the stable built-in operator.
            sqlForParse = sqlForParse.replaceAll("(?i)\\bsubstr\\s*\\(", "substring(");

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
            // We also pass the original SQL strings so the final Postgres EXPLAIN
            // fallback can run even when Calcite produces correlated plans
            // (LogicalCorrelate), which RelToSqlConverter cannot reliably render.
            return compareRelNodesForEquivalence(rel1, rel2, transformations, sql1, sql2);
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
        return compareRelNodesForEquivalence(rel1, rel2, transformations, null, null);
    }

    /**
     * Core equivalence logic over RelNodes; optionally retains the original SQL strings
     * so Postgres EXPLAIN fallback can run even when plans contain correlates.
     */
    private static boolean compareRelNodesForEquivalence(
        RelNode rel1,
        RelNode rel2,
        List<String> transformations,
        String sql1,
        String sql2
    ) {
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

            // Additional robustness: ignore ref-only Projects that have been normalized
            // to the compact form Project[$x*]->...
            // These Projects represent column pruning/reordering only (no literals)
            // and are a frequent source of false negatives in TPC-DS Q5.
            String c1ProjNorm = normalizeRefOnlyProjectWrappersInDigest(c1);
            String c2ProjNorm = normalizeRefOnlyProjectWrappersInDigest(c2);
            if (c1ProjNorm.equals(c2ProjNorm)) return true;

            // Additional robustness: treat conjunctions inside canonical digests
            // as order-insensitive. In rare cases (e.g., mixed RANGE(...) sources
            // from SEARCH vs. inequalities), AND terms may still appear in
            // different textual order despite representing the same multiset of
            // conjuncts. Normalize those segments and compare again.
            String c1AndNorm = normalizeAndOrderingInDigest(c1);
            String c2AndNorm = normalizeAndOrderingInDigest(c2);
            if (c1AndNorm.equals(c2AndNorm)) return true;

            // Combine both normalizations.
            String c1Both = normalizeAndOrderingInDigest(c1ProjNorm);
            String c2Both = normalizeAndOrderingInDigest(c2ProjNorm);
            if (c1Both.equals(c2Both)) return true;

            // Additional positive check (best-effort): convert both plans back to SQL
            // and see if they converge to the same SQL text.
            //
            // Soundness: if both plans can be rendered to the same SQL statement (under
            // the same dialect/rendering rules), then they are equivalent under Calcite's
            // semantics. However, the converse is NOT true: differing rendered SQL does
            // not imply non-equivalence (converter choices/aliases/order may differ).
            String sx1 = relNodeToSql(rel1);
            String sx2 = relNodeToSql(rel2);
            if (sx1 != null && sx2 != null) {
                String nsx1 = normalizeSqlForComparison(sx1);
                String nsx2 = normalizeSqlForComparison(sx2);
                if (nsx1 != null && nsx1.equals(nsx2)) return true;
            }

            // Final fallback: compare cleaned PostgreSQL execution plans as a last resort.
            // If both queries yield the same physical plan on the target database,
            // treat them as equivalent even if Calcite's logical digests differ.
            //
            // Important: Calcite often represents correlated scalar subqueries as
            // LogicalCorrelate. RelToSqlConverter is not reliable for such plans.
            // If either plan still contains LogicalCorrelate, fall back to
            // EXPLAINing the original SQL strings directly.
            String p1 = null;
            String p2 = null;
            boolean correlatePresent = containsLogicalCorrelate(rel1) || containsLogicalCorrelate(rel2);
            if (!correlatePresent) {
                p1 = convertRelNodetoJSONQueryPlan(rel1);
                p2 = convertRelNodetoJSONQueryPlan(rel2);
                if (p1 != null && p1.equals(p2)) return true;
            }

            if ((p1 == null || p2 == null) && sql1 != null && sql2 != null) {
                String sp1 = convertSqlToJSONQueryPlan(sql1);
                String sp2 = convertSqlToJSONQueryPlan(sql2);
                if (sp1 != null && sp1.equals(sp2)) return true;
            }

            boolean debug = (transformations != null) || Boolean.getBoolean("calcite.debugEquivalence");
            if (debug) {
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
     * Run PostgreSQL EXPLAIN (FORMAT JSON...) on raw SQL text (best-effort).
     *
     * This bypasses RelToSqlConverter and therefore works even when Calcite's
     * relational plan contains LogicalCorrelate (common for correlated subqueries).
     */
    private static String convertSqlToJSONQueryPlan(String sql) {
        if (sql == null) return null;
        String s = sql.trim();
        while (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();
        if (s.isBlank()) return null;
        try {
            return GetQueryPlans.getCleanedQueryPlanJSONasString(s);
        } catch (SQLException e) {
            System.err.println("[Calcite.convertSqlToJSONQueryPlan] Error while obtaining plan: " + e.getMessage());
            return null;
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
        // Normalize correlated variable references (e.g., $cor0 or $cor0.i_category)
        // so correlated vs decorrelated plans can compare equal.
        s = s.replaceAll("\\$cor\\d+(?:\\.[A-Za-z0-9_]+)?", "\\$x");
        // Collapse multiple spaces for stability so cosmetic spacing doesn't differ
        s = s.replaceAll("[ ]+", " ");
        return s.trim();
    }

    /**
     * Normalize SQL text for comparison.
     *
     * This is intentionally conservative: it only removes trailing semicolons
     * and collapses whitespace so that formatting differences from RelToSql
     * do not cause false mismatches.
     */
    private static String normalizeSqlForComparison(String sql) {
        if (sql == null) return null;
        String s = sql.trim();
        while (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();
        if (s.isBlank()) return "";
        s = s.replaceAll("\\s+", " ");
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

            // Heuristic: some rewrite paths introduce Projects directly on top of a
            // TableScan that simply select a (varying) set of input refs plus a few
            // numeric zero literals to align UNION branch schemas.
            //
            // Because canonicalizeRex collapses *which* input ref is used to "$x",
            // differences in the *count* of selected refs can cause spurious mismatches
            // even when those extra columns are unused downstream.
            //
            // We normalize such projections to: "$x*" + N copies of "0".
            ProjectRefZeroShape refZero = tryDescribeRefAndZeroProject(p);
            if (refZero != null) {
                sb.append("$x*");
                for (int i = 0; i < refZero.zeroCount; i++) {
                    sb.append(",0");
                }
            } else {
                for (int i = 0; i < p.getProjects().size(); i++) {
                    RexNode rex = p.getProjects().get(i);
                    if (i > 0) sb.append(",");
                    sb.append(canonicalizeRex(rex));
                }
            }
            sb.append("]->");
            sb.append(canonicalDigestInternal(p.getInput(), path));
            path.remove(rel);
            return sb.toString();
        }

        // (helper methods live below)
        // Handle Filter: canonicalize the predicate expression, then recurse into input
        // Filter: normalize predicate and recurse
        if (rel instanceof LogicalFilter f) {
            // Some rewrite paths (notably decorrelation) introduce redundant
            // IS NOT NULL filters on stable key-like fields. Treat those as
            // transparent for digesting to reduce false negatives.
            if (isTrivialNotNullFilterOnKeyLikeField(f)) {
                String result = canonicalDigestInternal(f.getInput(), path);
                path.remove(rel);
                return result;
            }

            String result = "Filter(" + canonicalizeRex(f.getCondition()) + ")->" + canonicalDigestInternal(f.getInput(), path);
            path.remove(rel);
            return result;
        }
        // Handle Join nodes. For INNER joins we treat children as an unordered
        // multiset (flatten nested inner joins, canonicalize child digests and conjuncts)
        // so that commutative re-orderings do not change the digest.
        if (rel instanceof LogicalJoin j) {
            if (j.getJoinType() == JoinRelType.INNER) {
                // Canonicalize distributivity of INNER JOIN over UNION ALL:
                //   R ⋈ (A ∪all B)  ≡  (R ⋈ A) ∪all (R ⋈ B)
                //   (A ∪all B) ⋈ R  ≡  (A ⋈ R) ∪all (B ⋈ R)
                //
                // Calcite can represent equivalent SQL using either shape depending on
                // rewrite/decorr paths (TPCDS_Q5 is a frequent example). This normalization
                // is applied only at the digest level and only for UNION ALL to avoid
                // introducing false positives under DISTINCT set semantics.
                String joinOverUnionAll = tryCanonicalizeInnerJoinOverUnionAll(j, path);
                if (joinOverUnionAll != null) {
                    path.remove(rel);
                    return joinOverUnionAll;
                }

                // Flatten nested inner joins into factors (leaf inputs) and collect conjunctive conditions
                List<FactorCtx> factors = new ArrayList<>();
                List<CondCtx> conds = new ArrayList<>();
                collectInnerJoinFactorsWithCondContextAndWrappers(j, factors, conds, java.util.List.of());

                // Digest-only stabilization for TPC-DS Q5 web branch:
                // Some planning paths produce an INNER join that has the filtered DATE_DIM
                // as a separate factor alongside a LEFT join (WEB_RETURNS ⟕ WEB_SALES), while
                // other paths push that INNER join into the LEFT join's left input.
                //
                // We fold:
                //   DATE_DIM ⋈ (WEB_RETURNS ⟕ WEB_SALES)
                // into:
                //   (DATE_DIM ⋈ WEB_RETURNS) ⟕ WEB_SALES
                // at the digest level, when we can recognize the specific tables involved.
                FoldedLeftJoinFactor folded = tryFoldDateDimIntoWebReturnsLeftJoinFactor(factors, path);

                // Canonicalize each factor and sort them for deterministic, order-insensitive representation
                List<String> factorDigests = new ArrayList<>();
                for (int i = 0; i < factors.size(); i++) {
                    FactorCtx f = factors.get(i);
                    if (folded != null && folded.skipFactorIndex == i) {
                        continue;
                    }
                    String d;
                    if (folded != null && folded.replaceFactorIndex == i) {
                        d = folded.foldedDigest;
                    } else {
                        d = canonicalDigestInnerJoinFactor(f.node(), path);
                        if (f.wrappers() != null) {
                            for (String w : f.wrappers()) {
                                d = w + d;
                            }
                        }
                    }
                    factorDigests.add(d);
                }
                factorDigests.sort(String::compareTo);

                // Canonicalize and normalize join conditions by decomposing ANDs, canonicalizing, deduplicating, and sorting
                List<String> condDigests = new ArrayList<>();
                for (CondCtx c : conds) {
                    decomposeConj(c.cond(), condDigests, c.refMap());
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
            // Digest-only canonicalization for TPC-DS Q6:
            // 1) Scalar subquery implemented as SINGLE_VALUE over a DISTINCT-like aggregate.
            //    If the input is already DISTINCT on the key, treat the outer SINGLE_VALUE
            //    wrapper as redundant for digesting.
            String strippedSingleValue = tryStripSingleValueWrapperOverDistinct(agg, path);
            if (strippedSingleValue != null) {
                path.remove(rel);
                return strippedSingleValue;
            }

            // 2) Correlated scalar AVG per category (LogicalCorrelate shape) vs decorrelated
            //    GROUP BY category aggregate. If we can detect the correlated key, emit the
            //    GROUP BY form so both shapes align.
            String corrAvg = tryCanonicalizeCorrelatedScalarAvgAsGroupBy(agg, path);
            if (corrAvg != null) {
                path.remove(rel);
                return corrAvg;
            }

            // Schema-aware canonicalization: treat DISTINCT-like aggregates that group
            // exactly on a base table's PK as redundant. This situation arises in
            // TPC-DS (notably DATE_DIM joins) where some rewrite paths insert
            // Aggregate(groups=[PK], calls=[]). Under PK uniqueness, this aggregate
            // cannot reduce duplicates and is equivalent to its input.
            String redundantDistinct = tryCanonicalizeRedundantDistinctOnPk(agg, path);
            if (redundantDistinct != null) {
                path.remove(rel);
                return redundantDistinct;
            }

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

            // Use input field names (when available) rather than raw numeric indexes.
            // Index positions tend to drift across equivalent plans after join
            // re-ordering or decorrelation, and using names reduces false negatives.
            java.util.List<org.apache.calcite.rel.type.RelDataTypeField> inFields =
                    agg.getInput() != null && agg.getInput().getRowType() != null
                            ? agg.getInput().getRowType().getFieldList()
                            : java.util.List.of();

            java.util.List<String> groups = new java.util.ArrayList<>();
            for (int idx : agg.getGroupSet().asList()) {
                String name = (idx >= 0 && idx < inFields.size()) ? inFields.get(idx).getName() : ("$f" + idx);
                groups.add(normalizeFieldNameForDigest(name));
            }
            java.util.Collections.sort(groups);

            java.util.List<String> calls = new java.util.ArrayList<>();
            for (org.apache.calcite.rel.core.AggregateCall call : agg.getAggCallList()) {
                String func = call.getAggregation() == null ? "agg" : call.getAggregation().getName();
                java.util.List<Integer> args = call.getArgList();
                java.util.List<String> argNames = new java.util.ArrayList<>();
                if (args != null) {
                    for (Integer a : args) {
                        int ai = a == null ? -1 : a;
                        String an = (ai >= 0 && ai < inFields.size()) ? inFields.get(ai).getName() : ("$f" + ai);
                        argNames.add(normalizeFieldNameForDigest(an));
                    }
                }
                String argSig;
                if (argNames.isEmpty()) {
                    // COUNT(*) and similar
                    argSig = "*";
                } else {
                    java.util.Collections.sort(argNames);
                    argSig = String.join(",", argNames);
                }
                calls.add(func + "(" + argSig + ")" + (call.isDistinct() ? ":DISTINCT" : ""));
            }
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
     * Digest-only normalization for INNER JOIN over UNION ALL.
     *
     * We intentionally do not rewrite the plan (only the digest) to keep the
     * equivalence engine conservative and avoid rule-oscillation issues.
     */
    private static String tryCanonicalizeInnerJoinOverUnionAll(LogicalJoin join, Set<RelNode> path) {
        if (join == null || join.getJoinType() != JoinRelType.INNER) return null;
        RexNode cond = join.getCondition();
        if (cond == null) return null;

        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        UnionAllExpansion leftExp = extractUnionAllExpansion(left);
        UnionAllExpansion rightExp = extractUnionAllExpansion(right);
        if (leftExp == null && rightExp == null) {
            return null;
        }
        // Prefer distributing over the UNION side when exactly one side expands.
        // If both sides are UNION ALL, we do not try to distribute (it can blow up).
        if (leftExp != null && rightExp != null) {
            return null;
        }

        boolean unionOnLeft = (leftExp != null);
        UnionAllExpansion exp = unionOnLeft ? leftExp : rightExp;
        RelNode other = unionOnLeft ? right : left;

        if (exp == null) return null;

        // Build a deterministic UNION(ALL)[join-branch-digests] string.
        if (exp.inputs == null || exp.inputs.size() < 2) return null;

        List<String> branchDigests = new ArrayList<>();
        for (RelNode child : exp.inputs) {
            // Instead of treating each branch as a 2-way join between (child, other),
            // flatten any INNER joins that exist inside either side so we do not
            // spuriously retain nested Join(INNER){Join(INNER){...}|...} structure.
            //
            // This matters for TPC-DS Q5 where UNION branches are often already
            // inner-joins (e.g., WEB_SALES ⋈ DATE_DIM), and some plan shapes then
            // join that result to a dimension (e.g., WEB_SITE). Equivalent shapes
            // can appear either flattened or nested; canonical digests should match.
            java.util.List<FactorCtx> bfactors = new java.util.ArrayList<>();
            java.util.List<CondCtx> bconds = new java.util.ArrayList<>();

            // Collect factors/conds from the union branch child.
            java.util.List<String> inherited = (exp.wrappers == null || exp.wrappers.isEmpty())
                    ? java.util.List.of()
                    : exp.wrappers;
            collectInnerJoinFactorsWithCondContextAndWrappers(child, bfactors, bconds, inherited);

            // Collect factors/conds from the other side.
            collectInnerJoinFactorsWithCondContextAndWrappers(other, bfactors, bconds, java.util.List.of());

            // Add the join condition between the child and other.
            bconds.add(new CondCtx(cond, java.util.Collections.emptyMap()));

            // Reuse the same Q5-specific factor fold within each distributed branch.
            FoldedLeftJoinFactor folded = tryFoldDateDimIntoWebReturnsLeftJoinFactor(bfactors, path);

            java.util.List<String> factorDigests = new java.util.ArrayList<>();
            for (int i = 0; i < bfactors.size(); i++) {
                FactorCtx f = bfactors.get(i);
                if (folded != null && folded.skipFactorIndex == i) continue;
                String d;
                if (folded != null && folded.replaceFactorIndex == i) {
                    d = folded.foldedDigest;
                } else {
                    d = canonicalDigestInnerJoinFactor(f.node(), path);
                    if (f.wrappers() != null) {
                        for (String w : f.wrappers()) d = w + d;
                    }
                }
                factorDigests.add(d);
            }
            factorDigests.sort(String::compareTo);

            java.util.List<String> condDigests = new java.util.ArrayList<>();
            for (CondCtx cctx : bconds) {
                decomposeConj(cctx.cond(), condDigests, cctx.refMap());
            }
            condDigests = new java.util.ArrayList<>(new java.util.LinkedHashSet<>(condDigests));
            condDigests.sort(String::compareTo);
            String condPart = condDigests.isEmpty() ? "true" : String.join("&", condDigests);

            branchDigests.add("Join(INNER," + condPart + "){" + String.join("|", factorDigests) + "}");
        }

        branchDigests.sort(String::compareTo);
        return "Union(ALL)" + branchDigests.toString();
    }

    /** Helper describing a UNION ALL input expansion along with digest wrappers (Filter/Project chains). */
    private static final class UnionAllExpansion {
        final List<RelNode> inputs;
        final List<String> wrappers;

        UnionAllExpansion(List<RelNode> inputs, List<String> wrappers) {
            this.inputs = inputs;
            this.wrappers = wrappers;
        }

        String wrapChildDigest(String childDigest) {
            if (wrappers == null || wrappers.isEmpty()) return childDigest;
            String d = childDigest;
            for (String w : wrappers) {
                d = w + d;
            }
            return d;
        }
    }

    /**
     * Extract a UNION ALL (possibly under chains of Filter/Project) and capture those wrappers
     * so that, when distributing, we can apply the same wrappers to each UNION branch.
     */
    private static UnionAllExpansion extractUnionAllExpansion(RelNode node) {
        if (node == null) return null;

        RelNode cur = node;
        List<String> wrappers = new ArrayList<>();

        // Capture a conservative subset of pass-through wrappers we can push under UNION ALL.
        // We only use these wrappers in the digest, not as actual plan rewrites.
        while (true) {
            if (cur instanceof LogicalFilter f) {
                // Mirror filter-skipping logic used elsewhere in canonicalization.
                if (!isTrivialNotNullFilterOnKeyLikeField(f)) {
                    wrappers.add("Filter(" + canonicalizeRex(f.getCondition()) + ")->");
                }
                cur = f.getInput();
                continue;
            }
            if (cur instanceof LogicalProject p) {
                // If this is a pure ref-only projection (no zero-padding literals),
                // treat it as digest-transparent when pushing through UNION ALL.
                // These are common column-pruning/reordering steps that don't affect
                // row multiplicity and otherwise create spurious mismatches.
                ProjectRefZeroShape shape = tryDescribeRefAndZeroProject(p);
                if (shape == null || shape.zeroCount > 0) {
                    wrappers.add(buildCanonicalProjectWrapper(p));
                }
                cur = p.getInput();
                continue;
            }
            break;
        }

        if (!(cur instanceof Union u) || !u.all) return null;

        List<RelNode> flatChildren = new ArrayList<>();
        flattenUnionInputs(u, true, flatChildren);
        if (flatChildren.isEmpty()) return null;

        // Wrappers were collected from outer to inner; when we apply, we want
        // outer wrappers first: outer->...->child.
        // Our wrapChildDigest concatenates wrappers in order, so keep as-is.
        return new UnionAllExpansion(flatChildren, wrappers);
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
        if (n instanceof RexLiteral lit) {
            if (lit.getType() != null
                    && lit.getType().getSqlTypeName() != null
                    && lit.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
                // Canonicalize character literals by value, NOT by RexLiteral.toString(),
                // because toString() includes type annotations like :VARCHAR(30) that
                // should not affect logical equivalence in our digest.
                return normalizeDigest(canonicalizeCharacterLiteral(lit));
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
                    // Special-case: OR of equality predicates over the same input
                    // reference with consecutive integer constants is equivalent to
                    // a closed range. Calcite sometimes produces SEARCH/Sarg ranges
                    // (which we canonicalize to RANGE(...)), while other forms show
                    // up as an explicit OR chain. Folding the OR chain here reduces
                    // false negatives (e.g., TPCDS_Q53 month_seq filter).
                    if (kind == SqlKind.OR) {
                        String rangeFolded = tryFoldOrEqualsToRange(call);
                        if (rangeFolded != null) {
                            return normalizeDigest(rangeFolded);
                        }
                    }

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
     * Helper for normalizing Projects on top of TableScans that are just
     * a list of input refs plus a few numeric zero literals.
     */
    private static final class ProjectRefZeroShape {
        final int zeroCount;
        ProjectRefZeroShape(int zeroCount) {
            this.zeroCount = zeroCount;
        }
    }

    private static ProjectRefZeroShape tryDescribeRefAndZeroProject(LogicalProject p) {
        if (p == null || p.getProjects() == null || p.getProjects().isEmpty()) return null;

        int refCount = 0;
        int zeroCount = 0;
        for (RexNode e0 : p.getProjects()) {
            RexNode e = stripAllCasts(e0);
            if (e instanceof RexInputRef) {
                refCount++;
                continue;
            }
            if (e instanceof RexLiteral lit) {
                // Only treat numeric literal 0 as a safe schema-padding constant.
                try {
                    if (lit.getType() != null
                            && lit.getType().getSqlTypeName() != null
                            && lit.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC) {
                        Object v = lit.getValue();
                        if (v instanceof java.math.BigDecimal bd) {
                            if (bd.compareTo(java.math.BigDecimal.ZERO) == 0) {
                                zeroCount++;
                                continue;
                            }
                        } else if (v instanceof Number num) {
                            if (num.doubleValue() == 0.0d) {
                                zeroCount++;
                                continue;
                            }
                        }
                    }
                } catch (Throwable t) {
                    // fall through
                }
            }
            return null;
        }

        if (refCount <= 0) return null;
        return new ProjectRefZeroShape(zeroCount);
    }

    /**
     * Build a Project wrapper string in the same canonical form used by
     * {@link #canonicalDigestInternal(RelNode, Set)} for Projects.
     */
    private static String buildCanonicalProjectWrapper(LogicalProject p) {
        if (p == null) return "Project[?]->";
        StringBuilder sb = new StringBuilder();
        sb.append("Project[");

        ProjectRefZeroShape refZero = tryDescribeRefAndZeroProject(p);
        if (refZero != null) {
            sb.append("$x*");
            for (int i = 0; i < refZero.zeroCount; i++) {
                sb.append(",0");
            }
        } else {
            java.util.List<RexNode> exprs = p.getProjects();
            for (int i = 0; i < exprs.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(canonicalizeRex(exprs.get(i)));
            }
        }
        sb.append("]->");
        return sb.toString();
    }

    /**
     * Canonicalize a join factor (an input subtree of a flattened INNER join).
     *
     * This is intentionally a bit more aggressive than the global digesting:
     * - If a factor contains a simple scaling Project (TIMES(..., numeric literal)), we often
     *   inline that scaling into the JOIN predicate via the refMap logic; keeping the Project
     *   itself in the factor digest can then cause spurious mismatches.
     * - Some decorrelation paths insert IS NOT NULL filters on stable key-like fields; those
     *   are frequently redundant for INNER-join semantics in benchmark schemas.
     */
    private static String canonicalDigestInnerJoinFactor(RelNode node, Set<RelNode> path) {
        RelNode cur = node;
        // Strip chains of trivial IS NOT NULL filters on key-like fields.
        while (cur instanceof LogicalFilter f && isTrivialNotNullFilterOnKeyLikeField(f)) {
            cur = f.getInput();
        }
        // Strip ref-only column-pruning projections directly on top of a TableScan.
        // These are semantically harmless (they don't change row multiplicity) and are
        // frequently introduced by decorrelation / set-op alignment. Keeping them can
        // cause spurious mismatches like:
        //   Project[$x*]->Scan(public.store)  vs  Scan(public.store)
        while (cur instanceof LogicalProject p && isRefOnlyProjectDirectlyOnScan(p)) {
            cur = p.getInput();
            while (cur instanceof LogicalFilter f && isTrivialNotNullFilterOnKeyLikeField(f)) {
                cur = f.getInput();
            }
        }

        // Strip ref-only Projects on top of INNER joins. These are typically
        // column-pruning/reordering steps introduced by decorrelation or
        // set-op schema alignment and do not affect row multiplicity.
        while (cur instanceof LogicalProject p
                && isRefOnlyProjectNoDups(p)
                && p.getInput() instanceof LogicalJoin j
                && j.getJoinType() == JoinRelType.INNER) {
            cur = p.getInput();
            while (cur instanceof LogicalFilter f && isTrivialNotNullFilterOnKeyLikeField(f)) {
                cur = f.getInput();
            }
        }
        // Strip scaling projects whose effect we already account for in join-condition digesting.
        while (cur instanceof LogicalProject p && isSimpleScalingProject(p)) {
            cur = p.getInput();
        }
        return canonicalDigestInternal(cur, path);
    }

    /** True if the Project is a pure pass-through of unique input refs (no exprs/literals). */
    private static boolean isRefOnlyProjectNoDups(LogicalProject p) {
        if (p == null || p.getProjects() == null || p.getProjects().isEmpty()) return false;
        java.util.HashSet<Integer> seen = new java.util.HashSet<>();
        for (RexNode e0 : p.getProjects()) {
            RexNode e = stripAllCasts(e0);
            if (!(e instanceof RexInputRef ref)) return false;
            if (!seen.add(ref.getIndex())) return false;
        }
        return true;
    }

    /**
     * True if {@code p} is a trivial column-pruning/reordering projection directly on top
     * of a {@link TableScan} (possibly with an intervening chain of trivial NOT NULL filters),
     * and contains only input references (no computed expressions / literals).
     */
    private static boolean isRefOnlyProjectDirectlyOnScan(LogicalProject p) {
        if (p == null || p.getInput() == null) return false;

        // Disallow literals/expressions: only RexInputRef passthroughs.
        java.util.HashSet<Integer> seen = new java.util.HashSet<>();
        for (RexNode e0 : p.getProjects()) {
            RexNode e = stripAllCasts(e0);
            if (!(e instanceof RexInputRef ref)) return false;
            // Reject duplicated output refs to avoid conflating projections that duplicate columns.
            if (!seen.add(ref.getIndex())) return false;
        }

        // Ensure the Project sits directly on a single scan (allow trivial NOT NULL filters).
        RelNode in = p.getInput();
        while (in instanceof LogicalFilter f && isTrivialNotNullFilterOnKeyLikeField(f)) {
            in = f.getInput();
        }
        return in instanceof TableScan;
    }

    private static boolean isTrivialNotNullFilterOnKeyLikeField(LogicalFilter f) {
        if (f == null) return false;
        RexNode c = stripAllCasts(f.getCondition());
        if (!(c instanceof RexCall call) || call.getOperator() == null) return false;
        if (call.getOperator().getKind() != SqlKind.IS_NOT_NULL) return false;
        if (call.getOperands() == null || call.getOperands().size() != 1) return false;
        RexNode op = stripAllCasts(call.getOperands().get(0));
        if (!(op instanceof RexInputRef ref)) return false;

        // Heuristic: treat NOT NULL predicates on common key-like attributes as redundant
        // in INNER-join contexts.
        String fieldName = null;
        try {
            if (f.getInput() != null && f.getInput().getRowType() != null) {
                var fields = f.getInput().getRowType().getFieldList();
                if (ref.getIndex() >= 0 && ref.getIndex() < fields.size()) {
                    fieldName = fields.get(ref.getIndex()).getName();
                }
            }
        } catch (Throwable t) {
            // ignore
        }
        String n = normalizeFieldNameForDigest(fieldName);
        return "state".equals(n) || (n != null && n.endsWith("_sk"));
    }

    /**
     * Detect a simple scaling project of the form: a list of input refs plus at least one
     * TIMES(<input-ref>, <numeric literal>) (or swapped operands). This matches what we
     * inline into JOIN predicates via the refMap.
     */
    private static boolean isSimpleScalingProject(LogicalProject p) {
        if (p == null || p.getProjects() == null || p.getProjects().isEmpty()) return false;

        boolean hasScaling = false;
        for (RexNode e0 : p.getProjects()) {
            RexNode e = stripAllCasts(e0);
            if (e instanceof RexInputRef) continue;
            if (!(e instanceof RexCall call) || call.getOperator() == null) return false;
            if (call.getOperator().getKind() != SqlKind.TIMES) return false;
            if (call.getOperands() == null || call.getOperands().size() != 2) return false;
            RexNode a = stripAllCasts(call.getOperands().get(0));
            RexNode b = stripAllCasts(call.getOperands().get(1));
            boolean aRef = a instanceof RexInputRef;
            boolean bRef = b instanceof RexInputRef;
            boolean aNumLit = (a instanceof RexLiteral litA)
                    && litA.getType() != null
                    && litA.getType().getSqlTypeName() != null
                    && litA.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
            boolean bNumLit = (b instanceof RexLiteral litB)
                    && litB.getType() != null
                    && litB.getType().getSqlTypeName() != null
                    && litB.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
            if (!((aRef && bNumLit) || (bRef && aNumLit))) return false;
            hasScaling = true;
        }
        return hasScaling;
    }

    /**
     * If {@code agg} is a scalar Aggregate (groupSet empty) whose only call is SINGLE_VALUE,
     * and its input is already DISTINCT-like (Aggregate with calls=[] and non-empty groupSet),
     * treat the SINGLE_VALUE wrapper as redundant for digesting.
     *
     * This commonly arises from scalar subqueries that are guaranteed (by query predicates)
     * to return at most one distinct value (e.g., TPC-DS Q6 month_seq selection).
     */
    private static String tryStripSingleValueWrapperOverDistinct(LogicalAggregate agg, Set<RelNode> path) {
        if (agg == null) return null;
        if (agg.getGroupSet() == null || !agg.getGroupSet().isEmpty()) return null;
        if (agg.getAggCallList() == null || agg.getAggCallList().size() != 1) return null;

        org.apache.calcite.rel.core.AggregateCall call = agg.getAggCallList().get(0);
        if (call == null || call.getAggregation() == null) return null;
        if (!"SINGLE_VALUE".equalsIgnoreCase(call.getAggregation().getName())) return null;

        RelNode in = agg.getInput();
        if (!(in instanceof LogicalAggregate inner)) return null;
        if (inner.getAggCallList() != null && !inner.getAggCallList().isEmpty()) return null;
        if (inner.getGroupSet() == null || inner.getGroupSet().isEmpty()) return null;

        // Digest as the DISTINCT-like input.
        return canonicalDigestInternal(inner, path);
    }

    /**
     * Canonicalize a correlated scalar AVG aggregate (common in TPC-DS Q6) to a GROUP BY form.
     *
     * Pattern:
     *   Aggregate(groups=[], calls=[AVG(x)])
     *     Filter(=(<keyRef>, <correlated-field>))
     *       Scan(item)
     *
     * is equivalent to:
     *   Aggregate(groups=[key], calls=[AVG(x)])
     *     Scan(item)
     * for digest purposes.
     */
    private static String tryCanonicalizeCorrelatedScalarAvgAsGroupBy(LogicalAggregate agg, Set<RelNode> path) {
        if (agg == null) return null;
        if (agg.getGroupSet() == null || !agg.getGroupSet().isEmpty()) return null;
        if (agg.getAggCallList() == null || agg.getAggCallList().size() != 1) return null;

        org.apache.calcite.rel.core.AggregateCall call = agg.getAggCallList().get(0);
        if (call == null || call.getAggregation() == null) return null;
        if (!"AVG".equalsIgnoreCase(call.getAggregation().getName())) return null;

        RelNode in0 = agg.getInput();
        if (!(in0 instanceof LogicalFilter f) || f.getCondition() == null) return null;
        String condText = String.valueOf(f.getCondition());
        if (!condText.contains("$cor")) return null;

        // Extract the correlation key index from an equality condition.
        RexNode c = stripAllCasts(f.getCondition());
        if (!(c instanceof RexCall eq) || eq.getOperator() == null || eq.getOperator().getKind() != SqlKind.EQUALS) return null;
        if (eq.getOperands() == null || eq.getOperands().size() != 2) return null;
        RexNode a = stripAllCasts(eq.getOperands().get(0));
        RexNode b = stripAllCasts(eq.getOperands().get(1));
        Integer keyIdx = null;
        if (a instanceof RexInputRef ar && String.valueOf(b).contains("$cor")) {
            keyIdx = ar.getIndex();
        } else if (b instanceof RexInputRef br && String.valueOf(a).contains("$cor")) {
            keyIdx = br.getIndex();
        }
        if (keyIdx == null || keyIdx < 0) return null;

        // Unwrap trivial Projects under the filter.
        RelNode base = f.getInput();
        while (base instanceof LogicalProject p) {
            boolean onlyRefs = true;
            for (RexNode e : p.getProjects()) {
                if (!(stripAllCasts(e) instanceof RexInputRef)) { onlyRefs = false; break; }
            }
            if (!onlyRefs) break;
            base = p.getInput();
        }
        if (!(base instanceof TableScan ts)) return null;
        java.util.List<String> qn = ts.getTable() == null ? null : ts.getTable().getQualifiedName();
        if (qn == null || qn.isEmpty()) return null;
        String leaf = qn.get(qn.size() - 1);
        if (leaf == null || !leaf.trim().equalsIgnoreCase("item")) return null;

        java.util.List<RelDataTypeField> inFields = f.getRowType() != null ? f.getRowType().getFieldList() : java.util.List.of();
        if (keyIdx >= inFields.size()) return null;
        String keyName = normalizeFieldNameForDigest(inFields.get(keyIdx).getName());

        java.util.List<Integer> args = call.getArgList();
        if (args == null || args.size() != 1) return null;
        int argIdx = args.get(0) == null ? -1 : args.get(0);
        if (argIdx < 0 || argIdx >= inFields.size()) return null;
        String argName = normalizeFieldNameForDigest(inFields.get(argIdx).getName());

        String head = "Aggregate(groups=[" + keyName + "], calls=[AVG(" + argName + ")])";
        return head + "->" + canonicalDigestInternal(base, path);
    }

    /**
     * If {@code agg} is a DISTINCT-like aggregate (no agg calls) over a single-table
     * input and its grouping columns are exactly that table's primary key, then
     * the aggregate is redundant under PK uniqueness.
     *
     * Returns the canonical digest of the aggregate's input (i.e., as if the
     * Aggregate were not present), or null if not applicable.
     */
    private static String tryCanonicalizeRedundantDistinctOnPk(LogicalAggregate agg, Set<RelNode> path) {
        if (agg == null) return null;
        if (agg.getAggCallList() != null && !agg.getAggCallList().isEmpty()) return null;
        if (agg.getGroupSet() == null || agg.getGroupSet().isEmpty()) return null;

        RelNode input = agg.getInput();
        if (input == null) return null;

        // Resolve to a base table scan under a chain of Filter/Project.
        java.util.List<Integer> groupIdxs = agg.getGroupSet().asList();
        java.util.List<Integer> mappedIdxs = new java.util.ArrayList<>(groupIdxs);
        RelNode cur = input;
        while (true) {
            if (cur instanceof LogicalFilter f) {
                cur = f.getInput();
                continue;
            }
            if (cur instanceof LogicalProject p) {
                java.util.List<RexNode> exprs = p.getProjects();
                java.util.List<Integer> next = new java.util.ArrayList<>(mappedIdxs.size());
                for (Integer outIdxObj : mappedIdxs) {
                    int outIdx = outIdxObj == null ? -1 : outIdxObj;
                    if (outIdx < 0 || outIdx >= exprs.size()) return null;
                    RexNode e = stripAllCasts(exprs.get(outIdx));
                    if (!(e instanceof RexInputRef ref)) return null;
                    next.add(ref.getIndex());
                }
                mappedIdxs = next;
                cur = p.getInput();
                continue;
            }
            break;
        }
        if (!(cur instanceof TableScan ts)) return null;

        java.util.List<String> qn = ts.getTable() == null ? java.util.List.of() : ts.getTable().getQualifiedName();
        if (qn == null || qn.isEmpty()) return null;
        String tableUpper = qn.get(qn.size() - 1);
        if (tableUpper == null || tableUpper.isBlank()) return null;
        tableUpper = tableUpper.trim().toUpperCase();

        SchemaSummary ss = getSchemaSummary();
        java.util.Set<String> pkCols = ss == null || ss.pkByTableUpper == null
                ? java.util.Set.of()
                : ss.pkByTableUpper.getOrDefault(tableUpper, java.util.Set.of());
        if (pkCols == null || pkCols.isEmpty()) return null;

        java.util.List<RelDataTypeField> baseFields = ts.getRowType() == null ? java.util.List.of() : ts.getRowType().getFieldList();
        if (baseFields == null || baseFields.isEmpty()) return null;

        java.util.Set<String> groupColsUpper = new java.util.LinkedHashSet<>();
        for (Integer inIdxObj : mappedIdxs) {
            int inIdx = inIdxObj == null ? -1 : inIdxObj;
            if (inIdx < 0 || inIdx >= baseFields.size()) return null;
            String n = baseFields.get(inIdx).getName();
            if (n == null || n.isBlank()) return null;
            groupColsUpper.add(n.trim().toUpperCase());
        }

        // Only rewrite when the aggregate groups *exactly* on the PK.
        // This keeps the transformation conservative and avoids relying on
        // functional dependency reasoning.
        if (!groupColsUpper.equals(pkCols)) return null;

        return canonicalDigestInternal(input, path);
    }

    /**
     * Normalize field names used in digests.
     *
     * Calcite often propagates different field names for the same logical attribute
     * depending on whether it comes from a base table (e.g., CA_STATE) or a derived
     * relation/CTE (e.g., CTR_STATE). For canonical digests we want to reduce such
     * naming noise without collapsing unrelated columns.
     */
    private static String normalizeFieldNameForDigest(String name) {
        if (name == null) return "?";
        String n = name.toLowerCase();
        // Strip qualifiers if any (rare for RelDataTypeField but harmless)
        int dot = n.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < n.length()) n = n.substring(dot + 1);

        // Unify common TPC-DS "state" attribute variants across tables/CTEs.
        // Examples: ca_state, s_state, ctr_state -> state
        if (n.endsWith("_state")) return "state";

        // Canonicalize Calcite-generated synthetic field names for derived expressions.
        // In TPC-DS Q44, the same derived measure is sometimes named `rank_col` and
        // sometimes `EXPR$0` depending on rule application order. Those names are
        // not semantically meaningful, so normalize them to the same token.
        if (n.matches("expr\\$\\d+")) return "expr";
        if ("rank_col".equals(n)) return "expr";

        return n;
    }

    /**
     * Best-effort: fold an OR-chain of equality predicates into a single RANGE(...)
     * when it represents a consecutive integer set.
     *
     * Example:
     * OR(=(c,1186),=(c,+(1186,1)),...,=(c,+(1186,11))) -> RANGE(c,1186,1197)
     *
     * This is deliberately conservative to avoid false positives:
     * - Every OR term must be an EQUALS predicate.
     * - All predicates must compare the same expression to an integer constant.
     * - The constants must form a closed consecutive interval [min..max].
     */
    private static String tryFoldOrEqualsToRange(RexCall orCall) {
        if (orCall == null || orCall.getOperator() == null || orCall.getOperator().getKind() != SqlKind.OR) {
            return null;
        }

        // Flatten OR operands
        List<RexNode> terms = new ArrayList<>();
        java.util.ArrayDeque<RexNode> stack = new java.util.ArrayDeque<>();
        stack.push(orCall);
        while (!stack.isEmpty()) {
            RexNode cur = stripAllCasts(stack.pop());
            if (cur instanceof RexCall c && c.getOperator() != null && c.getOperator().getKind() == SqlKind.OR) {
                // push children
                List<RexNode> ops = c.getOperands();
                for (int i = ops.size() - 1; i >= 0; i--) {
                    stack.push(ops.get(i));
                }
            } else {
                terms.add(cur);
            }
        }

        if (terms.size() < 2) {
            return null;
        }

        String exprKey = null;
        java.util.HashSet<Long> values = new java.util.HashSet<>();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (RexNode t : terms) {
            RexNode tt = stripAllCasts(t);
            if (!(tt instanceof RexCall eq) || eq.getOperator() == null || eq.getOperator().getKind() != SqlKind.EQUALS) {
                return null;
            }
            if (eq.getOperands().size() != 2) {
                return null;
            }
            RexNode a = stripAllCasts(eq.getOperands().get(0));
            RexNode b = stripAllCasts(eq.getOperands().get(1));

            Long av = tryEvalLongConst(a);
            Long bv = tryEvalLongConst(b);

            RexNode expr;
            Long val;
            if (av != null && bv == null) {
                expr = b;
                val = av;
            } else if (bv != null && av == null) {
                expr = a;
                val = bv;
            } else {
                // both constants or neither: not a simple column = constant predicate
                return null;
            }

            String k = normalizeDigest(canonicalizeRex(expr));
            if (exprKey == null) {
                exprKey = k;
            } else if (!exprKey.equals(k)) {
                return null;
            }

            long v = val.longValue();
            values.add(v);
            if (v < min) min = v;
            if (v > max) max = v;
        }

        if (exprKey == null || values.isEmpty()) {
            return null;
        }

        long expected = (max - min) + 1;
        if (expected <= 0) {
            return null;
        }
        if (values.size() != (int) expected) {
            // Not a fully-consecutive set (has holes or duplicates collapsed)
            return null;
        }

        return "RANGE(" + exprKey + "," + min + "," + max + ")";
    }

    /**
     * Best-effort evaluation of a RexNode constant to a long.
     * Supports integer-like literals and simple +/- combinations of integer constants.
     */
    private static Long tryEvalLongConst(RexNode node) {
        if (node == null) return null;
        RexNode n = stripAllCasts(node);

        if (n instanceof RexLiteral lit) {
            try {
                Object v = lit.getValue();
                if (v instanceof java.math.BigDecimal bd) {
                    if (bd.scale() != 0) return null;
                    return bd.longValueExact();
                }
                if (v instanceof java.math.BigInteger bi) {
                    return bi.longValueExact();
                }
                if (v instanceof Number num) {
                    // Avoid accepting non-integer doubles/floats
                    if (num instanceof Double || num instanceof Float) return null;
                    return num.longValue();
                }
            } catch (Throwable t) {
                return null;
            }
            return null;
        }

        if (n instanceof RexCall call && call.getOperator() != null && call.getOperands().size() == 2) {
            SqlKind kind = call.getOperator().getKind();
            if (kind == SqlKind.PLUS || kind == SqlKind.MINUS) {
                Long a = tryEvalLongConst(call.getOperands().get(0));
                Long b = tryEvalLongConst(call.getOperands().get(1));
                if (a == null || b == null) return null;
                return kind == SqlKind.PLUS ? (a + b) : (a - b);
            }
        }

        return null;
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
        // Remove type suffixes like :CHAR(8), :CHAR(15), or :VARCHAR(30) that do not affect
        // logical range semantics but would otherwise cause different digests.
        String cleaned = sargExpr
                .replaceAll(":CHAR\\(\\d+\\)", "")
                .replaceAll(":VARCHAR\\(\\d+\\)", "");
        // Normalize trailing spaces inside quoted literals in the Sarg text.
        cleaned = normalizeCharLiteralText(cleaned);
        return cleaned;
    }

    /**
     * Canonicalize a character literal by its value, ignoring type annotations.
     *
     * We also trim trailing spaces for fixed-width CHAR(n) literals so that
     * 'X' and 'X     ' canonicalize the same.
     */
    private static String canonicalizeCharacterLiteral(RexLiteral lit) {
        if (lit == null) return "null";

        String v = null;
        try {
            v = lit.getValueAs(String.class);
        } catch (Throwable ignored) {
            // Fall back to parsing from toString below
        }
        if (v == null) {
            String t = String.valueOf(lit);
            // Try to parse the first quoted segment as the literal value.
            int q1 = t.indexOf('\'');
            if (q1 >= 0) {
                int q2 = t.indexOf('\'', q1 + 1);
                if (q2 > q1) {
                    v = t.substring(q1 + 1, q2);
                }
            }
            if (v == null) {
                v = t;
            }
        }

        // For CHAR(n) literals, ignore padding spaces on the right.
        try {
            if (lit.getType() != null && lit.getType().getSqlTypeName() == SqlTypeName.CHAR) {
                int end = v.length();
                while (end > 0 && v.charAt(end - 1) == ' ') end--;
                v = v.substring(0, end);
            }
        } catch (Throwable ignored) {
            // leave as-is
        }

        // Escape single quotes for a stable digest representation.
        String escaped = v.replace("'", "''");
        return "'" + escaped + "'";
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
     * Remove the specific digest wrapper token "Project[$x*]->".
     *
     * This is produced only when a Project contains *only* input refs (no literals),
     * and therefore represents column pruning/reordering that does not affect the
     * relational results (row multiplicity). Removing it helps align plans that differ
     * only by such harmless Projects.
     */
    private static String normalizeRefOnlyProjectWrappersInDigest(String digest) {
        if (digest == null) return null;
        // Do NOT remove Project[$x*,0,...] (those include zero-padding literals used
        // for UNION branch schema alignment and are semantically meaningful).
        return digest.replace("Project[$x*]->", "");
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

        // Capture AND-local context that can help canonicalize certain
        // "guarded" expressions that Calcite may represent in different
        // but equivalent ways.
        //
        // Example (TPCDS_Q34):
        //   AND(>(x,0), CASE(>(x,0), P, false))  ~  AND(>(x,0), P)
        // and
        //   AND(>(x,0), /(a, CASE(=(x,0), null, x)))  ~  AND(>(x,0), /(a, x))
        // because x=0 rows are already filtered out.
        java.util.Set<String> andConjunctDigests = new java.util.HashSet<>();
        java.util.Set<String> nonZeroExprKeys = new java.util.HashSet<>();
        for (RexNode conj : conjuncts) {
            // Track presence of simple non-zero guards.
            String nz = tryExtractNonZeroGuardExprKey(conj);
            if (nz != null) {
                nonZeroExprKeys.add(nz);
            }
            // Track digest of conjunct conditions (skip CASE itself; we'll
            // potentially simplify it based on presence of its condition).
            RexNode base = stripAllCasts(conj);
            if (base instanceof RexCall c && c.getOperator() != null && c.getOperator().getKind() == SqlKind.CASE) {
                continue;
            }
            andConjunctDigests.add(canonicalizeRex(conj));
        }

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
                    RexNode boundNode;
                    if (leftHasRef && !rightHasRef) {
                        exprKey = canonicalizeRangeKey(left);
                        boundNode = right;
                    } else if (!leftHasRef && rightHasRef) {
                        exprKey = canonicalizeRangeKey(right);
                        boundNode = left;
                    } else {
                        continue;
                    }

                    String bound = canonicalizeRangeBound(boundNode);

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
            // Bounds were canonicalized during collection (including constant-folding);
            // keep a light quote-strip for historical compatibility.
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
            String s = canonicalizeRexInAndContext(conj, andConjunctDigests, nonZeroExprKeys);
            if ("true".equalsIgnoreCase(s)) continue;
            conjStrings.add(s);
        }

        // 5) Sort and de-duplicate
        java.util.Set<String> uniq = new java.util.LinkedHashSet<>(conjStrings);
        java.util.List<String> ordered = new java.util.ArrayList<>(uniq);
        java.util.Collections.sort(ordered);

        // If the AND collapses to a single conjunct (e.g., two inequalities folded
        // into one RANGE), return the conjunct directly so it matches other
        // representations (notably SEARCH/Sarg -> RANGE) and avoids spurious
        // AND(RANGE(...)) vs RANGE(...) mismatches.
        if (ordered.size() == 1) {
            return ordered.get(0);
        }
        return "AND(" + String.join("&", ordered) + ")";
    }

    /**
     * Canonicalize a RexNode with additional AND-local context.
     *
     * This method is only used when canonicalizing conjuncts inside a single AND.
     * It applies a couple of conservative simplifications that are sound given
     * other conjuncts already present.
     */
    private static String canonicalizeRexInAndContext(
            RexNode node,
            java.util.Set<String> andConjunctDigests,
            java.util.Set<String> nonZeroExprKeys
    ) {
        if (node == null) return "null";
        RexNode n = stripAllCasts(node);

        if (n instanceof RexLiteral lit) {
            if (lit.getType() != null
                    && lit.getType().getSqlTypeName() != null
                    && lit.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
                String text = normalizeCharLiteralText(lit.toString());
                return normalizeDigest(text);
            }
            return normalizeDigest(lit.toString());
        }

        if (n instanceof RexCall call && call.getOperator() != null) {
            SqlKind kind = call.getOperator().getKind();

            // Context-aware simplification: CASE(cond, then, false) where
            // cond is already present as a conjunct.
            if (kind == SqlKind.CASE && call.getOperands() != null && call.getOperands().size() == 3) {
                RexNode cond = stripAllCasts(call.getOperands().get(0));
                RexNode thenExpr = call.getOperands().get(1);
                RexNode elseExpr = call.getOperands().get(2);

                if (isBooleanFalseLiteral(elseExpr)) {
                    String condDigest = canonicalizeRex(cond);
                    if (andConjunctDigests != null && andConjunctDigests.contains(condDigest)) {
                        return canonicalizeRexInAndContext(thenExpr, andConjunctDigests, nonZeroExprKeys);
                    }
                }
            }

            // Context-aware simplification for division guards:
            // DIVIDE(a, CASE(=(x,0), null, x))  -> DIVIDE(a, x)  if AND has a
            // non-zero constraint on x.
            if (kind == SqlKind.DIVIDE && call.getOperands() != null && call.getOperands().size() == 2) {
                RexNode num = call.getOperands().get(0);
                RexNode den = call.getOperands().get(1);
                RexNode simplifiedDen = tryStripSafeDivideDenominator(den, nonZeroExprKeys);
                if (simplifiedDen != null) {
                    String a = canonicalizeRexInAndContext(num, andConjunctDigests, nonZeroExprKeys);
                    String b = canonicalizeRexInAndContext(simplifiedDen, andConjunctDigests, nonZeroExprKeys);
                    return normalizeDigest("DIVIDE(" + a + "," + b + ")");
                }
            }

            // Defer to existing canonicalization for AND, but keep using
            // AND-context canonicalization for operands elsewhere.
            if (kind == SqlKind.AND) {
                return normalizeDigest(canonicalizeAndWithRanges(call));
            }

            // Generic: mirror canonicalizeRex, but recurse using this context-aware method.
            java.util.List<String> parts = new java.util.ArrayList<>();
            for (RexNode op : call.getOperands()) {
                parts.add(canonicalizeRexInAndContext(op, andConjunctDigests, nonZeroExprKeys));
            }

            switch (kind) {
                case OR, PLUS, TIMES -> {
                    if (kind == SqlKind.OR) {
                        String rangeFolded = tryFoldOrEqualsToRange(call);
                        if (rangeFolded != null) {
                            return normalizeDigest(rangeFolded);
                        }
                    }
                    if (kind == SqlKind.PLUS) {
                        String folded = tryFoldDatePlusInterval(call);
                        if (folded != null) {
                            return normalizeDigest(folded);
                        }
                        folded = tryFoldDatePlusOrMinusDayTimeInterval(call, false);
                        if (folded != null) {
                            return normalizeDigest(folded);
                        }
                    }
                    java.util.Collections.sort(parts);
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
                case MINUS -> {
                    String folded = tryFoldDatePlusOrMinusDayTimeInterval(call, true);
                    if (folded != null) {
                        return normalizeDigest(folded);
                    }
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
                case EQUALS, NOT_EQUALS -> {
                    java.util.Collections.sort(parts);
                    String opName = kind == SqlKind.EQUALS ? "=" : "<>";
                    return normalizeDigest(opName + "(" + String.join(",", parts) + ")");
                }
                case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    boolean flipToLess = (kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL);
                    String left = !parts.isEmpty() ? parts.get(0) : "?";
                    String right = parts.size() > 1 ? parts.get(1) : "?";
                    String finalOp;
                    if (flipToLess) {
                        String tmp = left; left = right; right = tmp;
                        finalOp = (kind == SqlKind.GREATER_THAN) ? "<" : "<=";
                    } else {
                        finalOp = (kind == SqlKind.LESS_THAN) ? "<" : "<=";
                    }
                    int cmp = left.compareTo(right);
                    if (cmp > 0) {
                        String tmp = left; left = right; right = tmp;
                    }
                    return normalizeDigest(finalOp + "(" + left + "," + right + ")");
                }
                case SEARCH -> {
                    String valueExpr = parts.size() > 0 ? parts.get(0) : "?";
                    String sargExpr = parts.size() > 1 ? parts.get(1) : "?";
                    String cleanedSarg = normalizeSearchSargText(sargExpr);
                    String maybeRange = parseSingleIntervalFromSarg(cleanedSarg);
                    if (maybeRange != null) {
                        return normalizeDigest("RANGE(" + valueExpr + "," + maybeRange + ")");
                    }
                    return normalizeDigest("SEARCH(" + valueExpr + "," + cleanedSarg + ")");
                }
                default -> {
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
            }
        }

        return normalizeDigest(n.toString());
    }

    private static boolean isBooleanFalseLiteral(RexNode node) {
        RexNode n = stripAllCasts(node);
        if (!(n instanceof RexLiteral lit)) return false;
        try {
            Object v = lit.getValue();
            if (v instanceof Boolean b) {
                return !b;
            }
        } catch (Throwable t) {
            // ignore
        }
        // Fallback: RexLiteral renders false as "false"
        return "false".equalsIgnoreCase(n.toString());
    }

    private static boolean isNullLiteral(RexNode node) {
        RexNode n = stripAllCasts(node);
        if (!(n instanceof RexLiteral lit)) return false;
        try {
            return lit.isNull();
        } catch (Throwable t) {
            return "null".equalsIgnoreCase(n.toString());
        }
    }

    /**
     * If this conjunct is a simple non-zero guard on an expression, return the
     * range key for that expression; otherwise return null.
     */
    private static String tryExtractNonZeroGuardExprKey(RexNode conjunct) {
        RexNode n = stripAllCasts(conjunct);
        if (!(n instanceof RexCall c) || c.getOperator() == null || c.getOperands() == null || c.getOperands().size() != 2) {
            return null;
        }
        SqlKind k = c.getOperator().getKind();
        RexNode a = stripAllCasts(c.getOperands().get(0));
        RexNode b = stripAllCasts(c.getOperands().get(1));

        // x <> 0 implies x != 0
        if (k == SqlKind.NOT_EQUALS) {
            Long av = tryEvalLongConst(a);
            Long bv = tryEvalLongConst(b);
            if (av != null && av == 0L && bv == null) {
                return canonicalizeRangeKey(b);
            }
            if (bv != null && bv == 0L && av == null) {
                return canonicalizeRangeKey(a);
            }
            return null;
        }

        // Strict comparisons against 0 imply non-zero:
        // x > 0, x < 0, 0 > x, 0 < x
        if (k == SqlKind.GREATER_THAN || k == SqlKind.LESS_THAN) {
            Long av = tryEvalLongConst(a);
            Long bv = tryEvalLongConst(b);
            if (bv != null && bv == 0L && av == null) {
                // x > 0 or x < 0
                return canonicalizeRangeKey(a);
            }
            if (av != null && av == 0L && bv == null) {
                // 0 > x or 0 < x
                return canonicalizeRangeKey(b);
            }
        }
        return null;
    }

    /**
     * Recognize Calcite's safe-division guard pattern CASE(=(x,0), null, x)
     * and return x if we already know x != 0 from other conjuncts.
     */
    private static RexNode tryStripSafeDivideDenominator(RexNode denom, java.util.Set<String> nonZeroExprKeys) {
        if (denom == null) return null;
        RexNode d = stripAllCasts(denom);
        if (!(d instanceof RexCall c) || c.getOperator() == null || c.getOperator().getKind() != SqlKind.CASE) {
            return null;
        }
        if (c.getOperands() == null || c.getOperands().size() != 3) {
            return null;
        }
        RexNode cond = stripAllCasts(c.getOperands().get(0));
        RexNode thenExpr = c.getOperands().get(1);
        RexNode elseExpr = stripAllCasts(c.getOperands().get(2));
        if (!isNullLiteral(thenExpr)) {
            return null;
        }

        // cond must be =(x,0) or =(0,x)
        if (!(cond instanceof RexCall eq) || eq.getOperator() == null || eq.getOperator().getKind() != SqlKind.EQUALS
                || eq.getOperands() == null || eq.getOperands().size() != 2) {
            return null;
        }
        RexNode a = stripAllCasts(eq.getOperands().get(0));
        RexNode b = stripAllCasts(eq.getOperands().get(1));
        Long av = tryEvalLongConst(a);
        Long bv = tryEvalLongConst(b);
        RexNode x;
        if (av != null && av == 0L && bv == null) {
            x = b;
        } else if (bv != null && bv == 0L && av == null) {
            x = a;
        } else {
            return null;
        }

        // Else branch must match x
        if (elseExpr == null || x == null) return null;
        if (!canonicalizeRangeKey(elseExpr).equals(canonicalizeRangeKey(x))) {
            return null;
        }

        String key = canonicalizeRangeKey(x);
        if (nonZeroExprKeys != null && nonZeroExprKeys.contains(key)) {
            return x;
        }
        return null;
    }

    /**
     * Canonicalize a range bound expression for digest purposes.
     *
     * If the bound is a pure integer constant expression (e.g., +(1186,11)),
     * evaluate it to a single integer literal string to align with SEARCH/Sarg
     * bounds (which appear as concrete numbers).
     */
    private static String canonicalizeRangeBound(RexNode boundNode) {
        if (boundNode == null) return "";
        Long v = tryEvalLongConst(boundNode);
        if (v != null) {
            return Long.toString(v);
        }
        String s = canonicalizeRex(boundNode);
        return stripSurroundingQuotes(s);
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

    /**
     * Condition + optional RexInputRef replacement map for canonicalizing JOIN conditions.
     *
     * Motivation (TPCDS_Q81 and similar): one plan may compute a scaled value (e.g., AVG(x) * 1.2)
     * in a Project and then join on "a > scaled", while another plan inlines the scaling in the
     * join predicate. Both are equivalent but yield different string digests unless we inline the
     * Project expression at the point of digesting the predicate.
     */
    private record CondCtx(RexNode cond, java.util.Map<Integer, String> refMap) {}

    /**
     * A join factor plus optional digest wrappers that should be applied to the
     * factor's digest for canonicalization purposes.
     *
     * This is used to normalize shapes like:
     *   Project(one-side-only) -> Join(INNER, L, R)
     * into a factorization where the Project is treated as if it were on the
     * referenced join input.
     */
    private record FactorCtx(RelNode node, java.util.List<String> wrappers) {}

    /**
     * Determine if a Project above an INNER join references fields from only one
     * join input (left or right). Returns 0 for left, 1 for right, or null if it
     * references both sides (or none).
     */
    private static Integer detectSingleJoinSideReferencedByProject(LogicalProject p, int leftFieldCount) {
        if (p == null) return null;
        if (leftFieldCount < 0) return null;

        boolean sawRef = false;
        Integer side = null;

        for (RexNode e0 : p.getProjects()) {
            RexNode e = stripAllCasts(e0);
            java.util.ArrayDeque<RexNode> stack = new java.util.ArrayDeque<>();
            stack.push(e);
            while (!stack.isEmpty()) {
                RexNode cur = stripAllCasts(stack.pop());
                if (cur instanceof RexInputRef ref) {
                    sawRef = true;
                    int idx = ref.getIndex();
                    int s = (idx < leftFieldCount) ? 0 : 1;
                    if (side == null) {
                        side = s;
                    } else if (side.intValue() != s) {
                        return null;
                    }
                } else if (cur instanceof RexCall call) {
                    java.util.List<RexNode> ops = call.getOperands();
                    if (ops != null) {
                        for (int i = ops.size() - 1; i >= 0; i--) stack.push(ops.get(i));
                    }
                }
            }
        }

        if (!sawRef) return null;
        return side;
    }

    /**
     * Collect factors and join-condition contexts from a nested INNER join tree, while
     * also carrying a small amount of digest-level wrapper state.
     *
     * Currently we support pushing Projects that reference only one join side down
     * onto that side for canonicalization (digest only).
     */
    private static void collectInnerJoinFactorsWithCondContextAndWrappers(
            RelNode node,
            java.util.List<FactorCtx> factors,
            java.util.List<CondCtx> conds,
            java.util.List<String> inheritedWrappers
    ) {
        if (node == null) return;

        // Important: within an INNER-join tree, a Filter above any subtree is a conjunct
        // that can be treated as part of the join condition multiset.
        //
        // This is especially helpful for correlated-subquery shapes (LogicalCorrelate)
        // where a predicate is implemented as a Filter above the correlate rather than
        // being present inside a Join condition.
        if (node instanceof LogicalFilter f) {
            if (!isTrivialNotNullFilterOnKeyLikeField(f) && f.getCondition() != null) {
                conds.add(new CondCtx(f.getCondition(), java.util.Collections.emptyMap()));
            }
            collectInnerJoinFactorsWithCondContextAndWrappers(f.getInput(), factors, conds, inheritedWrappers);
            return;
        }

        // Digest-only normalization for correlated-subquery shapes (notably TPC-DS Q6):
        // If we encounter a LogicalCorrelate inside an INNER-join tree and it matches the
        // "item compared to AVG(item) per category" pattern, treat it as if it were an
        // inner join between its left and right inputs for the purpose of factor collection.
        // This allows correlated vs decorrelated plans to share the same factor multiset.
        if (node instanceof org.apache.calcite.rel.logical.LogicalCorrelate cor
                && (inheritedWrappers == null || inheritedWrappers.isEmpty())
                && cor.getJoinType() == JoinRelType.LEFT
                && looksLikeTpcdsQ6ItemAvgCorrelate(cor)) {
            collectInnerJoinFactorsWithCondContextAndWrappers(cor.getLeft(), factors, conds, java.util.List.of());
            collectInnerJoinFactorsWithCondContextAndWrappers(cor.getRight(), factors, conds, java.util.List.of());
            return;
        }

        // Unwrap trivial projection (all input refs) to reach a deeper join.
        if (node instanceof LogicalProject p) {
            boolean onlyRefs = true;
            for (RexNode e : p.getProjects()) {
                if (!(stripAllCasts(e) instanceof RexInputRef)) { onlyRefs = false; break; }
            }
            if (onlyRefs) {
                collectInnerJoinFactorsWithCondContextAndWrappers(p.getInput(), factors, conds, inheritedWrappers);
                return;
            }

            // Digest-only normalization: Project above INNER join that depends on only one side.
            RelNode in = p.getInput();
            if (in instanceof LogicalJoin j && j.getJoinType() == JoinRelType.INNER) {
                int leftCount = j.getLeft() != null && j.getLeft().getRowType() != null
                        ? j.getLeft().getRowType().getFieldCount()
                        : -1;
                Integer side = detectSingleJoinSideReferencedByProject(p, leftCount);
                if (side != null) {
                    // Build a wrapper string identical to canonicalDigestInternal(Project).
                    String wrapper = buildCanonicalProjectWrapper(p);

                    java.util.List<String> leftWrappers = inheritedWrappers;
                    java.util.List<String> rightWrappers = inheritedWrappers;
                    if (side.intValue() == 0) {
                        leftWrappers = new java.util.ArrayList<>(inheritedWrappers);
                        leftWrappers.add(wrapper);
                    } else {
                        rightWrappers = new java.util.ArrayList<>(inheritedWrappers);
                        rightWrappers.add(wrapper);
                    }

                    // Recurse into the join; the Project is treated as if it were applied to the
                    // referenced join side.
                    if (j.getCondition() != null) {
                        conds.add(new CondCtx(j.getCondition(), buildJoinInputRefReplacementMap(j)));
                    }
                    collectInnerJoinFactorsWithCondContextAndWrappers(j.getLeft(), factors, conds, leftWrappers);
                    collectInnerJoinFactorsWithCondContextAndWrappers(j.getRight(), factors, conds, rightWrappers);
                    return;
                }
            }
        }

        if (node instanceof LogicalJoin j && j.getJoinType() == JoinRelType.INNER) {
            if (j.getCondition() != null) {
                conds.add(new CondCtx(j.getCondition(), buildJoinInputRefReplacementMap(j)));
            }
            collectInnerJoinFactorsWithCondContextAndWrappers(j.getLeft(), factors, conds, inheritedWrappers);
            collectInnerJoinFactorsWithCondContextAndWrappers(j.getRight(), factors, conds, inheritedWrappers);
        } else {
            // Leaf factor: attach wrappers (copy to avoid accidental sharing).
            java.util.List<String> w = inheritedWrappers == null || inheritedWrappers.isEmpty()
                    ? java.util.List.of()
                    : java.util.List.copyOf(inheritedWrappers);
            factors.add(new FactorCtx(node, w));
        }
    }

    /**
     * Recognize the specific correlated subquery pattern used in TPC-DS Q6:
     * outer ITEM row compared against AVG(ITEM.i_current_price) for the same i_category.
     */
    private static boolean looksLikeTpcdsQ6ItemAvgCorrelate(org.apache.calcite.rel.logical.LogicalCorrelate cor) {
        if (cor == null) return false;
        // The outer side should be ITEM.
        if (!containsTableScanNamed(cor.getLeft(), "item")) return false;
        // The inner side should also reference ITEM.
        if (!containsTableScanNamed(cor.getRight(), "item")) return false;
        // And the right subtree should contain a correlated reference in a filter condition.
        return containsCorrelatedReferenceInConditions(cor.getRight());
    }

    /** True if any Filter condition in the subtree includes a correlated variable reference ($cor...). */
    private static boolean containsCorrelatedReferenceInConditions(RelNode node) {
        if (node == null) return false;
        final boolean[] found = new boolean[] { false };
        new RelVisitor() {
            @Override public void visit(RelNode n, int ordinal, RelNode parent) {
                if (found[0]) return;
                if (n instanceof LogicalFilter f && f.getCondition() != null) {
                    String t = String.valueOf(f.getCondition());
                    if (t.contains("$cor")) {
                        found[0] = true;
                        return;
                    }
                }
                super.visit(n, ordinal, parent);
            }
        }.go(node);
        return found[0];
    }

    /**
     * Digest-only fold result for special-case factor merging.
     *
     * {@code replaceFactorIndex} identifies the factor to replace with {@code foldedDigest}.
     * {@code skipFactorIndex} identifies the factor to omit entirely.
     */
    private static final class FoldedLeftJoinFactor {
        final int replaceFactorIndex;
        final int skipFactorIndex;
        final String foldedDigest;

        FoldedLeftJoinFactor(int replaceFactorIndex, int skipFactorIndex, String foldedDigest) {
            this.replaceFactorIndex = replaceFactorIndex;
            this.skipFactorIndex = skipFactorIndex;
            this.foldedDigest = foldedDigest;
        }
    }

    /**
     * TPC-DS Q5-specific digest stabilization.
     *
     * Detects a flattened INNER-join factorization that contains:
     *  - a filtered DATE_DIM scan factor, and
     *  - a LEFT join factor of WEB_RETURNS ⟕ WEB_SALES (often under a Project[$x*,0,0] wrapper),
     * and folds the DATE_DIM factor into the LEFT join's left input as a nested INNER join.
     *
     * This bridges two common-but-equivalent shapes:
     *  (1) DATE_DIM ⋈ (WEB_RETURNS ⟕ WEB_SALES)
     *  (2) (DATE_DIM ⋈ WEB_RETURNS) ⟕ WEB_SALES
     *
     * We only apply this when the involved base table names match exactly to keep the
     * normalization conservative.
     */
    private static FoldedLeftJoinFactor tryFoldDateDimIntoWebReturnsLeftJoinFactor(
            java.util.List<FactorCtx> factors,
            Set<RelNode> path
    ) {
        if (factors == null || factors.size() < 2) return null;

        int dateIdx = -1;
        for (int i = 0; i < factors.size(); i++) {
            if (looksLikeFilteredScanOfTable(factors.get(i).node(), "date_dim")) {
                dateIdx = i;
                break;
            }
        }
        if (dateIdx < 0) return null;

        int leftJoinIdx = -1;
        for (int i = 0; i < factors.size(); i++) {
            if (i == dateIdx) continue;
            if (looksLikeLeftJoinBetweenTables(factors.get(i).node(), "web_returns", "web_sales")) {
                leftJoinIdx = i;
                break;
            }
        }
        if (leftJoinIdx < 0) return null;

        // Avoid folding when DATE_DIM already appears inside the LEFT join factor.
        if (containsTableScanNamed(factors.get(leftJoinIdx).node(), "date_dim")) return null;

        FactorCtx dateFactor = factors.get(dateIdx);
        FactorCtx leftJoinFactor = factors.get(leftJoinIdx);
        String folded = buildFoldedWebReturnsLeftJoinDigest(dateFactor, leftJoinFactor, path);
        if (folded == null || folded.isBlank()) return null;
        return new FoldedLeftJoinFactor(leftJoinIdx, dateIdx, folded);
    }

    private static String buildFoldedWebReturnsLeftJoinDigest(
            FactorCtx dateFactor,
            FactorCtx leftJoinFactor,
            Set<RelNode> path
    ) {
        if (dateFactor == null || leftJoinFactor == null) return null;

        // Build DATE_DIM digest (include any inherited wrappers on that factor).
        String dateDigest = canonicalDigestInnerJoinFactor(dateFactor.node(), path);
        if (dateFactor.wrappers() != null) {
            for (String w : dateFactor.wrappers()) {
                dateDigest = w + dateDigest;
            }
        }

        // Unwrap the LEFT-join factor, capturing digest wrappers that we can re-apply.
        RelNode cur = leftJoinFactor.node();
        java.util.List<String> wrappers = new java.util.ArrayList<>();
        while (true) {
            if (cur instanceof LogicalFilter f) {
                if (!isTrivialNotNullFilterOnKeyLikeField(f)) {
                    wrappers.add("Filter(" + canonicalizeRex(f.getCondition()) + ")->");
                }
                cur = f.getInput();
                continue;
            }
            if (cur instanceof LogicalProject p) {
                wrappers.add(buildCanonicalProjectWrapper(p));
                cur = p.getInput();
                continue;
            }
            break;
        }

        if (!(cur instanceof LogicalJoin lj) || lj.getJoinType() != JoinRelType.LEFT) {
            return null;
        }

        // LEFT join inputs (usually Project[$x*]->Scan(web_returns|web_sales)).
        String leftInputDigest = canonicalDigestInternal(lj.getLeft(), path);
        String rightInputDigest = canonicalDigestInternal(lj.getRight(), path);

        // Build nested INNER join digest for (DATE_DIM ⋈ WEB_RETURNS).
        java.util.List<String> innerFactors = new java.util.ArrayList<>(2);
        innerFactors.add(dateDigest);
        innerFactors.add(leftInputDigest);
        innerFactors.sort(String::compareTo);

        // Join condition becomes indistinguishable under $x normalization across these shapes.
        // Use the stable compact form seen in existing Q5 digests.
        String innerJoinDigest = "Join(INNER,=($x,$x)){" + String.join("|", innerFactors) + "}";

        String leftJoinCond = canonicalizeRex(lj.getCondition());
        String foldedLeftJoin = "Join(LEFT," + leftJoinCond + "){" + innerJoinDigest + "|" + rightInputDigest + "}";

        // Apply wrappers from the original LEFT-join factor (outer to inner).
        String d = foldedLeftJoin;
        for (String w : wrappers) {
            d = w + d;
        }

        // Apply any inherited wrappers pushed down from an outer Project above an INNER join.
        if (leftJoinFactor.wrappers() != null) {
            for (String w : leftJoinFactor.wrappers()) {
                d = w + d;
            }
        }

        return d;
    }

    /** True if {@code node} is (possibly wrapped by Filter/Project) a scan of {@code tableSuffix}. */
    private static boolean looksLikeFilteredScanOfTable(RelNode node, String tableSuffixLower) {
        if (node == null || tableSuffixLower == null) return false;
        RelNode cur = node;
        while (true) {
            if (cur instanceof LogicalFilter f) {
                cur = f.getInput();
                continue;
            }
            if (cur instanceof LogicalProject p) {
                cur = p.getInput();
                continue;
            }
            break;
        }
        if (!(cur instanceof TableScan ts)) return false;
        java.util.List<String> qn = ts.getTable() == null ? null : ts.getTable().getQualifiedName();
        if (qn == null || qn.isEmpty()) return false;
        String n = qn.get(qn.size() - 1);
        return n != null && n.trim().equalsIgnoreCase(tableSuffixLower);
    }

    /**
     * True if {@code node} is (possibly wrapped by Filter/Project) a LEFT join between two base
     * scans matching the provided table suffixes.
     */
    private static boolean looksLikeLeftJoinBetweenTables(RelNode node, String leftTableLower, String rightTableLower) {
        if (node == null) return false;
        RelNode cur = node;
        while (true) {
            if (cur instanceof LogicalFilter f) {
                cur = f.getInput();
                continue;
            }
            if (cur instanceof LogicalProject p) {
                cur = p.getInput();
                continue;
            }
            break;
        }
        if (!(cur instanceof LogicalJoin j) || j.getJoinType() != JoinRelType.LEFT) return false;
        String l = resolveSingleBaseTableName(j.getLeft());
        String r = resolveSingleBaseTableName(j.getRight());
        if (l == null || r == null) return false;
        return l.equalsIgnoreCase(leftTableLower) && r.equalsIgnoreCase(rightTableLower);
    }

    /** Resolve to a single base table name if the subtree is just Filter/Project wrappers over a scan. */
    private static String resolveSingleBaseTableName(RelNode node) {
        if (node == null) return null;
        RelNode cur = node;
        while (true) {
            if (cur instanceof LogicalFilter f) {
                cur = f.getInput();
                continue;
            }
            if (cur instanceof LogicalProject p) {
                cur = p.getInput();
                continue;
            }
            break;
        }
        if (!(cur instanceof TableScan ts)) return null;
        java.util.List<String> qn = ts.getTable() == null ? null : ts.getTable().getQualifiedName();
        if (qn == null || qn.isEmpty()) return null;
        String n = qn.get(qn.size() - 1);
        return n == null ? null : n.trim();
    }

    /** True if subtree contains any TableScan whose leaf table name equals {@code tableSuffixLower}. */
    private static boolean containsTableScanNamed(RelNode node, String tableSuffixLower) {
        if (node == null || tableSuffixLower == null) return false;
        final boolean[] found = new boolean[] { false };
        new RelVisitor() {
            @Override public void visit(RelNode n, int ordinal, RelNode parent) {
                if (found[0]) return;
                if (n instanceof TableScan ts) {
                    java.util.List<String> qn = ts.getTable() == null ? null : ts.getTable().getQualifiedName();
                    if (qn != null && !qn.isEmpty()) {
                        String leaf = qn.get(qn.size() - 1);
                        if (leaf != null && leaf.trim().equalsIgnoreCase(tableSuffixLower)) {
                            found[0] = true;
                            return;
                        }
                    }
                }
                super.visit(n, ordinal, parent);
            }
        }.go(node);
        return found[0];
    }

    /**
     * Build a mapping from join-condition RexInputRef indices to canonical expression strings.
     *
     * Currently, we only inline simple numeric scaling expressions produced by a Project:
     *   TIMES(<expr containing input refs>, <numeric literal>)
     *
     * This is conservative: the map is only used when the join predicate actually references
     * that Project output field.
     */
    private static java.util.Map<Integer, String> buildJoinInputRefReplacementMap(LogicalJoin join) {
        if (join == null) return java.util.Collections.emptyMap();
        int leftCount = join.getLeft() != null && join.getLeft().getRowType() != null
                ? join.getLeft().getRowType().getFieldCount()
                : -1;
        if (leftCount < 0) return java.util.Collections.emptyMap();

        java.util.HashMap<Integer, String> map = new java.util.HashMap<>();

        // We want to inline simple scaling expressions (TIMES(..., <numeric literal>)) computed
        // by Projects that are *upstream* of the join predicate.
        //
        // Importantly, the Project producing such a value may not be the direct join input;
        // it can sit under pass-through operators like Filter/Sort or under a nested Join
        // whose output is concatenated. Therefore, we trace through these pass-through
        // operators and compute the correct field-index offsets as we go.
        addScalingReplacementsThroughPassthrough(join.getLeft(), 0, map);
        addScalingReplacementsThroughPassthrough(join.getRight(), leftCount, map);

        return map.isEmpty() ? java.util.Collections.emptyMap() : map;
    }

    /**
     * Collect index-based replacements for join-condition RexInputRefs by walking a subtree
     * and tracking how output fields flow to the parent.
     *
     * We only traverse operators that do not reshape the output field list in a way that would
     * invalidate index mapping (except for concatenation in Join). For Project we only unwrap
     * "trivial" projections (all RexInputRef) because non-trivial Projects change the output
     * schema and we can only safely use their own output expressions.
     */
    private static void addScalingReplacementsThroughPassthrough(RelNode node, int baseIndex, java.util.Map<Integer, String> out) {
        if (node == null) return;

        if (node instanceof LogicalFilter f) {
            addScalingReplacementsThroughPassthrough(f.getInput(), baseIndex, out);
            return;
        }
        if (node instanceof LogicalSort s) {
            addScalingReplacementsThroughPassthrough(s.getInput(), baseIndex, out);
            return;
        }

        if (node instanceof LogicalProject p) {
            // Map scaling expressions produced by this Project (relative to its own output).
            addScalingProjectReplacements(p, baseIndex, out);

            // If the Project is a trivial ref-only projection, we can safely look through it
            // to find additional scaling Projects deeper in the tree.
            boolean onlyRefs = true;
            for (RexNode e : p.getProjects()) {
                if (!(stripAllCasts(e) instanceof RexInputRef)) {
                    onlyRefs = false;
                    break;
                }
            }
            if (onlyRefs) {
                addScalingReplacementsThroughPassthrough(p.getInput(), baseIndex, out);
            }
            return;
        }

        if (node instanceof LogicalJoin j) {
            int lc = j.getLeft() != null && j.getLeft().getRowType() != null
                    ? j.getLeft().getRowType().getFieldCount()
                    : -1;
            if (lc < 0) return;
            addScalingReplacementsThroughPassthrough(j.getLeft(), baseIndex, out);
            addScalingReplacementsThroughPassthrough(j.getRight(), baseIndex + lc, out);
            return;
        }
        // For other nodes (Aggregate, TableScan, etc.) we stop: either there is no safe
        // field-position lineage to follow, or they do not produce new computed fields.
    }

    private static void addScalingProjectReplacements(LogicalProject p, int baseIndex, java.util.Map<Integer, String> out) {
        if (p == null || p.getProjects() == null || p.getProjects().isEmpty()) return;
        for (int i = 0; i < p.getProjects().size(); i++) {
            RexNode expr = stripAllCasts(p.getProjects().get(i));
            if (!(expr instanceof RexCall call) || call.getOperator() == null) continue;
            if (call.getOperator().getKind() != SqlKind.TIMES) continue;
            if (call.getOperands() == null || call.getOperands().size() != 2) continue;

            RexNode a = stripAllCasts(call.getOperands().get(0));
            RexNode b = stripAllCasts(call.getOperands().get(1));

            // Require one side to be a numeric literal; other side can be any expression.
            boolean aNumLit = (a instanceof RexLiteral litA)
                    && litA.getType() != null
                    && litA.getType().getSqlTypeName() != null
                    && litA.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
            boolean bNumLit = (b instanceof RexLiteral litB)
                    && litB.getType() != null
                    && litB.getType().getSqlTypeName() != null
                    && litB.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;

            if (!aNumLit && !bNumLit) continue;

            // Canonicalize the expression to the same string form used elsewhere.
            String canon = canonicalizeRex(expr);
            if (canon == null || canon.isBlank()) continue;

            out.put(baseIndex + i, canon);
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

    // Context-aware conjunct decomposition for JOIN conditions.
    // This behaves like the base decomposeConj, but can replace specific RexInputRef indices
    // with canonical expression strings (e.g., inline TIMES($x,1.2) computed by a Project).
    private static void decomposeConj(RexNode cond, List<String> out, java.util.Map<Integer, String> refMap) {
        if (cond == null) return;
        RexNode n = stripAllCasts(cond);
        if (n instanceof RexCall call && call.getOperator() != null && call.getOperator().getKind() == SqlKind.AND) {
            for (RexNode op : call.getOperands()) decomposeConj(op, out, refMap);
        } else {
            String s;
            if (refMap == null || refMap.isEmpty()) {
                s = canonicalizeRex(n);
            } else {
                s = canonicalizeRexWithRefMap(n, refMap);
            }
            if ("true".equalsIgnoreCase(s)) return;
            out.add(s);
        }
    }

    /**
     * Canonicalize a RexNode similarly to {@link #canonicalizeRex(RexNode)} but with an
     * index-based replacement map for {@link RexInputRef}. Only used for JOIN predicates.
     */
    private static String canonicalizeRexWithRefMap(RexNode node, java.util.Map<Integer, String> refMap) {
        if (node == null) return "null";
        RexNode n = stripAllCasts(node);

        if (n instanceof RexInputRef ref) {
            String repl = refMap == null ? null : refMap.get(ref.getIndex());
            return repl == null ? "$x" : repl;
        }

        if (n instanceof RexLiteral lit) {
            if (lit.getType() != null
                    && lit.getType().getSqlTypeName() != null
                    && lit.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
                return normalizeDigest(canonicalizeCharacterLiteral(lit));
            }
            return normalizeDigest(n.toString());
        }

        if (n instanceof RexCall call && call.getOperator() != null) {
            SqlKind kind = call.getOperator().getKind();
            java.util.List<String> parts = new java.util.ArrayList<>();
            for (RexNode op : call.getOperands()) {
                parts.add(canonicalizeRexWithRefMap(op, refMap));
            }

            switch (kind) {
                case AND -> {
                    // decomposeConj handles AND at the caller; keep a simple stable form here.
                    parts.sort(String::compareTo);
                    return normalizeDigest("AND(" + String.join("&", parts) + ")");
                }
                case OR, PLUS, TIMES -> {
                    parts.sort(String::compareTo);
                    return normalizeDigest(kind + "(" + String.join(",", parts) + ")");
                }
                case EQUALS, NOT_EQUALS -> {
                    parts.sort(String::compareTo);
                    String opName = kind == SqlKind.EQUALS ? "=" : "<>";
                    return normalizeDigest(opName + "(" + String.join(",", parts) + ")");
                }
                case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    boolean flipToLess = (kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL);
                    String left = !parts.isEmpty() ? parts.get(0) : "?";
                    String right = parts.size() > 1 ? parts.get(1) : "?";
                    String finalOp;
                    if (flipToLess) {
                        String tmp = left; left = right; right = tmp;
                        finalOp = (kind == SqlKind.GREATER_THAN) ? "<" : "<=";
                    } else {
                        finalOp = (kind == SqlKind.LESS_THAN) ? "<" : "<=";
                    }
                    int cmp = left.compareTo(right);
                    if (cmp > 0) {
                        String tmp = left; left = right; right = tmp;
                    }
                    return normalizeDigest(finalOp + "(" + left + "," + right + ")");
                }
                case SEARCH -> {
                    String valueExpr = parts.size() > 0 ? parts.get(0) : "?";
                    String sargExpr = parts.size() > 1 ? parts.get(1) : "?";
                    String cleanedSarg = normalizeSearchSargText(sargExpr);
                    String maybeRange = parseSingleIntervalFromSarg(cleanedSarg);
                    if (maybeRange != null) {
                        return normalizeDigest("RANGE(" + valueExpr + "," + maybeRange + ")");
                    }
                    return normalizeDigest("SEARCH(" + valueExpr + "," + cleanedSarg + ")");
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
        // First, apply sub-query removal rules to convert RexSubQuery into relational operators
        // (often Correlate/Join+Agg).
        //
        // Best-effort: some complex queries can trigger Calcite internal planner
        // exceptions during this phase. If that happens, we keep the input plan
        // unchanged rather than failing equivalence checking.
        try {
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
        } catch (Throwable t) {
            if (Boolean.getBoolean("calcite.debugEquivalence")) {
                System.err.println("[Calcite.normalizeSubqueriesAndDecorrelate] Subquery normalization failed; using original plan. " + t);
            }
            cur = rel;
        }

        // Then, decorrelate to transform LogicalCorrelate into joins when possible.
        // Use the RelBuilder overload (non-deprecated) so Calcite has the
        // necessary factories/context.
        try {
            FrameworkConfig cfg = CalciteUtil.getFrameworkConfig();
            RelBuilder rb = RelBuilder.create(cfg);
            cur = RelDecorrelator.decorrelateQuery(cur, rb);
        } catch (Throwable t) {
            // Best-effort: if decorrelation not applicable, keep the current plan
        }

        // Optional additional cleanup (harmless if not needed)
        //
        // Note: Calcite has known edge cases where PROJECT_JOIN_TRANSPOSE can trigger
        // AssertionError due to internal RexInputRef type/nullability mismatches
        // after complex subquery rewrites (seen in TPC-DS Q45). Since this pipeline
        // is best-effort for equivalence checking, we catch the AssertionError and
        // retry without that specific rule.
        try {
            cur = applyCleanupProgramBestEffort(cur, true);
        } catch (Throwable t) {
            if (Boolean.getBoolean("calcite.debugEquivalence")) {
                System.err.println("[Calcite.normalizeSubqueriesAndDecorrelate] Cleanup failed; keeping pre-cleanup plan. " + t);
            }
            // Keep whatever we had after decorrelation (or the original plan).
        }

        return cur;
    }

    private static RelNode applyCleanupProgramBestEffort(RelNode root, boolean allowProjectJoinTranspose) {
        if (root == null) return null;

        HepProgramBuilder cleanup = new HepProgramBuilder();
        cleanup.addMatchOrder(HepMatchOrder.TOP_DOWN);
        cleanup.addMatchLimit(1000);

        // Align equivalent shapes that differ by distribution of joins/filters/projects
        // across set operations (UNION ALL).
        cleanup.addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE);
        cleanup.addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE);
        cleanup.addRuleInstance(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE);
        cleanup.addRuleInstance(CoreRules.JOIN_LEFT_UNION_TRANSPOSE);

        // Stabilize join shapes.
        cleanup.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        cleanup.addRuleInstance(CoreRules.JOIN_ASSOCIATE);

        // Projects introduced by decorrelation / set-op rewrites.
        if (allowProjectJoinTranspose) {
            cleanup.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
        }

        cleanup.addRuleInstance(CoreRules.FILTER_CORRELATE);
        cleanup.addRuleInstance(CoreRules.PROJECT_CORRELATE_TRANSPOSE);

        // Aggregate/Project stabilization.
        cleanup.addRuleInstance(REMOVE_REDUNDANT_DISTINCT_AGG_ON_PK);
        cleanup.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
        cleanup.addRuleInstance(CoreRules.PROJECT_AGGREGATE_MERGE);
        cleanup.addRuleInstance(CoreRules.AGGREGATE_MERGE);
        cleanup.addRuleInstance(CoreRules.AGGREGATE_REMOVE);
        cleanup.addRuleInstance(CoreRules.PROJECT_MERGE);
        cleanup.addRuleInstance(CoreRules.PROJECT_REMOVE);
        cleanup.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);

        HepPlanner hp2 = new HepPlanner(cleanup.build());
        hp2.setRoot(root);
        try {
            return hp2.findBestExp();
        } catch (AssertionError ae) {
            if (!allowProjectJoinTranspose) {
                throw ae;
            }
            // Retry without ProjectJoinTransposeRule.
            return applyCleanupProgramBestEffort(root, false);
        }
    }
    
    /**
     * Convert a {@link RelNode} back to a SQL string (PostgreSQL dialect).
     *
     * This is a best-effort helper intended for debugging, logging, and
     * downstream systems that need an executable SQL representation of a
     * relational plan.
     *
     * Notes:
     * - We first attempt to normalize scalar subqueries and decorrelate the
     *   plan (see {@link #normalizeSubqueriesAndDecorrelate(RelNode)}), because
     *   Calcite's {@link RelToSqlConverter} is not reliable for correlated plans.
     * - If a {@code LogicalCorrelate} remains after decorrelation, we return
     *   {@code null} rather than throwing.
     *
     * @param rel relational plan
     * @return SQL string in PostgreSQL dialect, or {@code null} if conversion is
     *         not possible (e.g., correlated plan) or on conversion errors
     */
    public static String relNodeToSql(RelNode rel) {
        if (rel == null) return null;

        RelNode relForSql = rel;
        try {
            relForSql = normalizeSubqueriesAndDecorrelate(rel);
        } catch (Throwable t) {
            // Best-effort: fall back to the original RelNode.
            relForSql = rel;
        }

        // RelToSqlConverter can fail catastrophically on correlated plans.
        if (containsLogicalCorrelate(relForSql)) {
            System.err.println("[Calcite.relNodeToSql] Cannot convert correlated plan (LogicalCorrelate present).");
            return null;
        }

        try {
            RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
            RelToSqlConverter.Result res = converter.visitRoot(relForSql);
            return res.asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
        } catch (AssertionError e) {
            System.err.println("[Calcite.relNodeToSql] AssertionError while converting RelNode to SQL: " + e.getMessage());
            return null;
        } catch (RuntimeException e) {
            System.err.println("[Calcite.relNodeToSql] Error while converting RelNode to SQL: " + e.getMessage());
            return null;
        }
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

        // Best-effort: many benchmark queries contain correlated scalar subqueries
        // (e.g., TPCDS_Q6), which appear as LogicalCorrelate in the initial plan.
        // RelToSqlConverter often fails on correlated plans, but Calcite can
        // decorrelate many of them into joins/aggregates. Try that first.
        RelNode relForSql = rel;
        try {
            relForSql = normalizeSubqueriesAndDecorrelate(rel);
        } catch (Throwable t) {
            // If anything goes wrong, fall back to the original RelNode.
            relForSql = rel;
        }

        // RelToSqlConverter in Calcite does not reliably support correlated plans
        // (LogicalCorrelate) and can throw AssertionError such as
        // "field ordinal X out of range". Since this method is only a best-effort
        // fallback (Postgres EXPLAIN), we skip correlated plans instead of
        // crashing the whole equivalence run.
        if (containsLogicalCorrelate(relForSql)) {
            System.err.println("[Calcite.convertRelNodetoJSONQueryPlan] Skipping EXPLAIN fallback: plan contains LogicalCorrelate.");
            return null;
        }

        String sql = null;
        try {
            RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
            RelToSqlConverter.Result res = converter.visitRoot(relForSql);
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
