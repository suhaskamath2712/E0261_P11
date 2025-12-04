package com.ac.iisc;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;
import static com.openai.models.responses.ResponseCreateParams.builder;

/**
 * LLM helper for optional plan-level comparison assistance.
 *
 * Behavior changes / contract:
 * - Constructs a strict prompt that includes two cleaned plan JSON blobs and
 *   an allow-list of supported Calcite transformation names, sourced from transformation_list.txt.
 * - Performs a fast environment check for `OPENAI_API_KEY`; if missing, the
 *   helper returns a safe "not equivalent" contract string instead of
 *   attempting an interactive request.
 * - Contacts the OpenAI Responses API and extracts assistant text from the
 *   returned object; this extraction is defensive and will fall back to a
 *   non-equivalent response if parsing fails.
 * - The expected assistant output contract is a single JSON object with two fields:
 *   `equivalent` (string; `"true"` | `"false"` | `"dont_know"`) and
 *   `transformations` (array of transformation names, may be empty). The implementation
 *   continues to accept the legacy line-oriented output for backward compatibility.
 *
 * Note: LLM integration is optional and the rest of the toolchain works
 * without it. Keep credentials out of source control and ensure your JVM
 * process inherits `OPENAI_API_KEY` (restart terminal/IDE after `setx`).
 */
public class LLM
{
    private static final String BASE_PROMPT = """
            System Message:
            You are an expert in relational query optimization and Apache Calcite transformations.
            Always respond with exactly one JSON object matching the schema requested. Do not
            output anything else. Use only transformation names from SUPPORTED_TRANSFORMATIONS.
            Temperature must be 0.
            
            TASK:
            Given ORIGINAL_PLAN and TARGET_PLAN (both in simplified JSON plan format),
            decide whether they are equivalent. If equivalent, produce an ordered list of Apache
            Calcite transformations (from SUPPORTED_TRANSFORMATIONS) that map
            ORIGINAL_PLAN -> TARGET_PLAN.
            
            INPUTS PROVIDED:
            1) SCHEMA_SUMMARY: a compact JSON describing tables and primary/foreign keys.
            (Use the schema summary only for reasoning about join keys and uniqueness.)
            2) SUPPORTED_TRANSFORMATIONS:
            """;

    private static final String PROMPT_2 = """
            RESPONSE SCHEMA (MUST return exactly this JSON object):
            {
            "equivalent": <"true" | "false" | "dont_know">,
            "transformations": [ <ordered list of exact transformation names from SUPPORTED_TRANSFORMATIONS> ],
            "preconditions": [ <objects describing required preconditions for each transformation, in same order> ]
            }
            
            RULES:
            1) If plans are identical, return "equivalent":"true", "transformations":[], and "preconditions": [].
            2) Only use names from SUPPORTED_TRANSFORMATIONS. If none apply, return "equivalent":"dont_know" with empty lists.
            3) For each transformation listed, provide a corresponding precondition object describing the minimal, strictly necessary condition (e.g., "node X is Project on top of Aggregate", or "filter predicate P exists on child", "join keys (A = B) exist"). Keep preconditions concise.
            4) Do NOT invent any transformation names. If unsure, return "dont_know".
            5) Output NOTHING but the required JSON object.
            
            ONE-SHOT EXAMPLE:
            SCHEMA_SUMMARY:
            { "orders": {"pk":["o_orderkey"], "cols":["o_custkey","o_orderdate"]}, "customer":{"pk":["c_custkey"], "cols":["c_mktsegment"]} }
            
            SUPPORTED_TRANSFORMATIONS: ["AggregateProjectMergeRule","ProjectRemoveRule","JoinAssociateRule", ...]  // (full list)
            
            ORIGINAL_PLAN_JSON:
            { "Node Type":"Join", "Join Type":"Inner",
            "Plans":[
                {"Node Type":"Project","Plans":[{"Node Type":"Aggregate","Plans":[{"Node Type":"TableScan","Relation Name":"orders"}]}]},
                {"Node Type":"TableScan","Relation Name":"customer"}
            ],
            "Join Cond":"(orders.o_custkey = customer.c_custkey)"
            }
            
            TARGET_PLAN_JSON:
            { "Node Type":"Join", "Join Type":"Inner",
            "Plans":[
                {"Node Type":"Aggregate","Plans":[{"Node Type":"TableScan","Relation Name":"orders"}]},
                {"Node Type":"TableScan","Relation Name":"customer"}
            ],
            "Join Cond":"(orders.o_custkey = customer.c_custkey)"
            }
            
            EXPECTED_RESPONSE:
            {
            "equivalent":"true",
            "transformations":["AggregateProjectMergeRule"],
            "preconditions":[ {"requires":"Project on top of Aggregate with no column reordering"} ]
            }
            
            NOW PROCESS:
            """;
    
    /**
     * Transformation rule names the LLM is allowed to emit.
     * These correspond to Calcite CoreRules and are validated in LLMResponse.
     * The list is now always sourced from transformation_list.txt and may change as that file changes.
     * Keep the canonical names; unknown or misspelled names will be rejected.
     */
    private static final List<String> SUPPORTED_TRANSFORMATIONS = List.of(
        "AggregateExpandDistinctAggregatesRule",
        "AggregateExtractProjectRule",
        "AggregateFilterToCaseRule",
        "AggregateFilterTransposeRule",
        "AggregateJoinJoinRemoveRule",
        "AggregateJoinRemoveRule",
        "AggregateJoinTransposeRule",
        "AggregateMergeRule",
        "AggregateProjectMergeRule",
        "AggregateProjectPullUpConstantsRule",
        "AggregateProjectStarTableRule",
        "AggregateReduceFunctionsRule",
        "AggregateRemoveRule",
        "AggregateStarTableRule",
        "AggregateUnionAggregateRule",
        "AggregateUnionTransposeRule",
        "AggregateValuesRule",
        "CalcMergeRule",
        "CalcRemoveRule",
        "CalcSplitRule",
        "CoerceInputsRule",
        "ExchangeRemoveConstantKeysRule",
        "FilterAggregateTransposeRule",
        "FilterCalcMergeRule",
        "FilterCorrelateRule",
        "FilterJoinRule.FilterIntoJoinRule",
        "FilterJoinRule.JoinConditionPushRule",
        "FilterMergeRule",
        "FilterMultiJoinMergeRule",
        "FilterProjectTransposeRule",
        "FilterSampleTransposeRule",
        "FilterSetOpTransposeRule",
        "FilterTableFunctionTransposeRule",
        "FilterToCalcRule",
        "FilterWindowTransposeRule",
        "IntersectToDistinctRule",
        "JoinAddRedundantSemiJoinRule",
        "JoinAssociateRule",
        "JoinDeriveIsNotNullFilterRule",
        "JoinExtractFilterRule",
        "JoinProjectBothTransposeRule",
        "JoinProjectLeftTransposeRule",
        "JoinProjectRightTransposeRule",
        "JoinPushExpressionsRule",
        "JoinPushTransitivePredicatesRule",
        "JoinToCorrelateRule",
        "JoinToMultiJoinRule",
        "JoinLeftUnionTransposeRule",
        "JoinRightUnionTransposeRule",
        "MatchRule",
        "MinusToAntiJoinRule",
        "MinusToDistinctRule",
        "MinusMergeRule",
        "MultiJoinOptimizeBushyRule",
        "ProjectAggregateMergeRule",
        "ProjectCalcMergeRule",
        "ProjectCorrelateTransposeRule",
        "ProjectFilterTransposeRule",
        "ProjectJoinJoinRemoveRule",
        "ProjectJoinRemoveRule",
        "ProjectJoinTransposeRule",
        "ProjectMergeRule",
        "ProjectMultiJoinMergeRule",
        "ProjectRemoveRule",
        "ProjectSetOpTransposeRule",
        "ProjectToCalcRule",
        "ProjectToWindowRule",
        "ProjectToWindowRule.CalcToWindowRule",
        "ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule",
        "ProjectWindowTransposeRule",
        "ReduceDecimalsRule",
        "ReduceExpressionsRule.CalcReduceExpressionsRule",
        "ReduceExpressionsRule.FilterReduceExpressionsRule",
        "ReduceExpressionsRule.JoinReduceExpressionsRule",
        "ReduceExpressionsRule.ProjectReduceExpressionsRule",
        "ReduceExpressionsRule.WindowReduceExpressionsRule",
        "SampleToFilterRule",
        "SemiJoinFilterTransposeRule",
        "SemiJoinJoinTransposeRule",
        "SemiJoinProjectTransposeRule",
        "SemiJoinRemoveRule",
        "SemiJoinRule",
        "SemiJoinRule.JoinOnUniqueToSemiJoinRule",
        "SemiJoinRule.JoinToSemiJoinRule",
        "SemiJoinRule.ProjectToSemiJoinRule",
        "SortJoinCopyRule",
        "SortJoinTransposeRule",
        "SortProjectTransposeRule",
        "SortRemoveConstantKeysRule",
        "SortRemoveRedundantRule",
        "SortRemoveRule",
        "SortUnionTransposeRule",
        "TableScanRule",
        "UnionMergeRule",
        "UnionPullUpConstantsRule",
        "UnionToDistinctRule"
    );

    private static final String CANONICALISE_PROMPT = """
            You are a SQL LOGICAL PLAN CANONICALIZER.
 
Your job:
Given ONE query plan in a textual tree format, output an equivalent plan in a CANONICAL NORMAL FORM so that semantically equivalent plans end up with the same text, even if they came from different optimizers or physical planners.
 
VERY IMPORTANT:
- Preserve the logical meaning exactly.
- Do NOT add or remove predicates, joins, group-by keys, or expressions.
- Only normalize representation according to the rules below.
- Output ONLY the canonicalized plan, no explanations, no comments, no prose.
 
Input format:
- You will receive a single plan as plain text.
- Node examples: 
  Sort[...]
    Join[...]
      Filter[condition](Scan(table))
      SubqueryScan alias( ... )
- The plan may contain physical artifacts like HashJoin, NestedLoop, SeqScan, IndexScan, Hash[…], etc.
 
Your output must be:
- A single logical plan in the SAME general textual style as the input.
- Indented tree structure preserved.
- No extra commentary.
 
================= CANONICALIZATION RULES =================
 
1) Normalize join operator names
--------------------------------
- Replace ALL inner/outer join operator names that depend on physical strategy with a generic logical name:
  - "Join", "HashJoin", "MergeJoin", "NestedLoop" -> "LogicalJoin"
- Replace SEMI joins:
  - "NestedLoop SEMI", "SemiJoin" -> "LogicalSemiJoin"
- Keep the join condition inside the brackets as-is logically (after other normalization rules like field/predicate ordering).
 
Example:
  HashJoin[o_custkey = c_custkey]
becomes:
  LogicalJoin[o_custkey = c_custkey]
 
  NestedLoop SEMI[orders.o_orderkey = lineitem_1.l_orderkey]
becomes:
  LogicalSemiJoin[orders.o_orderkey = lineitem_1.l_orderkey]
 
 
2) Normalize join order (commutativity)
----------------------------------------
Many joins are commutative (e.g., inner joins). Canonicalize the order of their children.
 
For any Binary join (LogicalJoin, LogicalSemiJoin where applicable):
- Compute a deterministic key for each child subtree as the textual representation of that subtree (you can approximate by the first table name or alias appearing under that subtree).
- Reorder the two children so that the child with the lexicographically smaller key comes FIRST.
- This is purely a presentational reordering; do NOT change the join condition itself.
 
Example (conceptual):
 
  LogicalJoin[cond]
    Scan(orders)
    Scan(customer)
 
and
 
  LogicalJoin[cond]
    Scan(customer)
    Scan(orders)
 
must both canonicalize to the same child order, e.g.:
 
  LogicalJoin[cond]
    Scan(customer)
    Scan(orders)
 
 
3) Strip purely physical wrappers
----------------------------------
Remove physical artifacts or wrappers that do not change logical semantics:
 
- Remove "Hash[ ... ]" wrapper nodes; inline their child.
- Normalize scans:
  - "SeqScan table", "IndexScan table", "IndexOnlyScan table", "Scan(table)" -> "Scan(table)"
- Remove physical distribution/parallelism operators if present:
  - e.g., "Exchange[...] (...)", "SortExchange(...)" -> just their children in canonical form.
- Remove redundant project nodes that only re-order columns or pass-through fields unchanged.
  (If a Project changes expressions, keep it.)
 
Example:
 
  Hash[
    HashJoin[o_custkey = c_custkey]
      Filter[...] (SeqScan orders)
      Hash[
        Filter[...] (SeqScan customer)
      ]
  ]
 
might canonicalize to:
 
  LogicalJoin[o_custkey = c_custkey]
    Filter[...] (Scan(orders))
    Filter[...] (Scan(customer))
 
 
4) Normalize filter predicates
-------------------------------
For Filter[...] or join conditions in [...]:
 
- Treat logical AND as commutative.
- For expressions of the form "A AND B AND C", sort the individual conjuncts in a deterministic way (lexicographically by their text form).
- Remove redundant parentheses that do not affect logic if they are purely syntactic.
- Keep comparison operators and arithmetic exactly the same otherwise.
 
Example:
 
  Filter[(a > 10 AND b = 5) AND c < 3]
 
and
 
  Filter[c < 3 AND b = 5 AND a > 10]
 
must canonicalize to the same text, e.g.:
 
  Filter[a > 10 AND b = 5 AND c < 3]
 
 
5) Normalize aggregates
------------------------
For Aggregate nodes:
 
- Keep the Aggregate node, but canonicalize its arguments:
  - GROUP BY list:
    - Sort grouping keys lexicographically by their textual representation.
  - Aggregate calls:
    - Sort aggregate functions lexicographically by:
      1. function name (e.g., COUNT, SUM, MAX),
      2. then by the textual representation of their argument(s).
- Do NOT drop or invent aggregate functions.
 
Example:
 
  Aggregate[
    GROUP BY (b, a),
    sum_x = SUM(x),
    count_y = COUNT(y)
  ]
 
and
 
  Aggregate[
    GROUP BY (a, b),
    count_y = COUNT(y),
    sum_x = SUM(x)
  ]
 
must canonicalize to something like:
 
  Aggregate[
    GROUP BY (a, b),
    count_y = COUNT(y),
    sum_x = SUM(x)
  ]
 
 
6) Normalize field and table name formatting (lightweight)
-----------------------------------------------------------
- Keep the actual field and table names as given (do NOT try to invent $0, $1 columns).
- However, normalize formatting:
  - Use "table.column" consistently.
  - Remove redundant schema qualifiers if not needed and inconsistent (e.g., "dbo.customer.c_custkey" vs "customer.c_custkey": pick a consistent minimal form if they appear mixed).
- Do NOT rename columns across the plan; the same input name must stay the same everywhere in the canonical form.
 
 
7) Normalize subquery / alias wrappers
--------------------------------------
- Keep SubqueryScan nodes, but normalize alias formatting:
  - "SubqueryScan lr(" and "SubqueryScan lr (" → "SubqueryScan lr(" (no extra spaces).
- If two different input plans use different but insignificant alias names, do NOT attempt to unify them manually; just normalize spacing and parentheses.
 
 
8) Sort children where order does not matter
--------------------------------------------
Besides join commutativity:
 
- For UNION/UNION ALL/INTERSECT/EXCEPT where order is not semantically fixed by the operator, sort the children lexicographically by their subtree key, similar to join children.
- For lists inside nodes (e.g., projection lists, group lists, order-by lists), keep the order given UNLESS you know the order is logically irrelevant (e.g., GROUP BY), in which case apply the sorting rules above.
 
 
9) Preserve sorts that matter, normalize their formatting
---------------------------------------------------------
- Keep Sort nodes because ORDER BY is semantically meaningful.
- Normalize formatting of sort keys:
  - E.g., "Sort[revenue DESC, o_orderdate]" and "Sort[ revenue DESC , o_orderdate ASC ]" → "Sort[revenue DESC, o_orderdate ASC]"
- If direction is omitted and default is ASC, you may write it explicitly as ASC.
 
 
10) Output format and constraints
---------------------------------
- Output ONLY the canonicalized logical plan.
- Keep the same tree-like indentation style as input.
- No additional explanation, no prose text, no extra labels.
- Do not wrap the result in JSON or markdown; just the plan text.
 
11) Normalize table/alias qualifiers in column names
----------------------------------------------------
Column references may appear as:
- "table.column"
- "alias.column"
- just "column"
 
These are often logically the same attribute in different renderings of the plan.
 
For canonicalization:
 
- Strip ALL table or alias qualifiers from column references wherever they appear.
- That is, transform any "<something>.<identifier>" into just "<identifier>".
 
Examples:
- "lineitem.l_orderkey"  -> "l_orderkey"
- "orders.o_orderdate"   -> "o_orderdate"
- "lr.revenue"           -> "revenue"
- "customer.c_custkey"   -> "c_custkey"
 
Apply this consistently:
- In GROUP BY lists
- In ORDER BY / Sort keys
- In join conditions
- In Filter predicates
- In Aggregate argument lists
- Anywhere else a column is referenced.
 
The goal is that:
 
  GROUP BY (lineitem.l_orderkey)
  and
  GROUP BY (l_orderkey)
 
canonicalize to:
 
  GROUP BY (l_orderkey)
 
and
 
  Sort[lr.revenue DESC, orders.o_orderdate ASC]
  and
  Sort[revenue DESC, o_orderdate ASC]
 
canonicalize to:
 
  Sort[revenue DESC, o_orderdate ASC]
 
 
==========================================================
 
When you receive a plan, apply ALL rules above and respond only with the canonicalized plan.
            """;
    

    /**
     * Public accessor so parser/validator code can check transformation names.
     * The returned list is always in sync with transformation_list.txt.
     */
    public static List<String> getSupportedTransformations() {
        // immutable List.of
        return SUPPORTED_TRANSFORMATIONS;
    }
    /**
     * Contact the LLM with two cleaned plan JSON strings and receive the assistant's
     * text output. The method performs an environment check and a defensive
     * extraction of assistant text from the response object.
     *
     * @param sqlAJSON cleaned JSON plan for query A (may be null)
     * @param sqlBJSON cleaned JSON plan for query B (may be null)
     * @return assistant text trimmed, or a safe "false\nNo transformations found" contract string on failure
     */
    public static String contactLLM(String sqlAJSON, String sqlBJSON)
    {
        // Fail fast if API key is not present in the environment
        String apiKey = System.getenv("OPENAI_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            System.err.println("[LLM] OPENAI_API_KEY is not set in the environment; skipping LLM call.");
            // Return a minimal, valid contract string to avoid crashing callers
            return "false\nNo transformations found";
        }

        // Build strict prompt including both plans and the allowed rule list
        StringBuilder sb = new StringBuilder();
        sb.append(BASE_PROMPT);
        for (String t : SUPPORTED_TRANSFORMATIONS) sb.append(t).append('\n');
        sb.append(PROMPT_2);

        //Get database schemas
        sb.append("\n\nSCHEMA_SUMMARY:\n");
        sb.append(FileIO.readSchemaSummary());

        sb.append("\nORIGINAL_PLAN_JSON:\n").append(sqlAJSON == null ? "(null)" : sqlAJSON);
        sb.append("\n\nTARGET_PLAN_JSON:\n").append(sqlBJSON == null ? "(null)" : sqlBJSON);

        String prompt = sb.toString();

        // Contact the OpenAI Responses API using env configuration
        OpenAIClient client = OpenAIOkHttpClient.fromEnv();
        ResponseCreateParams params = ResponseCreateParams.builder()
            .input(prompt)
            .model("gpt-5")
            .build();

        Response resp = client.responses().create(params);

        // Best-effort extraction of assistant text content from the response.
        // If SDK accessors are unavailable, fall back to parsing the toString() output.
        String raw = resp.toString();
        String contentText = extractAssistantText(raw);

        if (contentText == null || contentText.isBlank()) {
            System.err.println("[LLM] Unable to extract assistant text from response; returning not equivalent.");
            return "false\nNo transformations found";
        }

        //DEBUG: Print reduced LLM output text
        //System.out.println("LLM Assistant Text: " + contentText);

        return contentText.trim();
    }

    public static String normaliseRelNodeLLM(RelNode relNode)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(CANONICALISE_PROMPT);

        String relText = RelOptUtil.toString(relNode, SqlExplainLevel.DIGEST_ATTRIBUTES);
        sb.append("\n\nINPUT PLAN:\n").append(relText);
        String prompt = sb.toString();

        // Contact the OpenAI Responses API using env configuration
        OpenAIClient client = OpenAIOkHttpClient.fromEnv();
        var params = builder()
            .input(prompt)
            .model("gpt-5")
            .build();

        Response resp = client.responses().create(params);

        // Best-effort extraction of assistant text content from the response.
        // If SDK accessors are unavailable, fall back to parsing the toString() output.
        String raw = resp.toString();
        String contentText = extractAssistantText(raw);

        if (contentText == null || contentText.isBlank()) {
            System.err.println("[LLM] Unable to extract assistant text from response.");
            return relText;
        }

        //DEBUG: Print reduced LLM output text
        //System.out.println("LLM Assistant Text: " + contentText);

        return contentText.trim();
    }

    public static LLMResponse getLLMResponse(String sqlA, String sqlB)
    {
        // Build cleaned plan JSON for both inputs and contact the LLM.
        // On plan retrieval failure, returns null so callers can skip LLM usage.
        String sqlAJSON;
        String sqlBJSON;
        try
        {
            sqlAJSON = GetQueryPlans.getCleanedQueryPlanJSONasString(sqlA);
            sqlBJSON = GetQueryPlans.getCleanedQueryPlanJSONasString(sqlB);
        }
        catch (SQLException ex)
        {
            System.err.println("Error obtaining query plans: " + ex.getMessage());
            return null;
        }

        String raw = contactLLM(sqlAJSON, sqlBJSON);
        try {
            return new LLMResponse(raw);
        } catch (IllegalArgumentException iae) {
            System.err.println("[LLM] Unexpected LLM output format; treating as not equivalent. " + iae.getMessage());
            return new LLMResponse(false, List.of());
        }
    }

    // --- Helpers ---
    /**
     * Extract the assistant's text from the Response.toString() output.
     * This is a fallback for environments where typed SDK getters are not available.
     */
    private static String extractAssistantText(String respString) {
        if (respString == null) return null;
        // Try to find the first occurrence of outputText=ResponseOutputText{... text=... , type=
        int ot = respString.indexOf("outputText=ResponseOutputText{");
        if (ot >= 0) {
            int textIdx = respString.indexOf("text=", ot);
            if (textIdx >= 0) {
                int typeIdx = respString.indexOf(", type=", textIdx);
                if (typeIdx > textIdx) {
                    String slice = respString.substring(textIdx + 5, typeIdx).trim();
                    // Remove any surrounding quotes if present (toString may omit)
                    return slice;
                }
            }
        }
        // Fallback: try generic " text=" capture
        int tIdx = respString.indexOf(" text=");
        if (tIdx >= 0) {
            int comma = respString.indexOf(",", tIdx + 6);
            if (comma > tIdx) return respString.substring(tIdx + 6, comma).trim();
        }
        return null;
    }
}
