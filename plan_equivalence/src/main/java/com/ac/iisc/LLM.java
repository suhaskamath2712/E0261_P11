package com.ac.iisc;

import java.sql.SQLException;
import java.util.List;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;

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
            Given ORIGINAL_PLAN and TARGET_PLAN (both in simplified JSON plan format, see "Plan Format" below),
            determine whether the two plans are equivalent by producing a sequence of Apache Calcite
            transformations (from SUPPORTED_TRANSFORMATIONS) that maps ORIGINAL_PLAN -> TARGET_PLAN.

            RESPONSE RULES:
            1) Output exactly one JSON object and nothing else.
            2) Schema:
                {
                "equivalent": <"true" or "false" or "dont_know">,
                "transformations": [ <ordered list of transformation names from SUPPORTED_TRANSFORMATIONS> ]  // may be empty
                }
            3) If plans are identical (no transform needed): set "equivalent":"true" and "transformations": []
            4) If you can propose a valid ordered list of transformations from SUPPORTED_TRANSFORMATIONS that
            plausibly maps ORIGINAL_PLAN to TARGET_PLAN: set "equivalent":"true" and list them in order.
            5) If you cannot find a sequence: set "equivalent":"dont_know" and set "transformations": []
            6) Do NOT invent transformer names outside SUPPORTED_TRANSFORMATIONS. If none apply, output "dont_know".
            7) Do not output any extra text, commentary, or diagnostics.

            PLAN FORMAT (provide only fields that matter; remove costs, timings):
            - Node: { "Node Type": "...", "Join Type": "...", "Filter": "...", "Hash Cond": "...",
            "Group Key": [...], "Sort Key": [...], "Index Cond": "...", "Relation Name": "...",
            "Alias": "...", "Strategy": "Hashed|Sorted|..." }
            - A plan is nested using "Plans": [ ... ].
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

    private static final String CONTEXT_PROMPT = """
            SCHEMA CONTEXT: Use the TPCH schema for table/column names and primary keys (see attached).
            Use it only for reasoning about join keys and uniqueness constraints.
            """;
    
    private static final String EXAMPLE = """
    ONE-SHOT EXAMPLE:
    ORIGINAL_PLAN:
    { "Node Type":"Join", "Join Type":"Inner", "Plans":[ { "Node Type":"Hash", "Plans":[ {"Node Type":"Seq Scan", "Relation Name":"orders"} ] }, { "Node Type":"Hash", "Plans":[ {"Node Type":"Seq Scan", "Relation Name":"customer"} ] } ], "Hash Cond":"(orders.o_custkey = customer.c_custkey)" }
    
    TARGET_PLAN:
    { "Node Type":"Join", "Join Type":"Inner", "Plans":[ { "Node Type":"Seq Scan", "Relation Name":"orders" }, { "Node Type":"Seq Scan", "Relation Name":"customer" } ], "Hash Cond":"(orders.o_custkey = customer.c_custkey)" }
    
    EXPECTED_RESPONSE:
    {
    "equivalent":"true",
    "transformations":["ProjectRemoveRule","ReduceExpressionsRule.HashReduceExpressionsRule","JoinToMultiJoinRule"]
    }

    NOW PROCESS:
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
        sb.append("\nSupported transformations (one per line if needed):\n");
        for (String t : SUPPORTED_TRANSFORMATIONS) sb.append(t).append('\n');

        //Get database schemas
        sb.append(CONTEXT_PROMPT + "\n");
        sb.append(GetQueryPlans.getDatabaseSchema()).append("\n");

        sb.append(EXAMPLE);

        sb.append("\nORIGINAL_PLAN_JSON:\n").append(sqlAJSON == null ? "(null)" : sqlAJSON);
        sb.append("\n\nTARGET_PLAN_JSON:\n").append(sqlBJSON == null ? "(null)" : sqlBJSON);

        String prompt = sb.toString();

        // Contact the OpenAI Responses API using env configuration
        OpenAIClient client = OpenAIOkHttpClient.fromEnv();
        ResponseCreateParams params = ResponseCreateParams.builder()
            .input(prompt)
            .model("gpt-5")
            //.temperature(0.0)
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

    public static LLMResponse getLLMResponse(String sqlA, String sqlB)
    {
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
