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
