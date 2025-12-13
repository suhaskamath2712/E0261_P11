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
 * - The expected assistant output contract is a single JSON object with the
 *   following fields:
 *     - `reasoning`: free-form string containing step-by-step analysis.
 *     - `equivalent`: string; `"true"` | `"false"` | `"dont_know"`.
 *     - `transformations`: array of transformation names (may be empty).
 *     - `preconditions`: array of objects describing minimal preconditions for
 *       each listed transformation (same order as `transformations`).
 *   The implementation currently consumes only `equivalent` and
 *   `transformations`; additional fields are included for interpretability.
 *   The helper continues to accept the legacy line-oriented output format for
 *   backward compatibility.
 *
 * Note: LLM integration is optional and the rest of the toolchain works
 * without it. Keep credentials out of source control and ensure your JVM
 * process inherits `OPENAI_API_KEY` (restart terminal/IDE after `setx`).
 */
public class LLM
{
    private static final String PROMPT_1 = """
            System Message:
            You are an expert in relational query optimization and Apache Calcite transformations.
            Your job is to compare two PostgreSQL logical plans and decide whether they are
            semantically equivalent, and if so, which Calcite transformations from
            SUPPORTED_TRANSFORMATIONS can map ORIGINAL_PLAN_JSON to TARGET_PLAN_JSON.
            
            Hard constraints:
            - Always respond with exactly one JSON object matching the schema requested in
              RESPONSE SCHEMA. Do not output anything else (no prose outside JSON).
            - Only use transformation names that appear in SUPPORTED_TRANSFORMATIONS.
            - If you are not confident, use "equivalent":"dont_know" rather than guessing.
            - Assume temperature is effectively 0: your answers must be deterministic and reproducible.
            
            TASK:
            Given ORIGINAL_PLAN_JSON and TARGET_PLAN_JSON (both in simplified JSON plan format),
            and SCHEMA_SUMMARY (primary/foreign keys and basic table info),
            decide whether they are equivalent. If equivalent, produce an ordered list of Apache
            Calcite transformations (from SUPPORTED_TRANSFORMATIONS) that map
            ORIGINAL_PLAN_JSON -> TARGET_PLAN_JSON.
            
            High-level reasoning steps (to be reflected in the 'reasoning' field of the JSON output):
            1) Compare overall structure: root operators, join tree shape, grouping/aggregation, projections.
            2) Compare key semantic properties: join keys, grouping keys, filter predicates, set-ops, limits.
            3) Identify local structural differences that could be explained by a small number of known
               transformations (e.g., project/aggregate/join/filter reorderings).
            4) If a plausible sequence exists, list those transformations and their minimal preconditions.
               Otherwise, return "equivalent":"dont_know".
            
            INPUTS PROVIDED (appended below this prompt):
            1) SCHEMA_SUMMARY: a compact JSON describing tables and primary/foreign keys.
               Use this only for reasoning about join keys, uniqueness, and nullability assumptions.
            2) SUPPORTED_TRANSFORMATIONS: the exact set of allowed transformation names.
            """;

    private static final String PROMPT_2 = """
            RESPONSE SCHEMA (MUST return exactly this JSON object):
            {
              "reasoning": "Step-by-step analysis of whether ORIGINAL_PLAN_JSON can be transformed into TARGET_PLAN_JSON using SUPPORTED_TRANSFORMATIONS. Think carefully here before deciding on 'equivalent' and 'transformations'.",
              "equivalent": <"true" | "false" | "dont_know">,
              "transformations": [ <ordered list of exact transformation names from SUPPORTED_TRANSFORMATIONS> ],
              "preconditions": [ <objects describing required preconditions for each transformation, in the same order as 'transformations'> ]
            }
            
            RULES:
            1) Use the "reasoning" field to think through the problem BEFORE choosing values for
               "equivalent", "transformations", and "preconditions". This is where you do your
               detailed comparison of ORIGINAL_PLAN_JSON and TARGET_PLAN_JSON.
            2) If plans are structurally and semantically identical, return
               "equivalent":"true", "transformations":[], and "preconditions":[].
            3) Only use names from SUPPORTED_TRANSFORMATIONS. If none apply or you are not confident,
               return "equivalent":"dont_know" with empty lists.
            4) For each transformation listed, provide a corresponding precondition object describing
               the minimal, strictly necessary condition (for example:
               - node X is a Project directly on top of an Aggregate with no column reordering, or
               - a filter predicate P exists on a specific child, or
               - join keys (A = B) exist between two inputs).
               Keep preconditions short and specific.
            5) Do NOT invent any transformation names. If you are unsure, prefer "equivalent":"dont_know".
            6) Output NOTHING but the required JSON object. No markdown, no extra text, no comments.
            
            ONE-SHOT EXAMPLE:
            SCHEMA_SUMMARY:
            { "orders": {"pk":["o_orderkey"], "cols":["o_custkey","o_orderdate"]},
              "customer":{"pk":["c_custkey"], "cols":["c_mktsegment"]} }
            
            SUPPORTED_TRANSFORMATIONS: ["AggregateProjectMergeRule","ProjectRemoveRule","JoinAssociateRule", ...]  // (full list)
            
            ORIGINAL_PLAN_JSON:
            { "Node Type":"Join", "Join Type":"Inner",
              "Plans":[
                {"Node Type":"Project",
                 "Plans":[{"Node Type":"Aggregate",
                           "Plans":[{"Node Type":"TableScan","Relation Name":"orders"}]}]},
                {"Node Type":"TableScan","Relation Name":"customer"}
              ],
              "Join Cond":"(orders.o_custkey = customer.c_custkey)"
            }
            
            TARGET_PLAN_JSON:
            { "Node Type":"Join", "Join Type":"Inner",
              "Plans":[
                {"Node Type":"Aggregate",
                 "Plans":[{"Node Type":"TableScan","Relation Name":"orders"}]},
                {"Node Type":"TableScan","Relation Name":"customer"}
              ],
              "Join Cond":"(orders.o_custkey = customer.c_custkey)"
            }
            
            EXPECTED_RESPONSE:
            {
              "reasoning": "The only difference is that in ORIGINAL_PLAN_JSON the left input has a Project on top of an Aggregate, whereas in TARGET_PLAN_JSON it is just the Aggregate. The Project appears to be redundant and can be merged into the Aggregate without changing semantics. AggregateProjectMergeRule is designed for exactly this pattern.",
              "equivalent": "true",
              "transformations": ["AggregateProjectMergeRule"],
              "preconditions": [ { "requires": "Project directly on top of Aggregate with no column reordering or expression changes" } ]
            }
            
            NOW PROCESS:
            """;
    
    /**
     * Transformation rule names the LLM is allowed to emit.
     * These correspond to Calcite CoreRules and are validated in LLMResponse.
     * The list is now always sourced from transformation_list.txt and may change as that file changes.
     * Keep the canonical names; unknown or misspelled names will be rejected.
     */
    private static final List<String> SUPPORTED_TRANSFORMATIONS;

    static {
        java.util.List<String> loaded = FileIO.readLinesResource("transformation_list.txt");
        if (loaded == null || loaded.isEmpty()) {
            SUPPORTED_TRANSFORMATIONS = List.of();
        } else {
            SUPPORTED_TRANSFORMATIONS = List.copyOf(loaded);
        }
    }
    

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
        sb.append(PROMPT_1);
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

        //System.out.println("[LLM] Received response from LLM: " + resp);

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

    public static String contactLLM(String sqlAJSON, String sqlBJSON, LLMResponse previousResponse)
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
        sb.append(PROMPT_1);
        for (String t : SUPPORTED_TRANSFORMATIONS) sb.append(t).append('\n');
        sb.append(PROMPT_2);

        //Get database schemas
        sb.append("\n\nSCHEMA_SUMMARY:\n");
        sb.append(FileIO.readSchemaSummary());

        sb.append("\nORIGINAL_PLAN_JSON:\n").append(sqlAJSON == null ? "(null)" : sqlAJSON);
        sb.append("\n\nTARGET_PLAN_JSON:\n").append(sqlBJSON == null ? "(null)" : sqlBJSON);

        sb.append("""
                
                The JSON object shown below was your previous response. It was judged incorrect
                by an external equivalence checker. Carefully reconsider SCHEMA_SUMMARY,
                ORIGINAL_PLAN_JSON, TARGET_PLAN_JSON, and SUPPORTED_TRANSFORMATIONS, then
                return a NEW JSON object that follows the RESPONSE SCHEMA. Do not simply repeat
                the same 'transformations' list unless you have a strong reason to believe it is correct.
                Previous response:
                """);
        sb.append('\n').append(previousResponse.toString()).append('\n');

        String prompt = sb.toString();

        // Contact the OpenAI Responses API using env configuration
        OpenAIClient client = OpenAIOkHttpClient.fromEnv();
        ResponseCreateParams params = ResponseCreateParams.builder()
            .input(prompt)
            .model("gpt-5")
            .build();

        Response resp = client.responses().create(params);

        //System.out.println("[LLM] Received response from LLM: " + resp);

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

    public static LLMResponse getLLMResponse(String sqlA, String sqlB, LLMResponse previousResponse)
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

        String raw = contactLLM(sqlAJSON, sqlBJSON, previousResponse);
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
