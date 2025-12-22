package com.ac.iisc;

import java.sql.SQLException;
import java.util.List;

import org.json.JSONArray;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;

public class LLMEqual
{
    private static final String PROMPT_1 = """
            System Message:
            You are an expert in relational query optimization and Apache Calcite rewrite rules.
            Your job is to compare two equivalent cleaned PostgreSQL EXPLAIN (FORMAT JSON) plan trees
            and propose a short sequence of Apache Calcite transformation rule names (from 
            SUPPORTED_TRANSFORMATIONS) that would transform a Calcite relational plan
            for the ORIGINAL query into one matching the TARGET query.

            (Even though the input plans are PostgreSQL plans, your output rule names must be
            Calcite rule names from the allow-list; do not invent names.)
            
            Hard constraints:
            - Always respond with exactly one JSON object matching the schema requested in
              RESPONSE SCHEMA. Do not output anything else (no prose outside JSON).
            - Only use transformation names that appear in SUPPORTED_TRANSFORMATIONS.
            - If you are not confident, use "equivalent":"dont_know" rather than guessing.
            - Assume temperature is effectively 0: your answers must be deterministic and reproducible.
            - Do NOT propose SQL rewrites or edits. Only propose transformation rule names.
            - Prefer the shortest valid transformation list (often 0-4 rules). Avoid long lists.
            
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
            2) SUPPORTED_TRANSFORMATIONS: a JSON array containing the exact set of allowed
                transformation rule names.
            """;

    private static final String PROMPT_2 = """
            RESPONSE SCHEMA (MUST return exactly ONE JSON object; copy this shape exactly):
            {
                "reasoning": "",
                "equivalent": "true",
                "transformations": [],
                "preconditions": []
            }
            
            RULES:
            1) Use the "reasoning" field to think through the problem BEFORE choosing values for
               "transformations", and "preconditions". This is where you do your
               detailed comparison of ORIGINAL_PLAN_JSON and TARGET_PLAN_JSON.
            2) Only use names from SUPPORTED_TRANSFORMATIONS.
            3) For each transformation listed, provide a corresponding precondition object describing
               the minimal, strictly necessary condition (for example:
               - node X is a Project directly on top of an Aggregate with no column reordering, or
               - a filter predicate P exists on a specific child, or
               - join keys (A = B) exist between two inputs).
               Keep preconditions short and specific.
            4) Do NOT invent any transformation names. If you are unsure, prefer "equivalent":"dont_know".
            5) Output NOTHING but the required JSON object. No markdown, no extra text, no comments.
            6) The output MUST be valid JSON (double quotes, no trailing commas). Do not wrap in code fences.
            
            NOW PROCESS:
            """;
            
    public static String contactLLM (String sqlAJSON, String sqlBJSON)
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
        sb.append("\nSUPPORTED_TRANSFORMATIONS:\n");
        sb.append(new JSONArray(LLM.SUPPORTED_TRANSFORMATIONS).toString());
        sb.append('\n');
        sb.append(PROMPT_2);

        //Get database schemas
        sb.append("\n\nSCHEMA_SUMMARY:\n");
        sb.append(FileIO.readSchemaSummary());

        sb.append("\nORIGINAL_PLAN_JSON:\n").append(sqlAJSON == null ? "(null)" : sqlAJSON);
        sb.append("\n\nTARGET_PLAN_JSON:\n").append(sqlBJSON == null ? "(null)" : sqlBJSON);

        String prompt = sb.toString();

        //System.out.println("Prompt sent to LLM: \n" + prompt);

        // Contact the OpenAI Responses API using env configuration
        OpenAIClient client = OpenAIOkHttpClient.fromEnv();
        String model = LLM.getConfiguredModel();

        ResponseCreateParams params = ResponseCreateParams.builder()
            .input(prompt)
            .model(model)
            .build();

        Response resp = client.responses().create(params);

        //System.out.println("[LLM] Received response from LLM: " + resp);

        // Best-effort extraction of assistant text content from the response.
        // If SDK accessors are unavailable, fall back to parsing the toString() output.
        String raw = resp.toString();
        String contentText = LLM.extractAssistantText(raw);

        if (contentText == null || contentText.isBlank()) {
            System.err.println("[LLM] Unable to extract assistant text from response; returning not equivalent.");
            return "false\nNo transformations found";
        }

        //DEBUG: Print reduced LLM output text
        //System.out.println("LLM Assistant Text: " + contentText);

        return contentText.trim();
    }

    public static String contactLLM (String sqlAJSON, String sqlBJSON, LLMResponse previousResponse)
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
        sb.append("\nSUPPORTED_TRANSFORMATIONS:\n");
        sb.append(new JSONArray(LLM.SUPPORTED_TRANSFORMATIONS).toString());
        sb.append('\n');
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
        String model = LLM.getConfiguredModel();

        ResponseCreateParams params = ResponseCreateParams.builder()
            .input(prompt)
            .model(model)
            .build();

        Response resp = client.responses().create(params);

        //System.out.println("[LLM] Received response from LLM: " + resp);

        // Best-effort extraction of assistant text content from the response.
        // If SDK accessors are unavailable, fall back to parsing the toString() output.
        String raw = resp.toString();
        String contentText = LLM.extractAssistantText(raw);

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
}
