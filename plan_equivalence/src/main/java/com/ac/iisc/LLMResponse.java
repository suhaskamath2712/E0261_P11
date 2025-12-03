package com.ac.iisc;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Parsed representation of the LLM's output contract.
 *
 * Supported input formats:
 * 1) JSON object form (preferred):
 *    {
 *      "equivalent": "true" | "false" | "dont_know",
 *      "transformations": [ "RuleName1", "RuleName2", ... ]
 *    }
 * 2) Legacy line-oriented form (fallback):
 *    First line: literal "true" or "false"
 *    Subsequent lines: transformation names (or a single line "No transformations needed" / "No transformations found")
 *
 * The constructor accepts either representation and validates transformation names
 * against `LLM.getSupportedTransformations()` where applicable.
 */
public class LLMResponse
{
    private boolean queriesAreEquivalent;
    private List<String> transformationSteps;

    public LLMResponse(boolean queriesAreEquivalent, List<String> transformationSteps)
    {
        this.queriesAreEquivalent = queriesAreEquivalent;
        this.transformationSteps = transformationSteps == null ? List.of() : List.copyOf(transformationSteps);
    }

    /**
     * Construct and validate from raw LLM output. Accepts either JSON object or legacy line format.
     * Throws IllegalArgumentException for malformed input or for unsupported transformation names.
     */
    public LLMResponse(String responseText)
    {
        if (responseText == null || responseText.isBlank()) {
            throw new IllegalArgumentException("LLM response is empty or null");
        }

        String trimmed = responseText.trim();

        // Attempt JSON parse first (preferred new contract)
        if (trimmed.startsWith("{")) {
            try {
                JSONObject obj = new JSONObject(trimmed);
                // Equivalent may be boolean or string; accept both
                boolean eq = false;
                if (obj.has("equivalent")) {
                    Object v = obj.get("equivalent");
                    if (v instanceof Boolean b) eq = b;
                    else eq = String.valueOf(v).trim().equalsIgnoreCase("true");
                } else {
                    throw new IllegalArgumentException("JSON response missing 'equivalent' field");
                }
                this.queriesAreEquivalent = eq;

                List<String> steps = new java.util.ArrayList<>();
                if (obj.has("transformations") && !obj.isNull("transformations")) {
                    JSONArray arr = obj.getJSONArray("transformations");
                    for (int i = 0; i < arr.length(); i++) {
                        String s = arr.getString(i).trim();
                        if (!s.isEmpty()) steps.add(s);
                    }
                }

                validateTransformations(steps);
                this.transformationSteps = List.copyOf(steps);
                return;
            } catch (org.json.JSONException jse) {
                // Fall through to legacy parsing below
            }
        }

        // Legacy line-based format (backwards compatibility)
        String normalized = trimmed.replace("\r", "").trim();
        String[] lines = normalized.split("\n+");
        if (lines.length == 0) {
            throw new IllegalArgumentException("LLM response contained no parsable lines");
        }

        String boolLine = lines[0].trim().toLowerCase();
        if (!boolLine.equals("true") && !boolLine.equals("false")) {
            throw new IllegalArgumentException("First line must be 'true' or 'false' but was: " + lines[0]);
        }
        this.queriesAreEquivalent = boolLine.equals("true");

        // Special tokens for no transformations
        List<String> specialTokens = List.of("No transformations needed", "No transformations found");

        // Remaining lines constitute transformations (may be zero)
        List<String> steps = new java.util.ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String raw = lines[i].trim();
            // skip blank lines
            if (raw.isEmpty()) continue;
            steps.add(raw);
        }

        // Handle special singleton cases
        if (steps.size() == 1 && specialTokens.contains(steps.get(0))) {
            // Treat as empty transformation list
            steps.clear();
        }

        validateTransformations(steps);
        this.transformationSteps = List.copyOf(steps);
    }

    private void validateTransformations(List<String> steps) {
        if (steps == null || steps.isEmpty()) return;
        List<String> supported = LLM.getSupportedTransformations();
        for (String step : steps) {
            if (!supported.contains(step))
                throw new IllegalArgumentException("Unsupported transformation: " + step);
        }
    }

    //Setter methods
    /** Set equivalence flag returned/decided by the LLM (or local logic). */
    public void setQueriesAreEquivalent(boolean queriesAreEquivalent){
        this.queriesAreEquivalent = queriesAreEquivalent;
    }
    /** Replace transformation steps; null is treated as an empty, immutable list. */
    public void setTransformationSteps(List<String> transformationSteps) {
        this.transformationSteps = transformationSteps == null ? List.of() : List.copyOf(transformationSteps);
    }

    //Getter methods
    /** True if queries are deemed equivalent by the response/contract. */
    public boolean areQueriesEquivalent(){
        return queriesAreEquivalent;
    }

    /** Ordered list of transformation names (may be empty, never null). */
    public List<String> getTransformationSteps() {
        return transformationSteps;
    }

}
