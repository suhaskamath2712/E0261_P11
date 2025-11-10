package com.ac.iisc;

import java.util.List;

/**
 * Parsed representation of the LLM's strict output contract.
 *
 * Contract:
 * - Line 1: literal "true" or "false" (case-insensitive) indicating equivalence.
 * - Lines 2..N: optional newline-separated transformation rule names from LLM.getSupportedTransformations().
 *   Special cases: a single line equal to "No transformations needed" or
 *   "No transformations found" is treated as an empty list.
 */
public class LLMResponse
{
    private boolean queriesAreEquivalent;
    private List<String> transformationSteps;

    public LLMResponse(boolean queriesAreEquivalent, List<String> transformationSteps)
    {
        this.queriesAreEquivalent = queriesAreEquivalent;
        this.transformationSteps = transformationSteps;
    }

    /**
     * Construct and validate from raw LLM text output. Throws IllegalArgumentException for malformed input
     * or for transformation names not in the supported allow-list.
     */
    public LLMResponse(String responseText)
    {
        if (responseText == null || responseText.isBlank()) {
            throw new IllegalArgumentException("LLM response is empty or null");
        }

        // Normalize newlines and split
        String normalized = responseText.replace("\r", "").trim();
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
            if (raw.isEmpty()) continue; // skip blank lines
            steps.add(raw);
        }

        // Handle special singleton cases
        if (steps.size() == 1 && specialTokens.contains(steps.get(0))) {
            // Treat as empty transformation list
            steps.clear();
        }

        // Validate transformations against supported list (only if there are steps)
        if (!steps.isEmpty()) {
            List<String> supported = LLM.getSupportedTransformations();
            for (String step : steps)
                if (!supported.contains(step))
                    throw new IllegalArgumentException("Unsupported transformation: " + step);
        }

        this.transformationSteps = List.copyOf(steps); // immutable snapshot
    }

    //Setter methods
    public void setQueriesAreEquivalent(boolean queriesAreEquivalent){
        this.queriesAreEquivalent = queriesAreEquivalent;
    }
    public void setTransformationSteps(List<String> transformationSteps) {
        this.transformationSteps = transformationSteps;
    }

    //Getter methods
    public boolean areQueriesEquivalent(){
        return queriesAreEquivalent;
    }

    public List<String> getTransformationSteps() {
        return transformationSteps;
    }

}
