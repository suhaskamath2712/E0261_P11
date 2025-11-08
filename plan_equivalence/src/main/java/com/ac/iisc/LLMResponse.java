package com.ac.iisc;

import java.util.List;

public class LLMResponse
{
    private boolean queriesAreEquivalent;
    private List<String> transformationSteps;

    public LLMResponse(boolean queriesAreEquivalent, List<String> transformationSteps)
    {
        this.queriesAreEquivalent = queriesAreEquivalent;
        this.transformationSteps = transformationSteps;
    }

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
