package com.ac.iisc;

import java.util.List;

/**
 * Minimal demo harness to compare SQL pairs by Query ID across the consolidated
 * SQL collections and optionally consult the LLM for transformation hints.
 *
 * Notes:
 * - This is intended for quick local runs; adjust {@code queryIDList} as needed.
 * - LLM integration is optional; if plan retrieval fails, {@code getLLMResponse}
 *   returns null and the code should proceed without LLM assistance.
 */
public class Test
{
    /**
     * List of all query IDs present in the SQL collections (original, rewritten, mutated).
     * This enables batch processing, testing, or iteration over all available queries.
     * Update this list if new queries are added to the SQL files.
    */
    
    /*
    private static final List<String> queryIDList = List.of(
        // U-Series
        "U1", "U2", "U3", "U4", "U5", "U6", "U7", "U8", "U9",
        // O-Series
        "O1", "O2", "O3", "O4", "O5", "O6",
        // A-Series
        "A1", "A2", "A3", "A4",
        // Nested/Misc
        "N1", "Alaap", "Nested_Test", "paper_sample",
        // F-Series
        "F1",
        // TPC-H Modified (MQ)
        "MQ1", "MQ2", "MQ3", "MQ4", "MQ5", "MQ6", "MQ10", "MQ11", "MQ17", "MQ18", "MQ21",
        // Standard TPC-H
        "TPCH_Q9", "TPCH_Q13",
        // Extended TPC-H (ETPCH)
        "ETPCH_Q1", "ETPCH_Q4", "ETPCH_Q5", "ETPCH_Q6", "ETPCH_Q6_1", "ETPCH_Q6_2",
        "ETPCH_Q7", "ETPCH_Q9", "ETPCH_Q10", "ETPCH_Q12", "ETPCH_Q14", "ETPCH_Q15", "ETPCH_Q21",
        "ETPCH_Q23", "ETPCH_Q24",
        // LITHE Series
        "LITHE_1", "LITHE_2", "LITHE_3", "LITHE_4", "LITHE_5", "LITHE_6", "LITHE_7", "LITHE_8", "LITHE_9",
        "LITHE_10", "LITHE_11", "LITHE_12", "LITHE_13", "LITHE_14", "LITHE_15", "LITHE_16", "LITHE_17",
        "LITHE_18", "LITHE_19", "LITHE_20", "LITHE_21", "LITHE_22"
    );

    private static final List<String> queryIDList = List.of(
        "A1", "F1", "MQ11", "ETPCH_Q9", "LITHE_1", "LITHE_3", "LITHE_4",
        "LITHE_5", "LITHE_6", "LITHE_9", "LITHE_10", "LITHE_15", "LITHE_16",
        "LITHE_20", "LITHE_22"
    );
    */

    private static final List<String> queryIDList = List.of(
        "LITHE_9", "LITHE_10", "LITHE_15", "LITHE_16", "LITHE_20", "LITHE_22"
    );

    //Rewritten queries: ETPCH_Q7, ETPCH_Q9, ETPCH_Q23, LITHE_9
    // gives expected answer, but gives some error also

    //Mutated queries: MQ11, ETPCH_Q9

    /**
     * Small demo entrypoint: builds two example SQL statements and prints the
     * comparison result. Adjust the SQL and the transformation list as needed
     * for local experiments. Ensure the referenced tables exist in PostgreSQL.
     */
    public static void main(String[] args) throws Exception
    {
        for (String id : queryIDList)
        {
            String sqlA = FileIO.readOriginalSqlQuery(id);
            String sqlB = FileIO.readRewrittenSqlQuery(id);

            System.out.println("-----------------------------------------------------");
            System.out.println("Query ID: " + id);

            boolean equivalence = Calcite.compareQueries(sqlA, sqlB, null);
            System.out.println("Equivalence without transformations: " + equivalence);

            //If RelNodes are equivalent, skip LLM call
            if (equivalence)    continue;

            LLMResponse llmResponse = LLM.getLLMResponse(sqlA, sqlB);
            System.out.println("LLM Equivalence 1: " + llmResponse.areQueriesEquivalent());

            if (!llmResponse.areQueriesEquivalent()) continue;

            //Perform transformation-based equivalence check only if transformations are provided
            if (llmResponse.getTransformationSteps() != null)
            {
                System.out.println("LLM Transformations 1: " + llmResponse.getTransformationSteps());
                System.out.println("Equivalence with transformations: " + Calcite.compareQueries(sqlA, sqlB, llmResponse.getTransformationSteps()));
            }
            
            llmResponse = LLM.getLLMResponse(sqlA, sqlB, llmResponse);
            System.out.println("LLM Equivalence 2: " + llmResponse.areQueriesEquivalent());

            if (!llmResponse.areQueriesEquivalent()) continue;

            if (llmResponse.getTransformationSteps() != null)
            {
                System.out.println("LLM Transformations 2: " + llmResponse.getTransformationSteps());
                System.out.println("Equivalence with transformations: " + Calcite.compareQueries(sqlA, sqlB, llmResponse.getTransformationSteps()));
            }

            //If still false, we attempt to get transformations to map from B to A

            llmResponse = LLM.getLLMResponse(sqlB, sqlA);
            System.out.println("LLM Equivalence 1 (B to A): " + llmResponse.areQueriesEquivalent());

            if (!llmResponse.areQueriesEquivalent()) continue;

            //Perform transformation-based equivalence check only if transformations are provided
            if (llmResponse.getTransformationSteps() != null)
            {
                System.out.println("LLM Transformations 1 (B to A): " + llmResponse.getTransformationSteps());
                System.out.println("Equivalence with transformations (B to A) " + Calcite.compareQueries(sqlA, sqlB, llmResponse.getTransformationSteps()));
            }
            
            llmResponse = LLM.getLLMResponse(sqlB, sqlA, llmResponse);
            System.out.println("LLM Equivalence 2 (B to A): " + llmResponse.areQueriesEquivalent());

            if (!llmResponse.areQueriesEquivalent()) continue;

            if (llmResponse.getTransformationSteps() != null)
            {
                System.out.println("LLM Transformations 2 (B to A): " + llmResponse.getTransformationSteps());
                System.out.println("Equivalence with transformations (B to A): " + Calcite.compareQueries(sqlA, sqlB, llmResponse.getTransformationSteps()));
            }
            
        } 
    }
}
