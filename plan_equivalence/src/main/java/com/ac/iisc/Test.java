package com.ac.iisc;

import java.util.List;


public class Test
{
    /**
     * List of all query IDs present in the SQL collections (original, rewritten, mutated).
     * This enables batch processing, testing, or iteration over all available queries.
     * Update this list if new queries are added to the SQL files.
     
    private static final List<String> queryIDList = List.of(
        "U1", "U2", "U3", "U4", "U5", "U6", "U7", "U8", "U9",
        "O1", "O2", "O3", "O4", "O5", "O6",
        "A1", "A2", "A3", "A4", "A5",
        "N1",
        "F1", "F2", "F3", "F4",
        "MQ1", "MQ2", "MQ3", "MQ4", "MQ5", "MQ6", "MQ10", "MQ11", "MQ17", "MQ18", "MQ21",
        "Alaap", "Nested_Test", "paper_sample"
    );*/
    
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
        "F1", "F2",
        // TPC-H Modified (MQ)
        "MQ1", "MQ2", "MQ3", "MQ4", "MQ5", "MQ6", "MQ10", "MQ11", "MQ17", "MQ18", "MQ21",
        // Standard TPC-H
        "TPCH_Q9", "TPCH_Q13",
        // Extended TPC-H (ETPCH)
        "ETPCH_Q1", "ETPCH_Q3", "ETPCH_Q4", "ETPCH_Q5", "ETPCH_Q6", "ETPCH_Q6_1", "ETPCH_Q6_2",
        "ETPCH_Q7", "ETPCH_Q9", "ETPCH_Q10", "ETPCH_Q12", "ETPCH_Q14", "ETPCH_Q15", "ETPCH_Q21",
        "ETPCH_Q23", "ETPCH_Q24",
        // LITHE Series
        "LITHE_1", "LITHE_2", "LITHE_3", "LITHE_4", "LITHE_5", "LITHE_6", "LITHE_7", "LITHE_8", "LITHE_9",
        "LITHE_10", "LITHE_11", "LITHE_12", "LITHE_13", "LITHE_14", "LITHE_15", "LITHE_16", "LITHE_17",
        "LITHE_18", "LITHE_19", "LITHE_20", "LITHE_21", "LITHE_22"
    );

    //Q17, Q20 : Goes into infinite loop
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

            System.out.print("Query ID: " + id + "\t" + Calcite.compareQueries(sqlA, sqlB, null) + "\t");

            // Try with transformations from LLM
            // Get Query Plan Transformations from LLM
            /*LLMResponse llmResponse = LLM.getLLMResponse(sqlA, sqlB);

            boolean doesLLMThinkEquivalent = llmResponse.areQueriesEquivalent();
            System.out.print(doesLLMThinkEquivalent + "\t");

            if (doesLLMThinkEquivalent)
            {
                //extract transformations only if LLM says equivalent
                List<String> llmTransformations = llmResponse.getTransformationSteps();
                
                System.out.print(llmTransformations + "\t");

                if (!llmTransformations.isEmpty())
                    System.out.println(Calcite.compareQueries(sqlA, sqlB, llmTransformations));
            }*/

            System.out.println();
        } 
    }
}
