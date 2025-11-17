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
    
    //private static final List<String> queryIDList = List.of("N1");

    //list of queries to be tested (without plain union queries)
    private static final List<String> queryIDList = List.of(
        "Q2"
    );

    //Q2, Q9: Transformations that are output will crash the program
    //Q20: Goes into infinite loop

    /**
     * Small demo entrypoint: builds two example SQL statements and prints the
     * comparison result. Adjust the SQL and the transformation list as needed
     * for local experiments. Ensure the referenced tables exist in PostgreSQL.
     */
    public static void main(String[] args) throws Exception
    {
        // Example transformation list (can be null or customized)

        // For demo: compare each query in the list between original and mutated
        int LLMtrue = 0, LLMfalse = 0, LLMGaveTransform = 0, LLMGaveCorrectTransform = 0;
        for (String id : queryIDList)
        {
            String sqlA = FileIO.readOriginalSqlQuery(id);
            String sqlB = FileIO.readRewrittenSqlQuery(id);

            boolean result = Calcite.compareQueriesDebug(sqlA, sqlB, null, id);
            System.out.print("Query ID: " + id + "\t" + result + "\t");

            if (result)
            {
                System.out.println();
                continue; // No need to check transformations if equivalence is proved without LLM
            }

            List<String> transformations = List.of(
                "AggregateJoinTransposeRule",
                "FilterJoinRule.FilterIntoJoinRule",
                "ProjectJoinTransposeRule",
                "AggregateProjectMergeRule",
                "ProjectMergeRule"
            );

            // Apply transformations
            result = Calcite.compareQueriesDebug(sqlA, sqlB, transformations, id);

            System.out.println(result);

            // Try with transformations from LLM
            // Get Query Plan Transformations from LLM
            /*LLMResponse llmResponse = LLM.getLLMResponse(sqlA, sqlB);

            boolean doesLLMThinkEquivalent = llmResponse.areQueriesEquivalent();
            System.out.print(doesLLMThinkEquivalent + "\t");

            if (doesLLMThinkEquivalent)
            {
                LLMtrue++;
                //extract transformations only if LLM says equivalent
                List<String> llmTransformations = llmResponse.getTransformationSteps();
                
                System.out.print(llmTransformations + "\t");

                if (!llmTransformations.isEmpty())
                {
                    LLMGaveTransform++; 
                    // Apply transformations
                    result = Calcite.compareQueries(sqlA, sqlB, llmTransformations);

                    if (result)
                        LLMGaveCorrectTransform++;

                    System.out.println(result);
                }
            }
            else LLMfalse++;*/

            //System.out.println("---------------------------------------------------");
        }

        //Print statistics
        System.out.println("Total Queries Processed: " + queryIDList.size());
        System.out.println("Offline Proven Equivalent Queries: " + (queryIDList.size() - (LLMtrue + LLMfalse)));
        System.out.println("LLM predicted equivalence for: " + LLMtrue);
        System.out.println("LLM predicted non-equivalence for: " + LLMfalse);
        System.out.println("LLM provided transformations for: " + LLMGaveTransform);
        System.out.println("LLM provided correct transformations for: " + LLMGaveCorrectTransform);
    }
}
