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
    
    private static final List<String> queryIDList = List.of("O2");

    /*
    private static final List<String> queryIDList = List.of(
        "O2", "O3", "O4", "O5", "O6",
        "A1", "A2", "A3", "A4", "A5",
        "N1",
        "F1", "F2", "F3", "F4",
        "MQ1", "MQ2", "MQ3", "MQ4", "MQ5", "MQ6", "MQ10", "MQ11", "MQ17", "MQ18", "MQ21",
        "Alaap", "Nested_Test", "paper_sample"
    );
    */

    /**
     * Small demo entrypoint: builds two example SQL statements and prints the
     * comparison result. Adjust the SQL and the transformation list as needed
     * for local experiments. Ensure the referenced tables exist in PostgreSQL.
     */
    public static void main(String[] args) throws Exception
    {
        // Example transformation list (can be null or customized)

        // For demo: compare each query in the list between original and mutated
        int tests = 0, truevalues = 0;
        for (String id : queryIDList)
        {
            String sqlA = FileIO.readOriginalSqlQuery(id);
            String sqlB = FileIO.readRewrittenSqlQuery(id);

            boolean result = Calcite.compareQueriesDebug(sqlA, sqlB, null, id);
            System.out.print("Query ID: " + id + "\t" + result + "\t");

            if (result)
            {
                System.out.println();
                truevalues++;
                tests++;
                continue; // No need to check transformations if already equivalent
            }

            List<String> transformations = List.of("UnionMergeRule");
            result = Calcite.compareQueriesDebug(sqlA, sqlB, transformations, id+"+rules");
            System.out.println(result);

            if (result)     truevalues++;
            tests++;
        }

        System.out.println("Total tests: " + tests + ", True results after transformations: " + truevalues);
    }
}
