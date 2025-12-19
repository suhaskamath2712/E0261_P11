package com.ac.iisc;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Frameworks;

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
        "TPCDS_Q1", "TPCDS_Q2", "TPCDS_Q3", "TPCDS_Q4", "TPCDS_Q5", "TPCDS_Q6",
        "TPCDS_Q7", "TPCDS_Q8", "TPCDS_Q9", "TPCDS_Q10", "TPCDS_Q11", "TPCDS_Q12",
        "TPCDS_Q13", "TPCDS_Q14", "TPCDS_Q15", "TPCDS_Q16", "TPCDS_Q17", "TPCDS_Q18",
        "TPCDS_Q19", "TPCDS_Q20", "TPCDS_Q21", "TPCDS_Q22", "TPCDS_Q23", "TPCDS_Q24",
        "TPCDS_Q25", "TPCDS_Q26", "TPCDS_Q27", "TPCDS_Q28", "TPCDS_Q29", "TPCDS_Q30",
        "TPCDS_Q31", "TPCDS_Q32", "TPCDS_Q33", "TPCDS_Q34", "TPCDS_Q35", "TPCDS_Q36",
        "TPCDS_Q37", "TPCDS_Q38", "TPCDS_Q39", "TPCDS_Q40", "TPCDS_Q41", "TPCDS_Q42",
        "TPCDS_Q43", "TPCDS_Q44", "TPCDS_Q45", "TPCDS_Q46", "TPCDS_Q47", "TPCDS_Q48",
        "TPCDS_Q49", "TPCDS_Q50", "TPCDS_Q51", "TPCDS_Q52", "TPCDS_Q53", "TPCDS_Q54",
        "TPCDS_Q55", "TPCDS_Q56", "TPCDS_Q57", "TPCDS_Q58", "TPCDS_Q59", "TPCDS_Q60",
        "TPCDS_Q61", "TPCDS_Q62", "TPCDS_Q63", "TPCDS_Q64", "TPCDS_Q65", "TPCDS_Q66",
        "TPCDS_Q67", "TPCDS_Q68", "TPCDS_Q69", "TPCDS_Q70", "TPCDS_Q71", "TPCDS_Q72",
        "TPCDS_Q73", "TPCDS_Q74", "TPCDS_Q75", "TPCDS_Q76", "TPCDS_Q77", "TPCDS_Q78",
        "TPCDS_Q79", "TPCDS_Q80", "TPCDS_Q81", "TPCDS_Q82", "TPCDS_Q83", "TPCDS_Q84",
        "TPCDS_Q85", "TPCDS_Q86", "TPCDS_Q87", "TPCDS_Q88", "TPCDS_Q89", "TPCDS_Q90",
        "TPCDS_Q91", "TPCDS_Q92", "TPCDS_Q93", "TPCDS_Q94", "TPCDS_Q95", "TPCDS_Q96",
        "TPCDS_Q97", "TPCDS_Q98", "TPCDS_Q99"
    );

    private static final List<String> queryIDList = List.of(
        "TPCDS_Q1", "TPCDS_Q5", "TPCDS_Q6", "TPCDS_Q9", "TPCDS_Q24",
        "TPCDS_Q30", "TPCDS_Q34", "TPCDS_Q41", "TPCDS_Q44", "TPCDS_Q46",
        "TPCDS_Q47", "TPCDS_Q53", "TPCDS_Q63", "TPCDS_Q65", "TPCDS_Q69",
        "TPCDS_Q81", "TPCDS_Q86", "TPCDS_Q89", "TPCDS_Q93", "TPCDS_Q96"
    );

    */

    private static final List<String> queryIDList = List.of(
        "TPCDS_Q47"
    );

    //A1, F1 are not equivalent in reality

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

            //ROUND 1 Check
            LLMResponse llmResponse = LLM.getLLMResponse(sqlA, sqlB);
            System.out.println("LLM Equivalence A->B 1: " + llmResponse.areQueriesEquivalent());

            if (llmResponse.areQueriesEquivalent())
                System.out.println("LLM Transformations A->B 1: " + llmResponse.getTransformationSteps());

            if (llmResponse.getTransformationSteps() != null && llmResponse.getTransformationSteps().size() > 0)
            {
                boolean check = Calcite.compareQueries(sqlA, sqlB, llmResponse.getTransformationSteps());
                System.out.println("Equivalence with transformations: " + check);
                //If transformations lead to equivalence, skip second LLM call
                if (check) continue;
            }

            //ROUND 2 check - Only if round 1 gives equivalent but transformations are wrong
            if (llmResponse.areQueriesEquivalent())
            {
                llmResponse = LLM.getLLMResponse(sqlA, sqlB, llmResponse);
                System.out.println("LLM Equivalence A->B 2: " + llmResponse.areQueriesEquivalent());

                if (llmResponse.areQueriesEquivalent())
                    System.out.println("LLM Transformations A->B 2: " + llmResponse.getTransformationSteps());

                if (llmResponse.getTransformationSteps() != null && llmResponse.getTransformationSteps().size() > 0)
                {
                    boolean check = Calcite.compareQueries(sqlA, sqlB, llmResponse.getTransformationSteps());
                    System.out.println("Equivalence with transformations: " + check);
                    //If transformations lead to equivalence, skip second LLM call
                    if (check) continue;
                }
            }

            //Repeat round 1 & 2 for B->A
            llmResponse = LLM.getLLMResponse(sqlB, sqlA);
            System.out.println("LLM Equivalence B->A 1: " + llmResponse.areQueriesEquivalent());

            if (llmResponse.areQueriesEquivalent())
                System.out.println("LLM Transformations B->A 1: " + llmResponse.getTransformationSteps());

            if (llmResponse.getTransformationSteps() != null && llmResponse.getTransformationSteps().size() > 0)
            {
                boolean check = Calcite.compareQueries(sqlB, sqlA, llmResponse.getTransformationSteps());
                System.out.println("Equivalence with transformations: " + check);
                //If transformations lead to equivalence, skip second LLM call
                if (check) continue;
            }

            //ROUND 2 check - Only if round 1 gives equivalent but transformations are wrong
            if (llmResponse.areQueriesEquivalent())
            {
                llmResponse = LLM.getLLMResponse(sqlB, sqlA, llmResponse);
                System.out.println("LLM Equivalence B->A 2: " + llmResponse.areQueriesEquivalent());

                if (llmResponse.areQueriesEquivalent())
                    System.out.println("LLM Transformations B->A 2: " + llmResponse.getTransformationSteps());

                if (llmResponse.getTransformationSteps() != null && llmResponse.getTransformationSteps().size() > 0)
                {
                    boolean check = Calcite.compareQueries(sqlB, sqlA, llmResponse.getTransformationSteps());
                    System.out.println("Equivalence with transformations: " + check);
                    //If transformations lead to equivalence, skip second LLM call
                    if (check) continue;
                }
            }
            
            //If still false, we try to get A -> X then B -> X transformations and check if they lead to same RelNode
            llmResponse = LLM.getLLMResponse(sqlA, sqlB);
            System.out.println("LLM Equivalence A->X: " + llmResponse.areQueriesEquivalent());

            if (!llmResponse.areQueriesEquivalent()) continue;

            //Perform transformation-based equivalence check only if transformations are provided
            System.out.println("LLM Transformations (A to X): " + llmResponse.getTransformationSteps());
            RelNode intermediateRel = Calcite.getOptimizedRelNode(Frameworks.getPlanner(Calcite.getFrameworkConfig()), sqlA);
            String intermediateSql = Calcite.relNodeToSql(intermediateRel);
            llmResponse = LLM.getLLMResponse(sqlB, intermediateSql);
            System.out.println("LLM Equivalence 2 B->X: " + llmResponse.areQueriesEquivalent());

            if (!llmResponse.areQueriesEquivalent()) continue;

            System.out.println("LLM Transformations (B to X): " + llmResponse.getTransformationSteps());
            System.out.println("Equivalence: " + Calcite.compareQueries(sqlB, intermediateSql, llmResponse.getTransformationSteps()));
        } 
    }
}
