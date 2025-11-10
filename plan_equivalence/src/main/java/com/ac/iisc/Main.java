package com.ac.iisc;

import java.util.Scanner;

/**
 * Minimal CLI entry that compares two SQL strings for equivalence.
 *
 * Flow:
 * 1) Fast textual equality check (case-insensitive).
 * 2) Calcite structural comparison without LLM.
 * 3) If still not equal, obtain cleaned plans and ask the LLM for admissible transformations,
 *    then validate via Calcite with those transformations.
 */
public class Main
{
    private static boolean compareQueries(String sqlA, String sqlB)
    {
        //Preliminary check: if the queries are textually identical, they are equivalent
        if (sqlA.equalsIgnoreCase(sqlB))
            return true;

        //Use Calcite to compare the queries for equivalence (without ChatGPT transformations)
        if (Calcite.compareQueries(sqlA, sqlB, null))
            return true;

        //Use LLM to compare the queries for equivalence (with ChatGPT transformations)
        LLMResponse resp = LLM.getLLMResponse(sqlA, sqlB);
        
        //Check the LLM response for equivalence and validate transformations using Calcite
        return resp.areQueriesEquivalent() && Calcite.compareQueries(sqlA, sqlB, resp.getTransformationSteps());
    }

    public static void main(String[] args)
    {
        try (Scanner sc = new Scanner(System.in))
        {
            System.out.print("Enter SQL query 1: ");
            String query1 = sc.nextLine();
            System.out.print("Enter SQL query 2: ");
            String query2 = sc.nextLine();
            
            System.out.println(compareQueries(query1, query2));
        }
    }

}
