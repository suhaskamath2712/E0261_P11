package com.ac.iisc;

import java.io.IOException;

import org.apache.calcite.tools.FrameworkConfig;

public class Test
{
    private static final String ORIGINAL_PATH = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql";
    private static final String REWRITTEN_PATH = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql";
    //private static final String mutatedPath = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\mutated_queries.sql";

    public static void main(String[] args)
    {
        //Enter queryID and transformations here:
        //No other change required
        String queryId = "U1";
        String[] originalToRewrittenTransformations = {""};

        FrameworkConfig cfg = Calcite.buildPostgresFrameworkConfig("localhost",5432,"tpch","postgres","123");

        try
        {
            String sql1 = Calcite.loadQueryFromFile(ORIGINAL_PATH, queryId);
            String sql2 = Calcite.loadQueryFromFile(REWRITTEN_PATH, queryId);

            boolean equivalent = Calcite.equivalent(sql1, sql2, cfg);
            System.out.println("Are the queries equivalent? " + equivalent);
        }
        catch (IOException e)
        {
            System.err.println("[Error] I/O failure: " + e.getMessage());
        }
        catch (Exception e)
        {
            System.err.println("[Error] Failure: " + e.getMessage());
        }
        /*
        try
        {
            RelNode originalSqlNode = Calcite.toRelNode(Calcite.loadQueryFromFile(ORIGINAL_PATH, queryId), cfg);
            RelNode rewrittenSqlNode = Calcite.toRelNode(Calcite.loadQueryFromFile(REWRITTEN_PATH, queryId), cfg);
            //RelNode mutatedSqlNode = Calcite.toRelNode(Calcite.SQLtoSqlNode(Calcite.loadQueryFromFile(mutatedPath, queryId)), cfg);

            System.out.println("Original:");
            System.out.println(originalSqlNode.toString());
            System.out.println("Rewritten:");
            System.out.println(rewrittenSqlNode.toString());

            RelNode originalToMutatedRelNode = Calcite.applyRelTransformations(originalSqlNode, originalToRewrittenTransformations);
            System.out.println("Original to Rewritten:");
            System.out.println(originalToMutatedRelNode.toString());
            System.out.println("Rewritten:");
            System.out.println(rewrittenSqlNode.toString());
            System.out.println("Are the RelNodes equal? " + originalToMutatedRelNode.equals(rewrittenSqlNode));

            //Convert relnode to sqlnode
            SqlNode originalToMutatedSqlNode = Calcite.relToSqlNode(originalToMutatedRelNode);
            SqlNode rewrittenRelNodeAsSqlNode = Calcite.relToSqlNode(rewrittenSqlNode);

            System.out.println("Original to Rewritten as SQL:");
            System.out.println(originalToMutatedSqlNode.toString());
            System.out.println("Rewritten as SQL:");
            System.out.println(rewrittenRelNodeAsSqlNode.toString());
            System.out.println("Are the SQLNode strings equal? " + originalToMutatedSqlNode.equals(rewrittenRelNodeAsSqlNode));
            System.out.println("Are the SQLNode equal?" + Calcite.sqlNodesEqual(originalToMutatedSqlNode, originalToMutatedSqlNode));
        }
        catch (ValidationException ve)
        {
            System.err.println("[ValidationException] Failed to validate SQL against schema: " + ve.getMessage());
        }
        catch (RelConversionException rce)
        {
            System.err.println("[RelConversionException] Failed to convert validated SQL to RelNode: " + rce.getMessage());
        }
        catch (SqlParseException | IOException e)
        {
            System.err.println("[Error] Parse or I/O failure: " + e.getMessage());
        }*/
    }
}
