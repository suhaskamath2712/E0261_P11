package com.ac.iisc;

import java.io.IOException;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class Test
{
    private static final String originalPath = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql";
    private static final String rewrittenPath = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\rewritten_queries.sql";
    //private static final String mutatedPath = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\mutated_queries.sql";

    public static void main(String[] args)
    {
        String queryId = "A1";
        try
        {
            SqlNode originalSqlNode = Calcite.SQLtoSqlNode(Calcite.loadQueryFromFile(originalPath, queryId));
            SqlNode rewrittenSqlNode = Calcite.SQLtoSqlNode(Calcite.loadQueryFromFile(rewrittenPath, queryId));
            //SqlNode mutatedSqlNode = Calcite.SQLtoSqlNode(Calcite.loadQueryFromFile(mutatedPath, queryId));

            System.out.println("Original:");
            System.out.println(originalSqlNode.toString());
            System.out.println("Rewritten:");
            System.out.println(rewrittenSqlNode.toString());

            String[] originalToRewrittenTransformations = {"AliasNormalization",
                                                            "ProjectRenameTranspose",
                                                            "FilterConditionNormalization",
                                                            "FilterPredicatePushDown",
                                                            "DateRangeSimplification",
                                                            "JoinConditionNormalization",
                                                            "AggregateGroupSetNormalization"};

            SqlNode originalToMutatedSqlNode = Calcite.applyTransformations(originalSqlNode, originalToRewrittenTransformations);
            System.out.println("Original to Rewritten:");
            System.out.println(originalToMutatedSqlNode.toString());
            System.out.println("Rewritten:");
            System.out.println(rewrittenSqlNode.toString());
            System.out.println("Are they equal? " + originalToMutatedSqlNode.toString().equals(rewrittenSqlNode.toString()));
        }
        catch (SqlParseException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
