package com.ac.iisc;

import java.io.IOException;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class Test
{
    private static final String originalPath = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql";
    private static final String rewrittenPath = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\rewritten_queries.sql";
    //private static final String mutatedPath = "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\mutated_queries.sql";

    public static void main(String[] args)
    {
        //Enter queryID and transformations here:
        //No other change required
        String queryId = "A1";
        String[] originalToRewrittenTransformations = {"AliasNormalization",
                                                            "ProjectRenameTranspose",
                                                            "FilterConditionNormalization",
                                                            "FilterPredicatePushDown",
                                                            "DateRangeSimplification",
                                                            "JoinConditionNormalization",
                                                            "AggregateGroupSetNormalization"};

        FrameworkConfig cfg = Calcite.buildPostgresFrameworkConfig("localhost",5432,"tpch","postgres","123");

        try
        {
            RelNode originalSqlNode = Calcite.toRelNode(Calcite.SQLtoSqlNode(Calcite.loadQueryFromFile(originalPath, queryId)), cfg);
            RelNode rewrittenSqlNode = Calcite.toRelNode(Calcite.SQLtoSqlNode(Calcite.loadQueryFromFile(rewrittenPath, queryId)), cfg);
            //RelNode mutatedSqlNode = Calcite.toRelNode(Calcite.SQLtoSqlNode(Calcite.loadQueryFromFile(mutatedPath, queryId)), cfg);

            System.out.println("Original:");
            System.out.println(originalSqlNode.toString());
            System.out.println("Rewritten:");
            System.out.println(rewrittenSqlNode.toString());

            RelNode originalToMutatedSqlNode = Calcite.applyRelTransformations(originalSqlNode, originalToRewrittenTransformations);
            System.out.println("Original to Rewritten:");
            System.out.println(originalToMutatedSqlNode.toString());
            System.out.println("Rewritten:");
            System.out.println(rewrittenSqlNode.toString());
            System.out.println("Are they equal? " + originalToMutatedSqlNode.toString().equals(rewrittenSqlNode.toString()));
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
        }
    }
}
