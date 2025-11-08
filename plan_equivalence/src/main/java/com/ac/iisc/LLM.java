package com.ac.iisc;

import java.sql.SQLException;
import java.util.List;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;

public class LLM
{
    private static final String BASE_PROMPT = """
            Compare these two query plans and if they are equivalent, output true. Otherwise, output false.\n
            If true, only the following:\n
            A list of Apache calcite transformations that can map from the first plan to the second plan.\n
            DO NOT OUTPUT ANYTHING ELSE. DO NOT GIVE ANY EXPLANATIONS.\n
            Output transformations from the following supported list only:\n
            If no transformations can map the first plan to the second plan, output "No transformations found".\n
            If no transformations are needed (plans are identical), output "No transformations needed".\n
            """;
    
    private static final List<String> SUPPORTED_TRANSFORMATIONS = List.of(
        // Projection rules
        "ProjectMergeRule",
        "ProjectRemoveRule",
        "ProjectJoinTransposeRule",
        "ProjectFilterTransposeRule",
        "ProjectSetOpTransposeRule",
        "ProjectTableScanRule",
        // Filter rules
        "FilterMergeRule",
        "FilterProjectTransposeRule",
        "FilterJoinRule",
        "FilterAggregateTransposeRule",
        "FilterWindowTransposeRule",
        // Join rules
        "JoinCommuteRule",
        "JoinAssociateRule",
        "JoinPushExpressionsRule",
        "JoinConditionPushRule",
        // Aggregate rules
        "AggregateProjectPullUpConstantsRule",
        "AggregateRemoveRule",
        "AggregateJoinTransposeRule",
        "AggregateUnionTransposeRule",
        "AggregateProjectMergeRule",
        "AggregateCaseToFilterRule",
        // Sort & limit rules
        "SortRemoveRule",
        "SortUnionTransposeRule",
        "SortProjectTransposeRule",
        "SortJoinTransposeRule",
        // Set operation rules
        "UnionMergeRule",
        "UnionPullUpConstantsRule",
        "IntersectToDistinctRule",
        "MinusToDistinctRule",
        // Window rules
        "ProjectWindowTransposeRule",
        "FilterWindowTransposeRule"
    );

    /** Public accessor so parser/validator code can check transformation names. */
    public static List<String> getSupportedTransformations() {
        return SUPPORTED_TRANSFORMATIONS; // immutable List.of
    }
    
    public static String contactLLM(String sqlAJSON, String sqlBJSON)
    {
        //Contact ChatGPT API with the prompt and return the response
        OpenAIClient client = OpenAIOkHttpClient.fromEnv();

        String prompt = BASE_PROMPT + "\n" + SUPPORTED_TRANSFORMATIONS.toString();

        ResponseCreateParams params = ResponseCreateParams.builder()
            .input(prompt)
            .model("gpt-5")
            .build();

        Response resp = client.responses().create(params);

        return resp.toString();
    }

    public static LLMResponse getLLMResponse(String sqlA, String sqlB)
    {
        String sqlAJSON;
        String sqlBJSON;
        try
        {
            sqlAJSON = GetQueryPlans.getCleanedQueryPlanJSONasString(sqlA);
            sqlBJSON = GetQueryPlans.getCleanedQueryPlanJSONasString(sqlB);
        }
        catch (SQLException ex)
        {
            System.err.println("Error obtaining query plans: " + ex.getMessage());
            return null;
        }
        
        return new LLMResponse(contactLLM(sqlAJSON, sqlBJSON));
    }
}
