package com.ac.iisc;

import java.io.File;

public class Main
{

    private final String original_json_path = "C:\\Users\\suhas\\Downloads\\E0261_P11\\original_query_plans";
    private static final String rewritten_json_path = "C:\\Users\\suhas\\Downloads\\E0261_P11\\rewritten_query_plans\\";
    //private final String mutilated_json_path = "C:\\Users\\suhas\\Downloads\\E0261_P11\\mutilated_query_plans";

    public static void main (String[] args)
    {
        File[] original_jsons = Util.getJSONs(new Main().original_json_path);
        for (File original_json : original_jsons)
        {
            String original_json_string = Util.readFile(original_json.getAbsolutePath());
            String original_query_id = original_json.getName().split("_")[0];
            
            String rewritten_json = Util.readFile(rewritten_json_path + original_query_id);

            System.out.println("Comparing " + original_query_id);

            LLM llm = new LLM();
            String response = llm.getResponse(original_json_string, rewritten_json);
            System.out.println("Response: " + response);
            System.out.println();
            
            break; // Remove this break to process all files
        }
    }

}
