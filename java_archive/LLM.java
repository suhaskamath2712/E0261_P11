package com.ac.iisc;

import java.io.IOException;

import org.json.JSONObject;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class LLM 
{
    private static final String url = "https://api.openai.com/v1/chat/completions";
    private static final String API_KEY = "Main nahi Bataunga"; // Replace with your actual API key
    //private final String model = "gemini-1.5-flash";
    private static final String prompt = "Which model is being used?";

    public static String getResponse (String json1, String json2)
    {
        OkHttpClient client = new OkHttpClient();

        //JSON Request Body
        JSONObject json = new JSONObject();

        json.put("model", "gpt-4o-mini");
        json.put("messages", new Object[]{
            new JSONObject().put("role","user").put("content",prompt)
        });

        RequestBody body = RequestBody.create(json.toString(), MediaType.parse("application/json; charset=utf-8"));

        //Build Request
        Request request = new Request.Builder()
            .url(url)
            .header("Authorization", "Bearer " + API_KEY)
            .post(body)
            .build();

        //Execute Request
        try (Response r = client.newCall(request).execute())
        {
            if (!r.isSuccessful())
                throw new RuntimeException("An error occurred :-)." + r);
            
            String responseBody = (r.body() != null) ? r.body().string() : "";
            System.out.print(responseBody);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return "";
    }
}