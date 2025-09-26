package com.ac.iisc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Util
{
    //Read JSON file to string
    public static String readFile(String filePath)
    {
        File jsonFile = new File(filePath);
        StringBuilder jsonString = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(jsonFile)))
        {
            String line;
            while ((line = br.readLine()) != null)
            {
                jsonString.append(line);
                jsonString.append(System.lineSeparator());
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return jsonString.toString();
    }

    //Get files in a directory
    public static File[] getJSONs(String directoryPath)
    {
        File dir = new File(directoryPath);
        if (dir.isDirectory())
        {
            return dir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"));
        }
        return new File[0];
    }
}
