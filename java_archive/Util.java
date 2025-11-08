package com.ac.iisc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Miscellaneous utilities for simple file operations used in experiments and
 * quick scripts. Prefer {@link FileIO} for production-focused I/O helpers
 * (paths, encoding, error messages). This class keeps minimal helpers that are
 * convenient in ad-hoc contexts.
 */
public class Util
{
    /**
     * Read a text file (commonly JSON) into a String using the platform default
     * charset via a simple BufferedReader loop.
     *
     * Inputs/outputs:
     * - filePath: Absolute or relative path to file.
     * - Returns: Entire file content with system line separators preserved between lines.
     *
     * Error handling:
     * - On IOException, prints stack trace and returns whatever content has been
     *   read so far (possibly empty). For robust error reporting/use, prefer
     *   {@link FileIO#readTextFile(String)} which throws on failure.
     */
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
            System.err.println("[Util.readFile] Failed to read file '" + filePath + "': " + e.getMessage());
        }
        return jsonString.toString();
    }

    /**
     * List all JSON files within a directory (non-recursive).
     *
     * Inputs/outputs:
     * - directoryPath: Directory to scan.
     * - Returns: Array of File objects whose names end with ".json" (case-insensitive).
     *   If the path is not a directory, returns an empty array.
     */
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
