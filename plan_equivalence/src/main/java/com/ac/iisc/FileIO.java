package com.ac.iisc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileIO {

	// Base directory for original query plans
	private static final Path ORIGINAL_PLANS_DIR = Paths.get(
			"C:\\Users\\suhas\\Downloads\\E0261_P11\\original_query_plans");

	// Base directory for rewritten query plans
	private static final Path REWRITTEN_PLANS_DIR = Paths.get(
			"C:\\Users\\suhas\\Downloads\\E0261_P11\\rewritten_query_plans");

	// Base directory for mutated query plans
	private static final Path MUTATED_PLANS_DIR = Paths.get(
			"C:\\Users\\suhas\\Downloads\\E0261_P11\\mutated_query_plans");

	private static String readPlanFrom(Path baseDir, String queryId) throws IOException {
		if (queryId == null || queryId.isBlank()) {
			throw new IllegalArgumentException("queryId must not be null or blank");
		}
		String fileName = queryId.endsWith(".json") ? queryId : queryId + ".json";
		Path filePath = baseDir.resolve(fileName);
		if (!Files.exists(filePath)) {
			throw new IOException("Plan file not found: " + filePath.toString());
		}
		return Files.readString(filePath, StandardCharsets.UTF_8);
	}

	/**
	 * Reads and returns the JSON plan content for a given query ID from the
	 * original_query_plans folder.
	 *
	 * @param queryId The identifier of the query (with or without .json extension)
	 * @return JSON content as a String (UTF-8)
	 * @throws IllegalArgumentException if queryId is null or blank
	 * @throws IOException              if the file does not exist or cannot be read
	 */
	public static String readOriginalQueryPlan(String queryId) throws IOException {
		return readPlanFrom(ORIGINAL_PLANS_DIR, queryId);
	}

	/** Reads and returns the JSON plan for a given query ID from the rewritten_query_plans folder. */
	public static String readRewrittenQueryPlan(String queryId) throws IOException {
		return readPlanFrom(REWRITTEN_PLANS_DIR, queryId);
	}

	/** Reads and returns the JSON plan for a given query ID from the mutated_query_plans folder. */
	public static String readMutatedQueryPlan(String queryId) throws IOException {
		return readPlanFrom(MUTATED_PLANS_DIR, queryId);
	}
}
