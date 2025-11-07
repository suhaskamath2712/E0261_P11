/*
 * =====================================================================================
 *  FileIO.java
 *
 *  Purpose
 *  -------
 *  Centralizes file input/output helpers used across the plan_equivalence module.
 *  This keeps low-level I/O (UTF-8 text reads, plan file reads) consistent and
 *  testable, and avoids sprinkling java.nio usage throughout the codebase.
 *
 *  What it provides
 *  ----------------
 *  - readTextFile(String): Read a UTF-8 text file by absolute path with validation.
 *  - readOriginalQueryPlan(String): Load a plan JSON string for a given Query ID
 *    from the original_query_plans directory.
 *  - readRewrittenQueryPlan(String): Same for rewritten_query_plans.
 *  - readMutatedQueryPlan(String): Same for mutated_query_plans.
 *
 *  Conventions
 *  -----------
 *  - Query plan files are expected to be stored as JSON: <QueryID>.json
 *    If your naming scheme includes a suffix (e.g., <QueryID>_original.json), you
 *    can pass the full filename (with .json) as the queryId parameter and it will
 *    be used as-is (no additional suffix is appended by these helpers).
 *
 *  Error handling
 *  --------------
 *  - Methods validate inputs (null/blank checks) and throw IllegalArgumentException
 *    for programming errors.
 *  - File-not-found and read failures produce IOException with a clear message.
 *
 *  Thread-safety
 *  -------------
 *  - This class is stateless and thread-safe. Methods do not share mutable state.
 *
 *  Extensibility
 *  -------------
 *  - If you add more plan folders or change root paths, update the constants below.
 *  - If you want to return parsed JSON (e.g., org.json.JSONObject or Jackson Map),
 *    add new methods that parse the Strings returned by these helpers.
 *
 * =====================================================================================
 */
package com.ac.iisc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Small, focused I/O utility. All methods are static for easy reuse from any
 * part of the application (CLI tools, tests, calcite helpers, etc.).
 */
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


	// Absolute paths to the SQL collections (under the sql_queries/ folder)
	private static final String ORIGINAL_SQL_PATH =
			"C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql";

	private static final String REWRITTEN_SQL_PATH =
			"C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\rewritten_queries.sql";

	private static final String MUTATED_SQL_PATH =
			"C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\mutated_queries.sql";

	// Read SQL files

	/** Source for a query when reading from the consolidated SQL files. */
	public enum SqlSource { ORIGINAL, REWRITTEN, MUTATED }

	/**
	 * Read a specific SQL query by its Query ID from one of the consolidated SQL
	 * files (original, rewritten, or mutated).
	 *
	 * File format assumptions (based on provided files):
	 *  - Each query is preceded by a header block with lines like:
	 *      -- =================================================================
	 *      -- Query ID: U1 (optional_suffix)
	 *      -- Description: ...
	 *      -- =================================================================
	 *  - The SQL text for that query follows until the next header line that
	 *    starts with "-- ===============" or EOF.
	 *  - The "Query ID:" line may include an optional parenthetical suffix such
	 *    as "(Rewritten)" or "(Mutated)" which should still match the same ID.
	 *
	 * @param source Which SQL file to read from
	 * @param queryId The ID to look up (e.g., "U1", "O4", "A3", etc.)
	 * @return The SQL text for the requested query, trimmed. Includes semicolons/comments inside the block.
	 * @throws IOException If the file can't be read or the query block can't be found
	 */
	public static String readSqlQuery(SqlSource source, String queryId) throws IOException
	{
		if (queryId == null || queryId.isBlank()) {
			throw new IllegalArgumentException("queryId must not be null or blank");
		}
		final String path = switch (source) {
			case ORIGINAL -> ORIGINAL_SQL_PATH;
			case REWRITTEN -> REWRITTEN_SQL_PATH;
			case MUTATED -> MUTATED_SQL_PATH;
		};
		String content = readTextFile(path);
		String sql = extractSqlBlockById(content, queryId);
		if (sql == null) {
			throw new IOException("Query ID '" + queryId + "' not found in " + path);
		}
		return sql;
	}

	/** Convenience: read from original SQL collection. */
	public static String readOriginalSqlQuery(String queryId) throws IOException {
		return readSqlQuery(SqlSource.ORIGINAL, queryId);
	}

	/** Convenience: read from rewritten SQL collection. */
	public static String readRewrittenSqlQuery(String queryId) throws IOException {
		return readSqlQuery(SqlSource.REWRITTEN, queryId);
	}

	/** Convenience: read from mutated SQL collection. */
	public static String readMutatedSqlQuery(String queryId) throws IOException {
		return readSqlQuery(SqlSource.MUTATED, queryId);
	}

	/**
	 * Parse the given SQL file content and extract the SQL text for the section
	 * that corresponds to the provided Query ID.
	 */
	private static String extractSqlBlockById(String fileContent, String queryId) {
		if (fileContent == null) return null;

		// 1) Find the header line that contains "-- Query ID: <ID>" with optional suffix e.g. (Rewritten)
		Pattern headerPattern = Pattern.compile(
				"(?m)^--\\s*Query ID:\\s*" + Pattern.quote(queryId) + "\\b(?:\\s*\\([^)]*\\))?\\s*$");
		Matcher headerMatcher = headerPattern.matcher(fileContent);
		if (!headerMatcher.find()) {
			return null; // Query ID not present
		}

		int afterHeaderPos = headerMatcher.end();

		// 2) Find the next separator line (the one after Description), then SQL starts after it
		Pattern sepPattern = Pattern.compile("(?m)^--\\s*=+\\s*$");
		Matcher sepAfterDesc = sepPattern.matcher(fileContent);
		if (!sepAfterDesc.find(afterHeaderPos)) {
			return null; // Malformed block: no separator after header/description
		}
		int sqlStart = sepAfterDesc.end();

		// 3) The SQL block ends right before the next separator line that begins a new block, or EOF
		Matcher nextHeader = sepPattern.matcher(fileContent);
		if (nextHeader.find(sqlStart)) {
			int sqlEnd = nextHeader.start();
			return fileContent.substring(sqlStart, sqlEnd).trim();
		} else {
			// No more headers; consume until EOF
			return fileContent.substring(sqlStart).trim();
		}
	}
	

	/**
	 * Internal helper to read a plan JSON file for a given query ID from a folder.
	 *
	 * Behavior:
	 *  - If queryId ends with ".json", it is treated as a file name (no change).
	 *  - Otherwise, ".json" is appended to form the file name.
	 *  - Reads the file content as UTF-8 and returns it as a String.
	 *
	 * @param baseDir The directory that contains the plan files (e.g., ORIGINAL_PLANS_DIR)
	 * @param queryId The query identifier (e.g., "TPCH_Q11" or "TPCH_Q11.json")
	 * @return JSON content as a String (UTF-8)
	 * @throws IllegalArgumentException if queryId is null or blank
	 * @throws IOException if file does not exist or cannot be read
	 */
	private static String readPlanFrom(Path baseDir, String queryId) throws IOException {
		if (queryId == null || queryId.isBlank()) {
			throw new IllegalArgumentException("queryId must not be null or blank");
		}
		// Accept both bare IDs and explicit filenames.
		String fileName = queryId.endsWith(".json") ? queryId : queryId + ".json";
		Path filePath = baseDir.resolve(fileName);
		if (!Files.exists(filePath)) {
			throw new IOException("Plan file not found: " + filePath.toString());
		}
		// Read and return raw JSON content (callers can parse if needed).
		return Files.readString(filePath, StandardCharsets.UTF_8);
	}

	/**
	 * Reads and returns the JSON plan content for a given query ID from the
	 * original_query_plans folder.
	 *
	 * @param queryId The identifier of the query (with or without .json extension)
	 * @return JSON content as a String (UTF-8)
	 * @throws IllegalArgumentException if queryId is null or blank
	 * @throws IOException if the file does not exist or cannot be read
	 */
	public static String readOriginalQueryPlan(String queryId) throws IOException {
		return readPlanFrom(ORIGINAL_PLANS_DIR, queryId);
	}

	/**
	 * Reads and returns the JSON plan for a given query ID from the
	 * rewritten_query_plans folder.
	 *
	 * @param queryId The identifier of the query (with or without .json extension)
	 * @return JSON content as a String (UTF-8)
	 * @throws IllegalArgumentException if queryId is null or blank
	 * @throws IOException if the file does not exist or cannot be read
	 */
	public static String readRewrittenQueryPlan(String queryId) throws IOException {
		return readPlanFrom(REWRITTEN_PLANS_DIR, queryId);
	}

	/**
	 * Reads and returns the JSON plan for a given query ID from the
	 * mutated_query_plans folder.
	 *
	 * @param queryId The identifier of the query (with or without .json extension)
	 * @return JSON content as a String (UTF-8)
	 * @throws IllegalArgumentException if queryId is null or blank
	 * @throws IOException if the file does not exist or cannot be read
	 */
	public static String readMutatedQueryPlan(String queryId) throws IOException {
		return readPlanFrom(MUTATED_PLANS_DIR, queryId);
	}

	/**
	 * Read a text file from an absolute path as UTF-8 and return its content.
	 * This centralizes file I/O for consumers like Calcite.
	 *
	 * @param absolutePath Absolute path to the file to read
	 * @return File content as UTF-8 String
	 * @throws IllegalArgumentException if absolutePath is null/blank
	 * @throws IOException if file does not exist or cannot be read
	 */
	public static String readTextFile(String absolutePath) throws IOException {
		if (absolutePath == null || absolutePath.isBlank()) {
			throw new IllegalArgumentException("absolutePath must not be null or blank");
		}
		Path p = Paths.get(absolutePath);
		if (!Files.exists(p)) {
			throw new IOException("File not found: " + p.toString());
		}
		return Files.readString(p, StandardCharsets.UTF_8);
	}

	/** Ensure that a directory exists; if not, create it (including parents). */
	public static void ensureDirectory(String absoluteDirPath) throws IOException {
		if (absoluteDirPath == null || absoluteDirPath.isBlank()) {
			throw new IllegalArgumentException("absoluteDirPath must not be null or blank");
		}
		Path dir = Paths.get(absoluteDirPath);
		if (!Files.exists(dir)) {
			Files.createDirectories(dir);
		}
	}

	/** Write UTF-8 text content to a file, creating parent directories if needed. */
	public static void writeTextFile(String absolutePath, String content) throws IOException {
		if (absolutePath == null || absolutePath.isBlank()) {
			throw new IllegalArgumentException("absolutePath must not be null or blank");
		}
		Path p = Paths.get(absolutePath);
		Path parent = p.getParent();
		if (parent != null && !Files.exists(parent)) {
			Files.createDirectories(parent);
		}
		Files.writeString(p, content == null ? "" : content, StandardCharsets.UTF_8);
	}
}
