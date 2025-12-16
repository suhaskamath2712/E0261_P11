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
 * - readSchemaSummary(): Load a JSON schema summary (e.g., `tpch_schema_summary.json`) used
 *   by canonicalization code to obtain table primary/foreign key metadata for conservative
 *   schema-aware transformations and checks.
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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Small, focused I/O utility. All methods are static for easy reuse from any
 * part of the application (CLI tools, tests, calcite helpers, etc.).
 */
public class FileIO 
{
    // Config handling
	private static volatile Properties CONFIG;
	private static final String DEFAULT_CONFIG_RESOURCE = "/home/suhas/Desktop/E0261_P11/plan_equivalence/src/main/resources/config.properties";

    // Absolute paths to the SQL collections (under the sql_queries/ folder)
    private static final String ORIGINAL_SQL_PATH = getProperty(
	    "original_sql_path",
	    "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql"
    );

    private static final String REWRITTEN_SQL_PATH = getProperty(
	    "rewritten_sql_path",
	    "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\rewritten_queries.sql"
    );

    private static final String MUTATED_SQL_PATH = getProperty(
	    "mutated_sql_path",
	    "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\mutated_queries.sql"
    );

    private static final String SCHEMA_SUMMARY_RESOURCE = getProperty(
	    "schema_summary_resource",
	    "C:\\Users\\suhas\\Downloads\\E0261_P11\\plan_equivalence\\src\\main\\resources\\tpch_schema_summary.json"
    );

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

	public static String readSchemaSummary() {
		try
		{
			return readTextFile(SCHEMA_SUMMARY_RESOURCE);
		}
		catch (IOException ex)
		{
			ex.printStackTrace();
			return null; // If the resource is not found, return null
		}
	}

	/**
	 * List all Query IDs present in the given SQL source file, in the order they
	 * appear, de-duplicated. The Query ID is taken as the token after
	 * "-- Query ID:" up to the first whitespace or '('.
	 */
	public static java.util.List<String> listQueryIds(SqlSource source) throws IOException {
		final String path = switch (source) {
			case ORIGINAL -> ORIGINAL_SQL_PATH;
			case REWRITTEN -> REWRITTEN_SQL_PATH;
			case MUTATED -> MUTATED_SQL_PATH;
		};
		String content = readTextFile(path);
		java.util.LinkedHashSet<String> ids = new java.util.LinkedHashSet<>();
		Pattern idLine = Pattern.compile("(?m)^--\\s*Query ID:\\s*([^\n\r( ]+)" );
		Matcher m = idLine.matcher(content);
		while (m.find()) {
			String id = m.group(1).trim();
			if (!id.isEmpty()) ids.add(id);
		}
		return new java.util.ArrayList<>(ids);
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
		int afterHeaderPos = -1;
		int matches = 0;
		while (headerMatcher.find()) {
			afterHeaderPos = headerMatcher.end();
			matches++;
		}
		if (afterHeaderPos < 0) {
			// Query ID not present
			return null;
		}
		if (matches > 1) {
			// Duplicate IDs can exist in the consolidated SQL files. Prefer the last
			// occurrence (typically the most recently added/corrected block) to avoid
			// silently reading an older version.
			System.err.println("[FileIO] Warning: Query ID '" + queryId + "' appears " + matches
					+ " times; using the last occurrence.");
		}

		// 2) Find the next separator line (the one after Description), then SQL starts after it
		Pattern sepPattern = Pattern.compile("(?m)^--\\s*=+\\s*$");
		Matcher sepAfterDesc = sepPattern.matcher(fileContent);
		if (!sepAfterDesc.find(afterHeaderPos)) {
			// Malformed block: no separator after header/description
			return null;
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

	// --- Config helpers (new) ---

	/** Load config from classpath resource `config.properties` or fallback to filesystem path. */
	private static Properties getConfig() {
		if (CONFIG == null) {
			synchronized (FileIO.class) {
				if (CONFIG == null) {
					Properties props = new Properties();
					// Try to load from classpath first (resource name without absolute path)
					try (InputStream is = FileIO.class.getClassLoader().getResourceAsStream("config.properties")) {
						if (is != null) {
							props.load(is);
						} else {
							// Fallback: load from absolute filesystem path provided
							Path cfgPath = Paths.get(DEFAULT_CONFIG_RESOURCE);
							if (Files.exists(cfgPath)) {
								try (InputStream fis = Files.newInputStream(cfgPath)) {
									props.load(fis);
								}
							}
						}
					} catch (IOException ignored) { }
					CONFIG = props;
				}
			}
		}
		return CONFIG;
	}

	/** Get property by key with a default fallback. */
	public static String getProperty(String key, String defaultValue) {
		String v = getConfig().getProperty(key);
		return (v == null || v.isBlank()) ? defaultValue : v.trim();
	}

	// Typed accessors
	public static String getOriginalSqlPath() { return ORIGINAL_SQL_PATH; }
	public static String getRewrittenSqlPath() { return REWRITTEN_SQL_PATH; }
	public static String getMutatedSqlPath() { return MUTATED_SQL_PATH; }
	public static String getSchemaSummaryResource() { return SCHEMA_SUMMARY_RESOURCE; }

	// DB configuration accessors for consumers (e.g., Calcite)
	public static String getPgUrl() { return getProperty("pg_url", "jdbc:postgresql://localhost:5432/tpch"); }
	public static String getPgUser() { return getProperty("pg_user", "postgres"); }
	public static String getPgPassword() { return getProperty("pg_password", "123"); }
	public static String getPgSchema() { return getProperty("pg_schema", "public"); }

	/**
	 * Read a resource text file (one item per line) from classpath or fall back to
	 * a filesystem path specified in config by `transformation_list_path`.
	 * Returns an empty list on any error.
	 */
	public static java.util.List<String> readLinesResource(String resourceName) {
		java.util.ArrayList<String> out = new java.util.ArrayList<>();
		// try classpath resource first
		try (InputStream is = FileIO.class.getClassLoader().getResourceAsStream(resourceName)) {
			if (is != null) {
				try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(is, StandardCharsets.UTF_8))) {
					String line;
					while ((line = r.readLine()) != null) {
						line = line.trim();
						if (!line.isEmpty()) out.add(line);
					}
					return out;
				}
			}
		} catch (IOException ignored) { }

		// fallback: check config for explicit path
		String cfgPath = getProperty("transformation_list_path", "");
		if (cfgPath != null && !cfgPath.isBlank()) {
			try {
				Path p = Paths.get(cfgPath);
				if (Files.exists(p)) {
					java.util.List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
					for (String ln : lines) {
						if (ln != null) {
							String t = ln.trim();
							if (!t.isEmpty()) out.add(t);
						}
					}
					return out;
				}
			} catch (IOException ignored) { }
		}

		// last-resort: try absolute path next to default config resource
		try {
			Path sibling = Paths.get(DEFAULT_CONFIG_RESOURCE).getParent().resolve(resourceName);
			if (Files.exists(sibling)) {
				java.util.List<String> lines = Files.readAllLines(sibling, StandardCharsets.UTF_8);
				for (String ln : lines) {
					if (ln != null) {
						String t = ln.trim();
						if (!t.isEmpty()) out.add(t);
					}
				}
				return out;
			}
		} catch (Exception ignored) { }

		return out;
	}
}
