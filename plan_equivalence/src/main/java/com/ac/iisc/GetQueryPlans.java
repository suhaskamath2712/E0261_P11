package com.ac.iisc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java migration of get_query_plans_cleaned.py
 * - Parses three SQL files (original/rewritten/mutated) for queries with "-- Query ID: <ID>" headers
 * - Connects once to PostgreSQL
 * - Runs EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) for each query
 * - Cleans the JSON plan by removing execution-specific keys and lifting the root 'Plan' node
 * - Writes cleaned JSON to matching output folders, skipping IDs already exported (both legacy and new names)
 */
public class GetQueryPlans {

    private static final Logger LOG = LoggerFactory.getLogger(GetQueryPlans.class);

    // DB settings - keep consistent with Python script defaults
    private static final String DB_NAME = "tpch";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "123";
    private static final String DB_HOST = "localhost";
    private static final String DB_PORT = "5432";

    // Sources: (label, sql file path, output dir)
    private static final String[][] SOURCES = new String[][]{
            {"original",
                    "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\original_queries.sql",
                    "C:\\Users\\suhas\\Downloads\\E0261_P11\\original_query_plans"},
            {"rewritten",
                    "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\rewritten_queries.sql",
                    "C:\\Users\\suhas\\Downloads\\E0261_P11\\rewritten_query_plans"},
            {"mutated",
                    "C:\\Users\\suhas\\Downloads\\E0261_P11\\sql_queries\\mutated_queries.sql",
                    "C:\\Users\\suhas\\Downloads\\E0261_P11\\mutated_query_plans"},
    };

    // Keys to drop from the JSON plan tree
    private static final Set<String> KEYS_TO_REMOVE = Set.of(
            "Planning Time", "Execution Time", "Actual Rows", "Actual Loops",
            "Actual Startup Time", "Actual Total Time",
            "Shared Hit Blocks", "Shared Read Blocks", "Shared Dirtied Blocks", "Shared Written Blocks",
            "Local Hit Blocks", "Local Read Blocks", "Local Dirtied Blocks", "Local Written Blocks",
            "Temp Read Blocks", "Temp Written Blocks",
            "I/O Read Time", "I/O Write Time",
            // remove the key 'Plan' at the top by lifting its contents
            "Plan"
    );

    private static Map<String, String> parseQueries(String sqlFilePath) throws Exception {
        String content = FileIO.readTextFile(sqlFilePath);
        // Normalize line endings to ensure regex with \n works on Windows files
        content = content.replace("\r\n", "\n");
        Pattern p = Pattern.compile("-- =+.*?\n-- Query ID: ([\\w-]+).*?\n-- =+.*?\n(.*?)(?=\n-- =+|$)", Pattern.DOTALL);
        Matcher m = p.matcher(content);
        Map<String, String> map = new LinkedHashMap<>();
        while (m.find()) {
            String id = m.group(1).trim();
            String sql = m.group(2).trim();
            if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1);
            if (!id.isEmpty() && !sql.isEmpty()) map.put(id, sql);
        }
        return map;
    }

    private static JSONArray explainPlan(Connection conn, String sql) throws SQLException {
        // Use a PreparedStatement to avoid issues with semicolons; EXPLAIN is server-side
        String explain = "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) " + sql;
        try (PreparedStatement ps = conn.prepareStatement(explain)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    // The first column contains the JSON array as text; we can parse via org.json
                    String json = rs.getString(1);
                    return new JSONArray(json);
                }
            }
        }
        return null;
    }

    private static Object cleanPlanTree(Object node) {
        if (node instanceof JSONObject obj) {
            JSONObject cleaned = new JSONObject();
            for (String key : obj.keySet()) {
                if (KEYS_TO_REMOVE.contains(key)) continue;
                cleaned.put(key, cleanPlanTree(obj.get(key)));
            }
            return cleaned;
        } else if (node instanceof JSONArray arr) {
            JSONArray out = new JSONArray();
            for (int i = 0; i < arr.length(); i++) {
                out.put(cleanPlanTree(arr.get(i)));
            }
            return out;
        } else {
            return node;
        }
    }

    private static JSONObject extractAndClean(JSONArray raw) {
        if (raw == null || raw.length() == 0) return null;
        Object first = raw.get(0);
        if (!(first instanceof JSONObject)) return null;
        JSONObject root = (JSONObject) first;
        // Lift the 'Plan' node
        JSONObject plan = root.optJSONObject("Plan");
        if (plan == null) return null;
        Object cleaned = cleanPlanTree(plan);
        return (cleaned instanceof JSONObject) ? (JSONObject) cleaned : null;
    }

    private static Set<String> detectProcessedIds(String outDir, String label) throws Exception {
        // Reads files in outDir and interprets processed IDs from both <ID>.json and <ID>_<label>.json
        Path dir = Paths.get(outDir);
        Set<String> processed = new LinkedHashSet<>();
        if (!Files.exists(dir) || !Files.isDirectory(dir)) return processed;
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().toLowerCase().endsWith(".json"))
                    .map(p -> p.getFileName().toString())
                    .forEach(name -> {
                        String base = name.substring(0, name.length() - 5);
                        String suffix = "_" + label;
                        if (base.endsWith(suffix)) processed.add(base.substring(0, base.length() - suffix.length()));
                        else processed.add(base);
                    });
        }
        return processed;
    }

    public static void main(String[] args) {
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", DB_HOST, DB_PORT, DB_NAME);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASS)) {
            LOG.info("Connected to PostgreSQL: {}", jdbcUrl);

            for (String[] src : SOURCES) {
                String label = src[0];
                String sqlPath = src[1];
                String outDir = src[2];

                LOG.info("\n{}", "-".repeat(80));
                LOG.info("Processing source: {}", label);
                LOG.info("SQL file: {}", sqlPath);
                LOG.info("Output: {}", outDir);

                FileIO.ensureDirectory(outDir);
                Map<String, String> queries = parseQueries(sqlPath);
                if (queries.isEmpty()) {
                    LOG.warn("No queries parsed; skipping: {}", label);
                    continue;
                }

                Set<String> processed = detectProcessedIds(outDir, label);
                int total = 0, saved = 0, skipped = 0;

                for (Map.Entry<String, String> e : queries.entrySet()) {
                    total++;
                    String id = e.getKey();
                    String sql = e.getValue();
                    if (processed.contains(id)) {
                        skipped++;
                        LOG.info("[{}] Skip existing: {}", label, id);
                        continue;
                    }

                    try {
                        JSONArray raw = explainPlan(conn, sql);
                        if (raw == null) {
                            LOG.warn("[{}] No plan returned; query skipped: {}", label, id);
                            continue;
                        }
                        JSONObject cleaned = extractAndClean(raw);
                        Object payload = (cleaned != null) ? cleaned : raw;
                        String outPath = outDir + java.io.File.separator + id + "_" + label + ".json";
                        String pretty;
                        if (payload instanceof JSONObject jo) {
                            pretty = jo.toString(2);
                        } else if (payload instanceof JSONArray ja) {
                            pretty = ja.toString(2);
                        } else {
                            pretty = String.valueOf(payload);
                        }
                        FileIO.writeTextFile(outPath, pretty);
                        saved++;
                        LOG.info("[{}] Saved: {}", label, outPath);
                    } catch (SQLException ex) {
                        LOG.error("[{}] ERROR for {}: {}", label, id, ex.getMessage());
                        try { conn.rollback(); } catch (SQLException ignored) { }
                    }
                }

                LOG.info("{} summary: total={}, saved={}, skipped={}", label, total, saved, skipped);
            }

        } catch (Exception e) {
            LOG.error("Fatal error running GetQueryPlans", e);
        }
    }
}
