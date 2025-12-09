package com.ac.iisc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Utilities to obtain PostgreSQL EXPLAIN (FORMAT JSON) output and
 * strip execution-specific fields to enable stable plan comparison.
 *
 * Key features:
 * - Runs EXPLAIN with options used by this project (FORMAT JSON, BUFFERS).
 * - Cleans the JSON plan by removing execution-specific keys and lifting the root 'Plan' node.
 * - Provides an optional genericization helper to normalize implementation-specific details.
 */
public class GetQueryPlans {

    // DB settings - now read from config via FileIO
    private static final String DB_USER = FileIO.getPgUser();
    private static final String DB_PASS = FileIO.getPgPassword();
    private static final String DB_HOST = parsePgUrlComponent(FileIO.getPgUrl(), PgPart.HOST);
    private static final String DB_PORT = parsePgUrlComponent(FileIO.getPgUrl(), PgPart.PORT);
    private static final String DB_NAME = parsePgUrlComponent(FileIO.getPgUrl(), PgPart.DBNAME);

    // Helper to parse parts of a JDBC Postgres URL: jdbc:postgresql://host:port/dbname
    private enum PgPart { HOST, PORT, DBNAME }
    private static String parsePgUrlComponent(String url, PgPart part) {
        if (url == null || url.isBlank()) return "";
        // Expecting format jdbc:postgresql://host:port/dbname?params
        try {
            String u = url.trim();
            int schemeIdx = u.indexOf("jdbc:postgresql://");
            String rest = schemeIdx >= 0 ? u.substring(schemeIdx + "jdbc:postgresql://".length()) : u;
            String withoutParams = rest.split("\\?")[0];
            String[] hostPortAndDb = withoutParams.split("/");
            String hostPort = hostPortAndDb.length > 0 ? hostPortAndDb[0] : "";
            String db = hostPortAndDb.length > 1 ? hostPortAndDb[1] : "";
            String[] hp = hostPort.split(":");
            String host = hp.length > 0 ? hp[0] : "";
            String port = hp.length > 1 ? hp[1] : "5432";
            return switch (part) {
                case HOST -> host;
                case PORT -> port;
                case DBNAME -> db;
            };
        } catch (Exception e) {
            return "";
        }
    }

    // Keys to drop from the JSON plan tree (implementation/executor-specific)
    private static final Set<String> KEYS_TO_REMOVE = Set.of(
        // Execution timings and counters
        "Planning Time", "Execution Time", "Actual Rows", "Actual Loops",
        "Actual Startup Time", "Actual Total Time",
        // Buffer and I/O metrics
        "Shared Hit Blocks", "Shared Read Blocks", "Shared Dirtied Blocks", "Shared Written Blocks",
        "Local Hit Blocks", "Local Read Blocks", "Local Dirtied Blocks", "Local Written Blocks",
        "Temp Read Blocks", "Temp Written Blocks",
        "I/O Read Time", "I/O Write Time",
        // Top-level wrapper lifted separately
        "Plan",
        // Implementation-specific executor details (from sample plan)
        "Sort Method", "Sort Space Used", "Sort Space Type", "Sort Key",
        "Workers Launched", "Workers Planned", "Workers", "Worker Number",
        "Parallel Aware", "Async Capable",
        // Costing and width/row estimates
        "Startup Cost", "Total Cost", "Plan Width", "Plan Rows",
        // Scan/index implementation details
        "Scan Direction", "Index Name", "Index Cond", "Rows Removed by Index Recheck", "Heap Fetches",
        // Hash join/hash node internals
        "Hash Buckets", "Original Hash Buckets", "Hash Batches", "Original Hash Batches", "Peak Memory Usage",
        // Join implementation extras
        "Inner Unique", "Join Filter", "Rows Removed by Join Filter",
        // Planner bookkeeping / tree wrapper
        "Parent Relationship"
    );

    // Run EXPLAIN (FORMAT JSON, BUFFERS) for a given SQL query.
    // Returns the raw JSONArray text produced by PostgreSQL as org.json types.
    /**
    * Execute an EXPLAIN (FORMAT JSON, BUFFERS) against the provided SQL statement.
     *
     * Responsibilities:
     *  - Prefix the SQL with the EXPLAIN options (FORMAT JSON, ANALYZE, BUFFERS).
     *  - Use a PreparedStatement for safe execution (no manual string concatenation of params).
     *  - Return the first column of the first row as a parsed {@link JSONArray}.
     *
     * Behavior:
     *  - If the query returns no rows (unexpected for EXPLAIN), returns null.
     *  - If any SQLException propagates, the caller is expected to handle it.
     *
     * @param conn Open JDBC connection to PostgreSQL.
     * @param sql  The raw SQL query to explain (without trailing semicolon; semicolons are tolerated by server).
     * @return JSONArray containing the EXPLAIN output or null if not obtainable.
     * @throws SQLException If preparing or executing the EXPLAIN fails.
     */
    private static JSONArray explainPlan(Connection conn, String sql) throws SQLException {
        // Use a PreparedStatement to avoid issues with semicolons; EXPLAIN is server-side
        String explain = "EXPLAIN (FORMAT JSON, BUFFERS) " + sql;

        try (PreparedStatement ps = conn.prepareStatement(explain)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    // The first column contains the JSON array as text; parse via org.json.
                    // Note: EXPLAIN ... ANALYZE actually executes the query. Be mindful
                    // when running this across many or heavy queries (it will run them).
                    String json = rs.getString(1);
                    return new JSONArray(json);
                }
            }
        }
        return null;
    }

    // Recursively remove execution-only/implementation-specific keys from a plan tree while preserving structure.
    // Accepts either JSONObject, JSONArray, or primitives and returns a cleaned copy. Logical structure is retained.
    /**
     * Recursively traverse a JSON plan node (JSONObject/JSONArray/primitive) and remove
     * transient execution-specific keys defined in {@link #KEYS_TO_REMOVE}.
     *
     * Implementation notes:
     *  - Creates new container objects (JSONObject / JSONArray) to avoid mutating the input.
     *  - Preserves the original hierarchy minus removed keys.
     *  - For arrays, each element is cleaned independently and appended in original order.
     *
     * @param node Arbitrary JSON structure returned by EXPLAIN (JSONObject, JSONArray, or primitive).
     * @return A cleaned deep copy of the JSON structure.
     */
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

    /**
     * Normalize implementation-specific details into generic forms.
     *
     * What it does:
     * - Maps executor node types to generic names (e.g., "Hash Join" → "Join", "Seq Scan" → "Scan").
     * - Renames certain keys to generic equivalents (e.g., "Hash Cond" → "Join Condition").
     * - Leaves logical fields intact (e.g., "Join Type", "Strategy", "Group Key").
     * - Recurses into nested objects/arrays including the standard "Plans" array.
     *
     * Note: This function is provided for consumers that want plan genericization. It is not invoked
     * by the default cleaning pipeline to avoid altering established behavior.
     */
    private static Object removeImplementationDetails(JSONObject plan)
    {
        if (plan == null) return null;
        // Create a new object to avoid mutating input
        JSONObject out = new JSONObject();

        for (String key : plan.keySet()) {
            Object val = plan.get(key);

            // Recursively process children arrays named "Plans"
            if ("Plans".equals(key) && val instanceof JSONArray arr) {
                JSONArray newArr = new JSONArray();
                for (int i = 0; i < arr.length(); i++) {
                    Object child = arr.get(i);
                    if (child instanceof JSONObject childObj) {
                        // First apply genericization on child
                        JSONObject cleanedChild = (JSONObject) removeImplementationDetails(childObj);
                        // Then strip implementation metrics via existing cleaner
                        Object fullyCleaned = cleanPlanTree(cleanedChild);
                        newArr.put(fullyCleaned);
                    } else {
                        newArr.put(child);
                    }
                }
                out.put(key, newArr);
                continue;
            }

            // Map implementation-specific node types to generic types
            if ("Node Type".equals(key) && val instanceof String s) {
                String normalized = normalizeNodeType(s);
                out.put(key, normalized);
                continue;
            }

            // Rename implementation-specific condition keys to generic ones
            if ("Hash Cond".equals(key) && val instanceof String) {
                out.put("Join Condition", val);
                // skip original key
                continue;
            }

            // Genericize scan-related details by renaming
            if ("Relation Name".equals(key)) {
                out.put("Relation", val);
                continue;
            }

            if ("Index Name".equals(key)) {
                out.put("Index", val);
                continue;
            }

            // Recurse into nested objects
            if (val instanceof JSONObject nested) {
                out.put(key, removeImplementationDetails(nested));
                continue;
            }

            // Recurse into arrays (non-"Plans" arrays) to normalize any embedded objects
            if (val instanceof JSONArray arr2) {
                JSONArray newArr = new JSONArray();
                for (int i = 0; i < arr2.length(); i++) {
                    Object elem = arr2.get(i);
                    if (elem instanceof JSONObject o) newArr.put(removeImplementationDetails(o));
                    else newArr.put(elem);
                }
                out.put(key, newArr);
                continue;
            }

            // Default: copy as-is
            out.put(key, val);
        }

        return out;
    }

    /**
     * Convert executor-specific node type labels into generic operator names.
     * Unrecognized or already-logical operators (e.g., "Sort", "Aggregate") are returned as-is.
     */
    private static String normalizeNodeType(String s) {
        String t = s == null ? "" : s.trim();
        return switch (t) {
            case "Hash Join", "Merge Join", "Nested Loop" -> "Join";
            case "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Bitmap Index Scan" -> "Scan";
            case "Gather Merge" -> "Gather";
            case "Hash" -> "Build";
            default -> t;
        }; // genericize hash build step
        // Keep known logical operators verbatim
        // e.g., "Sort", "Aggregate", "Project", "Filter"
    }

    // Given EXPLAIN (FORMAT JSON) output (a one-element array with a root object),
    // lift the nested "Plan" object to be the root and then clean execution-only keys.
    /**
     * Extract the inner "Plan" object from the EXPLAIN JSON array and return a cleaned version.
     *
     * Expected shape from PostgreSQL: [ { "Plan": { ... }, "Planning Time": ..., "Execution Time": ... } ]
     * This method:
     *  1. Validates the array and first element type.
     *  2. Retrieves the nested Plan JSONObject.
     *  3. Invokes {@link #cleanPlanTree(Object)} to strip execution metrics and lift the plan tree.
     *
     * @param raw JSONArray returned directly by EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS).
     * @return Cleaned root plan node or null if structure is unexpected.
     */
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

    /**
     * Convenience wrapper combining EXPLAIN retrieval and cleaning in one step.
     * Opens a PostgreSQL connection, runs EXPLAIN on the provided SQL, lifts and cleans
     * the plan node, and returns a pretty-printed JSON String.
     *
     * Responsibilities:
     *  - Manage the connection lifecycle (try-with-resources ensures closure).
     *  - Handle the plan extraction and formatting.
     *
     * @param sql The SQL text to produce an execution plan for.
     * @return A human-readable (indented) JSON string of the cleaned plan, or null if unavailable.
     * @throws SQLException If connection or EXPLAIN execution fails.
     */
    public static String getCleanedQueryPlanJSONasString(String sql) throws SQLException
    {
        String url = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASS))
        {   
            // NOTE: This issues EXPLAIN (FORMAT JSON, BUFFERS). Adjust options if you
            // require estimated (planner-only) plans or execution metrics via ANALYZE.
            JSONObject cleanedPlan = extractAndClean(explainPlan(conn, sql));
            return (cleanedPlan != null) ? cleanedPlan.toString(4) : null;
            // pretty-print with indent
        }
    }

    /**
     * Retrieve a textual representation of the current database schema.
     *
     * Uses the JDBC connection parameters defined in this class to connect
     * to PostgreSQL and queries {@code information_schema.columns} to
     * enumerate tables and columns. The returned string groups columns by
     * schema and table for readability.
     *
     * Note: This method opens a short-lived JDBC connection and performs
     * a simple metadata query. Any SQL exceptions are handled internally
     * and result in an empty string being returned.
     *
     * @return Multi-line human-readable schema description (empty string if unavailable)
     */
    public static String getDatabaseSchema() {
        String url = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
        StringBuilder sb = new StringBuilder();

        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASS)) {
            String q = "SELECT table_schema, table_name, column_name, data_type "
                     + "FROM information_schema.columns "
                     + "WHERE table_catalog = ? "
                     + "ORDER BY table_schema, table_name, ordinal_position";
            try (PreparedStatement ps = conn.prepareStatement(q)) {
                ps.setString(1, DB_NAME);
                try (ResultSet rs = ps.executeQuery()) {
                    String lastSchema = "";
                    String lastTable = "";
                    while (rs.next()) {
                        String schema = rs.getString("table_schema");
                        String table = rs.getString("table_name");
                        String col = rs.getString("column_name");
                        String type = rs.getString("data_type");

                        if (!schema.equals(lastSchema)) {
                            if (sb.length() > 0) sb.append(System.lineSeparator());
                            sb.append("Schema: ").append(schema).append(System.lineSeparator());
                            lastSchema = schema;
                            lastTable = "";
                        }

                        if (!table.equals(lastTable)) {
                            sb.append("  Table: ").append(table).append(System.lineSeparator());
                            lastTable = table;
                        }

                        sb.append("    ").append(col).append(" : ").append(type).append(System.lineSeparator());
                    }
                }
            }
        } catch (SQLException e) {
            // Swallow and return empty string; callers (e.g., LLM prompt) should continue gracefully
            System.err.println("[GetQueryPlans] Unable to retrieve database schema: " + e.getMessage());
        }

        return sb.toString();
    }
}
