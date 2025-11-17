package com.ac.iisc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

/**
 * Diagnostic tool to identify transformation rules that crash or hang.
 *
 * It iterates over all supported transformation names (from LLM.getSupportedTransformations()),
 * applies each one individually to the optimized RelNode of each query found in the consolidated
 * SQL files, and records the result. Crashes are captured with the rule name and exception details.
 *
 * Output: plan_equivalence/target/transformation_probe_report.csv
 * Columns: rule,source,queryId,result,error
 *   - result: OK | UNSUPPORTED | PLAN_ERROR | CRASH
 *   - error: short message for failures
 *
 * Note: Requires the PostgreSQL connection in Calcite.getFrameworkConfig() to be valid.
 */
public class TransformationProbe {

    private record Case(String source, String id, String sql) {}

    public static void main(String[] args) throws Exception {
        List<Case> cases = new ArrayList<>();
        // Collect queries from ORIGINAL and REWRITTEN sets
        cases.addAll(loadCases(FileIO.SqlSource.ORIGINAL));
        cases.addAll(loadCases(FileIO.SqlSource.REWRITTEN));

        List<String> rules = LLM.getSupportedTransformations();

        Path out = Path.of("c:/Users/suhas/Downloads/E0261_P11/plan_equivalence/target/transformation_probe_report.csv");
        if (out.getParent() != null && !Files.exists(out.getParent())) {
            Files.createDirectories(out.getParent());
        }
        List<String> lines = new ArrayList<>();
        lines.add("rule,source,queryId,result,error");

        FrameworkConfig cfg = Calcite.getFrameworkConfig();

        for (String rule : rules) {
            for (Case c : cases) {
                String result = "OK";
                String error = "";
                Planner planner = Frameworks.getPlanner(cfg);
                try {
                    RelNode rel = Calcite.getOptimizedRelNode(planner, c.sql);
                    // apply one rule at a time
                    try {
                        RelNode outRel = Calcite.applyTransformations(rel, java.util.List.of(rule));
                        if (outRel == null) {
                            result = "PLAN_ERROR";
                            error = "null result";
                        }
                    } catch (RuntimeException re) {
                        // applyTransformations identifies the rule in the exception message
                        result = "CRASH";
                        error = shortMsg(re);
                    } catch (Throwable t) {
                        result = "CRASH";
                        error = shortMsg(t);
                    }
                } catch (Throwable t) {
                    result = "PLAN_ERROR";
                    error = shortMsg(t);
                } finally {
                    try { planner.close(); } catch (Throwable ignore) {}
                }

                lines.add(csv(rule) + "," + csv(c.source) + "," + csv(c.id) + "," + csv(result) + "," + csv(error));
            }
        }

        Files.write(out, lines, StandardCharsets.UTF_8);
        System.out.println("Wrote report: " + out.toString());
    }

    private static List<Case> loadCases(FileIO.SqlSource src) throws IOException {
        List<Case> out = new ArrayList<>();
        for (String id : FileIO.listQueryIds(src)) {
            String sql;
            try {
                sql = FileIO.readSqlQuery(src, id);
            } catch (IOException e) {
                // skip if not loadable
                continue;
            }
            out.add(new Case(src.name(), id, sql));
        }
        return out;
    }

    private static String csv(String s) {
        if (s == null) return "";
        String v = s.replace("\r", " ").replace("\n", " ");
        if (v.contains(",") || v.contains("\"")) {
            v = '"' + v.replace("\"", "\"\"") + '"';
        }
        return v;
    }

    private static String shortMsg(Throwable t) {
        String cls = t.getClass().getSimpleName();
        String msg = t.getMessage();
        if (msg == null) msg = "";
        if (msg.length() > 180) msg = msg.substring(0, 180) + "...";
        return (cls + ": " + msg).trim();
    }
}
