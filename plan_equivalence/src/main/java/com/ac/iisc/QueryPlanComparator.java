package com.ac.iisc;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
/**
 * QueryPlanComparator
 * --------------------
 * Lightweight harness for experimenting with Apache Calcite on a PostgreSQL
 * catalog. It parses SQL into logical plans (RelNode), shows their structure,
 * and demonstrates how to obtain and walk plans.
 *
 * For full semantic equivalence checks and plan normalization, use the
 * helpers in {@link Calcite}:
 * - {@code Calcite.getFrameworkConfig()} to wire Calcite to PostgreSQL
 * - {@code Calcite.getOptimizedRelNode(...)} to parse/validate/optimize
 * - {@code Calcite.compareQueries(...)} for digest-based structural equality
 *
 * Notes and limitations (for the comparison helpers in Calcite):
 * - Only inner joins are treated as order-insensitive; outer/semi/anti joins
 *   remain order-sensitive.
 * - ORDER BY/LIMIT are part of semantics and are not normalized away.
 * - Canonical digest normalizes inner-join child ordering; projection order is
 *   preserved.
 */
public class QueryPlanComparator {

    
    /**
     * Small demo entrypoint: builds two example SQL statements and prints the
     * comparison result. Adjust the SQL and the transformation list as needed
     * for local experiments. Ensure the referenced tables exist in PostgreSQL.
     */
    public static void main(String[] args) {
        String sqlA = """
                        select n_name, c_acctbal from nation LEFT OUTER JOIN customer
                     ON n_nationkey = c_nationkey and c_nationkey > 3 and n_nationkey < 20 and
                     c_nationkey <> 10 LIMIT 200;""";
        String sqlB = """
                        SELECT
                            N.n_name,
                            C.c_acctbal
                        FROM
                            nation AS N
                        LEFT JOIN
                            customer AS C ON N.n_nationkey = C.c_nationkey
                        WHERE
                            C.c_nationkey > 3
                            AND N.n_nationkey < 20
                            AND C.c_nationkey <> 10
                        LIMIT 200;""";

        FrameworkConfig config = Calcite.getFrameworkConfig();
        Planner planner = Frameworks.getPlanner(config);
        List<String> transformations = List.of("filterjoinrule");

        try
        {
            RelTreeNode relTreeNodeA = Calcite.buildRelTree(Calcite.getRelNode(planner, sqlA));
            System.out.println("RelTreeNode A:\n" + relTreeNodeA.toString());
            RelNode relA = Calcite.getRelNode(planner, sqlA);
            System.out.println("RelNode A: " + relA.explain());
            Calcite.iteratePrintRelNode(relA);
        } catch (Exception ex) {
        }

        boolean result1 = Calcite.compareQueries(sqlA, sqlB, transformations);
        System.out.println("\nComparison Result (A vs B): " + result1);
        System.out.println("------------------------------------------------------------------\n");
    }
}
