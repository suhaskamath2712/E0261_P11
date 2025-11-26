## Plan Cleaning Notes (GetQueryPlans)

The plan cleaning logic removes implementation/executor-specific fields from PostgreSQL `EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS)` output to focus on logical structure.

- Keys removed (examples): timings, buffer counters, worker details, costing/width estimates, sort/hash internals, scan/index details, and planner bookkeeping. See `KEYS_TO_REMOVE` in `plan_equivalence/src/main/java/com/ac/iisc/GetQueryPlans.java` for the complete list.
- Optional genericization helper: `removeImplementationDetails(JSONObject)` maps executor-specific node types to generic names (e.g., `Hash Join`→`Join`, `Seq Scan`→`Scan`) and renames certain fields (e.g., `Hash Cond`→`Join Condition`, `Relation Name`→`Relation`, `Index Name`→`Index`). This helper is defined but not invoked by default to preserve existing behavior.

To enable genericization in a custom flow, call `removeImplementationDetails(plan)` on the lifted `Plan` object before passing it to downstream processing.

Plan Equivalence Helper (E0261_P11)
===================================

Overview
--------
This repository contains tools to parse, normalize, and compare SQL query plans using Apache Calcite. The aim is to detect semantic equivalence between an original SQL query and its rewritten or mutated variants while being tolerant to harmless syntactic or planner differences (CASTs, commutativity, join child order, etc.).

Highlights
----------
- Parse SQL to Calcite `RelNode` plans and optionally run planner rules.
- Multiple comparison layers (structural, normalized, canonical, tree-canonical) to explain mismatches.
- Several low-risk normalizations implemented:
  - Recursive CAST stripping.
  - Commutative/symmetric expression canonicalization (AND/OR/EQUALS).
  - Inequality orientation normalization (flip > to < with operand reordering).
  - UNION flattening and child ordering where safe.
  - Sort abstraction (ignore sort keys in canonicalization but preserve fetch/offset).

Project layout (relevant)
-------------------------
- `plan_equivalence/src/main/java/com/ac/iisc/`
  - `Calcite.java`         — Core parsing / optimization / canonicalization utilities.
  - `FileIO.java`          — SQL and plan file read/write helpers.
  - `GetQueryPlans.java`   — Helpers to capture/store plan JSONs from a schema environment.
  - `LLM.java` / `LLMResponse.java` — Optional LLM integration helpers for auxiliary tasks.
  - `RelTreeNode.java`     — Produces canonical tree digests for debugging.
  - `Main.java`            — Optional entrypoint for CLI/service wiring.
  - `Test.java`            — A small batch driver to compare queries and print diagnostics.
- `sql_queries/`, `original_query_plans/`, `mutated_query_plans/` — SQL and saved plan artifacts (used by `FileIO`/`Test`).

Quick start (developer)
-----------------------
Prereqs:
- JDK 17+ (project tested with Java 21).
- Optional: PostgreSQL JDBC driver if you want to validate `JdbcTableScan` naming against a live schema.

Run the simple batch test driver (PowerShell example, adjust classpath/build as needed):

```powershell
# From repo root
c:; cd 'c:\Users\suhas\Downloads\E0261_P11';
# If classes are compiled under plan_equivalence/target/classes use that in classpath
& 'C:\Program Files\Java\jdk-21\bin\java.exe' -cp 'plan_equivalence\target\classes;.' com.ac.iisc.Test
```

If you don't use a build tool, compile sources with `javac` and run `com.ac.iisc.Test`.

Design & behavior notes
-----------------------
- Conservative semantics: the tool does not assume schema constraints (foreign keys, not-null) unless explicitly provided. For example, it will not treat `LEFT JOIN` and `INNER JOIN` as equivalent just because join conditions look similar — outer-join preservation semantics are preserved.
- The canonicalization focuses on eliminating planner- or syntax-noise, not on proving deep semantic equivalence that requires constraints or data statistics.

Per-class documentation (in this single README)
-----------------------------------------------

Calcite.java
------------
Purpose:
- Central utility that interfaces with Apache Calcite: parsing SQL, building frameworks, configuring planners, applying transformations, and producing multi-layer digests for comparisons.

Key responsibilities:
- Preprocess SQL (strip semicolons, normalize `!=` to `<>`).
- Parse SQL to `RelNode` and (optionally) run a Hep planner with configurable rules.
- Provide layered comparison APIs (`compareQueriesDebug` etc.) that print structural, normalized and canonical digests.
- Implement normalization helpers: `canonicalizeRex`, `stripAllCasts`, `canonicalDigest`, and `summarizeNode`.

Important public methods & contract:
- `RelNode getRelNode(String sql)` — parse a SQL string into a `RelNode`.
- `RelNode getOptimizedRelNode(String sql, List<String> transformations)` — run optional transformations and return the optimized `RelNode`.
- `boolean compareQueries(String a, String b, List<String> transformations)` — boolean equivalence check.
- `boolean compareQueriesDebug(String a, String b, List<String> transformations, String tag)` — same, but prints diagnostic digests.

Notes:
- Keeps outer join types in canonicalization to avoid false positives.
- Consider enabling a "lenient" mode later that uses schema metadata to collapse safe differences.

FileIO.java
-----------
Purpose:
- Helper utilities to read SQL and plan files used by `Test.java` and other drivers.

Key responsibilities:
- Read original and rewritten SQL by query ID from repository folders.
- Read or write plan JSONs if needed for integration tests.

Usage:
- `FileIO.readOriginalSqlQuery(String id)` and `FileIO.readRewrittenSqlQuery(String id)` are the primary helpers used by the test driver.

GetQueryPlans.java
------------------
Purpose:
- Utility code to fetch query plans from a Calcite/JDBC environment and persist them as JSON under `original_query_plans/` or `mutated_query_plans/`.

Notes:
- Useful for re-capturing plans if the schema or planner rules change.
- May require a JDBC connection to a running database or a prebuilt schema.

LLM.java & LLMResponse.java
---------------------------
Purpose:
- Optional helper classes used when invoking an external Large Language Model for auxiliary tasks (e.g., generating transformation hints). Not required for core plan comparison.

Notes:
- Keep credentials out of source; these classes are thin wrappers and include safe fallbacks when the LLM is not present.

RelTreeNode.java
----------------
Purpose:
- Builds a human-friendly, canonical tree representation of a `RelNode` plan to highlight subtree structure differences.

Behavior:
- Walks the `RelNode` tree and produces a bracketed representation that stabilizes input refs and hides planner-generated column indices. Useful for debugging when canonical digests differ.

Main.java
---------
Purpose:
- An optional entrypoint for wiring the tool into a CLI or service. The `Test.java` class is currently the main developer driver.

Notes:
- If you want a CLI, add an argument parser (e.g., `picocli`) and wire `Calcite.compareQueriesDebug` for ad-hoc queries.

Test.java
---------
Purpose:
- Lightweight batch driver to compare pairs of SQL queries (original vs rewritten/mutated). Prints human-readable debug output for each pair.

How it works:
- The static `queryIDList` (in `Test.java`) controls which query IDs to process.
- For each ID it loads the original and rewritten SQL via `FileIO` and calls `Calcite.compareQueriesDebug` to produce layered diagnostics.

Customization:
- Edit `queryIDList` to run the subset you want.
- Optionally supply transformations (e.g., `List.of("UnionMergeRule")`) to test the effect of planner rule applications.

Caveats and future work
-----------------------
- Parser conformance: some SQL variants (non-standard constructs) may fail Calcite parsing. The code performs minor SQL fixes (like `!=` → `<>`) but larger conformance differences require manual edits or Calcite parser configuration.
- Sarg/interval normalization: date-range and interval equivalence can be tricky (inclusive/exclusive endpoints). Consider adding a normalization layer for commonly observed patterns.
- Join-type equivalence requires schema-level constraints (FKs, not-null) to be safe. If you have schema metadata, we can add a provable rewrite pass to collapse some outer vs inner join differences.

Contributing
------------
- Add SQL pairs under `sql_queries/` and update `Test.java`'s `queryIDList` to include new IDs.
- Add unit tests or small integration drivers for newly introduced normalization rules.
- When adding normalization, add debug prints to `compareQueriesDebug` to validate effects across the corpus.

License
-------
- No license provided. Add a LICENSE file if you plan to publish or share this repository publicly.

Contact / Next steps
--------------------
If you want, I can:
- Insert short one-line pointers at the top of each Java file linking back to this README.
- Add a small `BUILD.md` with exact `javac`/`mvn` commands for your environment (PowerShell / Windows examples).
- Add an optional `lenient` comparison mode that prints warnings when it would relax certain semantics (outer join, pre-join filters, etc.).

Recent documentation and comment updates
---------------------------------------
- Several Java files had end-of-line comments moved so that comments appear on standalone preceding lines (improves readability and avoids trailing comment noise). This includes `Calcite.java`, `FileIO.java`, `GetQueryPlans.java`, `LLM.java`, `LLMResponse.java`, and `Test.java`.
- Clarifying inline comments were added in `Calcite.java` around canonicalization logic (CAST stripping, commutative normalization, inner-join flattening). These are explanatory only and do not change behavior.
- A condensed LaTeX code reference has been added at `documentation/code_reference.tex` summarising these changes.

Deprecation note
----------------
- A deprecation warning is present for the use of `RelDecorrelator.decorrelateQuery(RelNode)` in `Calcite.java`. It remains to preserve behavior; we can migrate it to the non-deprecated API in a follow-up.

