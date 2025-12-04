<div align="center">

# E0261_P11 — Plan Equivalence Helper

Tools to parse, normalize, and compare SQL query plans using Apache Calcite, and to retrieve cleaned PostgreSQL EXPLAIN plans.

</div>

## Table of Contents
- Overview
- Features
- Architecture
- Quick Start
- Usage
- Configuration
- Documentation
- Contributing
- License
- Deprecation Note

## Overview
This project detects semantic equivalence between an original SQL query and its rewritten or mutated variants. It is tolerant to harmless syntactic or planner differences (CASTs, commutativity, inner‑join child order, etc.).

## Features
- Parse SQL into Calcite `RelNode` plans and optionally run planner rules.
- Layered comparison strategy: structural → normalized → canonical → tree‑canonical.
- Normalizations include:
  - Recursive CAST stripping.
  - Commutative/symmetric expression canonicalization (AND/OR/EQUALS).
  - Inequality orientation normalization (prefer `<`/`<=`).
  - UNION flattening with sorted children where safe.
  - Sort abstraction (ignore sort keys; preserve `fetch`/`offset`).
- Cleaned PostgreSQL EXPLAIN (FORMAT JSON, BUFFERS) output with executor‑specific fields removed.
- Optional schema introspection dump (schemas, tables, columns, types).

## Architecture
- `plan_equivalence/src/main/java/com/ac/iisc/`
  - `Calcite.java` — Core parsing, optimization, canonicalization, and comparison utilities.
  - `FileIO.java` — SQL and plan file read/write helpers.
  - `GetQueryPlans.java` — Capture EXPLAIN JSON and clean it for comparison.
  - `LLM.java` / `LLMResponse.java` — Optional LLM integration helpers.
  - `RelTreeNode.java` — Canonical tree digests for order‑insensitive comparisons.
  - `Test.java` — Batch driver to compare queries and print diagnostics.
- `sql_queries/`, `original_query_plans/`, `mutated_query_plans/` — SQL queries.

## Usage
- Compare queries programmatically: use `Calcite.compareQueries(String a, String b, List<String> transformations)`.
- Print layered diagnostics: use `Calcite.compareQueriesDebug(...)`.
- Obtain cleaned EXPLAIN JSON: `GetQueryPlans.getCleanedQueryPlanJSONasString(sql)`.
- Print database schema:
```java
String schema = GetQueryPlans.getDatabaseSchema();
System.out.println(schema);
```

## Configuration
- Update PostgreSQL connection constants in `GetQueryPlans.java` (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`).
- Update Calcite framework connection constants in `Calcite.java` (`PG_URL`, `PG_USER`, `PG_PASSWORD`, `PG_SCHEMA`).
- Planner reuse: planners are recreated per query for reliability.

## Documentation
- Conceptual overview and normalization details: `documentation/Calcite_Doc.md`.
- API and behavior reference: `documentation/code_reference.md`.
- Project notes and per‑class documentation: `documentation/documentation.md`.

### Plan Cleaning Notes (GetQueryPlans)
- Removes executor‑specific fields from EXPLAIN JSON (timings, buffers, worker details, costing/width estimates, sort/hash internals, scan/index minutiae, planner bookkeeping). See `KEYS_TO_REMOVE` in `GetQueryPlans.java`.
- Optional genericization helper `removeImplementationDetails(JSONObject)`: maps executor‑specific node types (`Hash Join`→`Join`, `Seq Scan`→`Scan`) and renames fields (`Hash Cond`→`Join Condition`, `Relation Name`→`Relation`, `Index Name`→`Index`). Not enabled by default.

