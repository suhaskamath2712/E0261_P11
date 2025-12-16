# Calcite integration

This document focuses on **how Apache Calcite is used** inside this repository.

Source of truth:

- `plan_equivalence/src/main/java/com/ac/iisc/Calcite.java`
- `plan_equivalence/src/main/java/com/ac/iisc/CalciteUtil.java`

## 🧩 What Calcite is doing here

The tool uses Calcite to:

1. Parse and validate SQL against a PostgreSQL-backed schema.
2. Convert SQL → logical plans (`RelNode`).
3. Apply a small normalization program (HepPlanner).
4. Compare plans using multiple digest stages (structural → normalized → canonical).
5. Optionally use PostgreSQL `EXPLAIN (FORMAT JSON, BUFFERS)` as a best-effort fallback.

## Key entry points

### Framework / planning

- `Calcite.getFrameworkConfig()`
  - Thin wrapper over `CalciteUtil.getFrameworkConfig()`.
  - Builds a Calcite `FrameworkConfig` using a PostgreSQL JDBC schema (`JdbcSchema`).

- `Calcite.getOptimizedRelNode(Planner planner, String sql)`
  - Parses → validates → converts to `RelNode`.
  - Applies a phased HepPlanner pipeline.
  - Runs a couple of SQL compatibility shims (e.g., `GROUP BY` alias rewrite via `CalciteUtil.rewriteGroupByAliases`).

### Equivalence

- `Calcite.compareQueries(String sql1, String sql2, List<String> transformations)`
  - Main user-facing comparison method.
  - Optionally applies a list of Calcite rule names (allow-listed) to the *left* plan.
  - Runs subquery conversion + best-effort decorrelation.
  - Compares using the ladder described in the root `README.md`.

## Digest ladder (what is compared)

The implementation uses multiple comparison stages. In order:

1. **Structural digest** — strict plan shape string.
2. **Normalized digest** — input refs normalized (`$0`, `$12` → `$x`).
3. **Canonical digest** — robust digest tolerant to safe reorderings and syntactic noise.
4. **AND ordering safety-net** — a last attempt that normalizes AND-term ordering inside the canonical digest.
5. **Optional** PostgreSQL EXPLAIN JSON equality (cleaned) — best-effort fallback.

Debug-only utilities like `RelTreeNode` exist (`Calcite.buildRelTree`, `Calcite.relTreeCanonicalDigest`), but they are *not* part of the default equivalence ladder.

## What “canonicalization” means (high level)

Canonicalization is designed to reduce *false negatives* while avoiding obvious *false positives*.

Examples of what it normalizes:

- **INNER join commutativity**
  - Treats INNER-join inputs as an unordered set (via flatten + sort).

- **Predicate normalization**
  - Splits conjunctive predicates (AND chains) into terms.
  - Deduplicates and sorts terms.
  - Normalizes commutative/symmetric operators where safe.

- **CAST stripping**
  - Strips CAST layers inside expressions when they are considered representational noise.

- **SEARCH / SARG normalization**
  - Normalizes Calcite SEARCH expressions and folds single-interval SARGs to a stable `RANGE(...)` representation.

- **Range folding**
  - Detects simple inequality ranges like `$c >= 10 AND $c < 20` and folds them into a single canonical range representation.

- **Date arithmetic folding**
  - Folds specific safe patterns of `DATE ± INTERVAL` into concrete `DATE` literals when applicable.

- **Top-N semantics**
  - When `FETCH`/`OFFSET` is present, the canonical digest includes sort collation because it impacts Top‑N semantics.

## EXPLAIN fallback (PostgreSQL)

When enabled/available, Calcite can render a `RelNode` back into SQL and request a PostgreSQL plan:

- `Calcite.convertRelNodetoJSONQueryPlan(RelNode rel)`
- `GetQueryPlans.getCleanedQueryPlanJSONasString(String sql)`

Important details:

- Uses `EXPLAIN (FORMAT JSON, BUFFERS)` (not ANALYZE).
- Cleaning removes run-specific/execution-only keys but preserves semantic keys to reduce false positives.
- Some plans cannot be converted to SQL (notably correlated plans); the fallback is best-effort.

## Configuration

Settings are read from `plan_equivalence/src/main/resources/config.properties` via `FileIO`.

Common keys:

- `pg_url`, `pg_user`, `pg_password`, `pg_schema`
- `original_sql_path`, `rewritten_sql_path`, `mutated_sql_path`
- `schema_summary_resource`

## Limits and gotchas

- Equivalence here is heuristic: matching digests is **not** a formal proof.
- Outer joins are intentionally treated more conservatively than inner joins.
- Planner output can still change across Calcite versions, which is why canonicalization exists.

