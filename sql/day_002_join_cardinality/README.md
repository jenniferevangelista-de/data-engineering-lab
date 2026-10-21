# Day 002: join-cardinality audit (SQLite)

A row-count comparison alone cannot prove a join preserves facts. This synthetic
example deliberately has **three source rows and three joined rows**, while
losing an orphan order and doubling another order's amount.

- Source amount: **17,500 cents**.
- Joined amount: **25,000 cents**.
- Dimension keys duplicated: `c1`; unmatched order: `o3`.

## Run

Python 3.12+, no third-party packages or credentials:

```bash
python -m unittest discover -s sql/day_002_join_cardinality -p 'test_*.py' -v
python sql/day_002_join_cardinality/join_guard.py
```

Eight `unittest` cases exercise row-count cancellation, duplicates, orphans,
blocking bad joins, empty input, refunds and valid one-profile-per-key output.
The guard is intentionally conservative: if the right-hand side is Type 2
history, derive the correct time-qualified slice **before** asserting uniqueness.

**Scope:** SQLite with synthetic data, not a Spark/Databricks benchmark, not a
production SCD2 join policy. `require_one_to_one()` is a teaching example, not
an authorization boundary; source snapshots must remain stable during checks.

## Article

[Your SQL Join Passed the Row-Count Check. Revenue Was Still Wrong.](https://medium.com/@jenniferevangelista.code/your-sql-join-passed-the-row-count-check-revenue-was-still-wrong-84d6ff448745) on Medium.

Official references: [Databricks JOIN](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-join), [PostgreSQL joined tables](https://www.postgresql.org/docs/current/queries-table-expressions.html).
