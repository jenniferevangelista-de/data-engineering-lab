"""A conservative audit for a supposed many-to-one revenue join.

Demonstration only: SQLite in memory, integer cents, synthetic data.
The caller owns the contract that customer_id should be unique in the dimension.
"""
from dataclasses import dataclass
import sqlite3


@dataclass(frozen=True)
class JoinAudit:
    source_rows: int
    joined_rows: int
    source_amount_cents: int
    joined_amount_cents: int
    duplicate_dimension_keys: tuple[str, ...]
    unmatched_order_ids: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return (
            not self.duplicate_dimension_keys
            and not self.unmatched_order_ids
            and self.source_rows == self.joined_rows
            and self.source_amount_cents == self.joined_amount_cents
        )


def make_connection() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE orders (
            order_id TEXT NOT NULL PRIMARY KEY,
            customer_id TEXT NOT NULL,
            amount_cents INTEGER NOT NULL
        );
        CREATE TABLE customer_profiles (
            customer_id TEXT NOT NULL,
            profile_version INTEGER NOT NULL,
            tier TEXT NOT NULL
        );
    """)
    return db


def seed_broken_example(db: sqlite3.Connection) -> None:
    """Equal row counts hide both an orphan and a fanout."""
    db.executemany("INSERT INTO orders VALUES (?, ?, ?)", [
        ("o1", "c1", 10000), ("o2", "c2", 5000), ("o3", "c3", 2500),
    ])
    db.executemany("INSERT INTO customer_profiles VALUES (?, ?, ?)", [
        ("c1", 1, "silver"), ("c1", 2, "gold"), ("c2", 1, "basic"),
    ])
    db.commit()


def inspect_join(db: sqlite3.Connection) -> JoinAudit:
    source_rows, source_amount = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(amount_cents), 0) FROM orders"
    ).fetchone()
    joined_rows, joined_amount = db.execute("""
        SELECT COUNT(*), COALESCE(SUM(o.amount_cents), 0)
        FROM orders AS o
        INNER JOIN customer_profiles AS p ON p.customer_id = o.customer_id
    """).fetchone()
    duplicate_keys = tuple(row[0] for row in db.execute("""
        SELECT customer_id FROM customer_profiles
        GROUP BY customer_id HAVING COUNT(*) > 1 ORDER BY customer_id
    """))
    unmatched_ids = tuple(row[0] for row in db.execute("""
        SELECT o.order_id FROM orders AS o
        WHERE NOT EXISTS (
            SELECT 1 FROM customer_profiles AS p
            WHERE p.customer_id = o.customer_id
        ) ORDER BY o.order_id
    """))
    return JoinAudit(source_rows, joined_rows, source_amount, joined_amount,
                     duplicate_keys, unmatched_ids)


def require_one_to_one(db: sqlite3.Connection) -> JoinAudit:
    audit = inspect_join(db)
    if not audit.valid:
        raise ValueError(
            "Unsafe join: "
            f"duplicate keys={audit.duplicate_dimension_keys}; "
            f"unmatched orders={audit.unmatched_order_ids}; "
            f"rows={audit.source_rows}->{audit.joined_rows}; "
            f"cents={audit.source_amount_cents}->{audit.joined_amount_cents}"
        )
    return audit


if __name__ == "__main__":
    connection = make_connection()
    try:
        seed_broken_example(connection)
        print(inspect_join(connection))
    finally:
        connection.close()
