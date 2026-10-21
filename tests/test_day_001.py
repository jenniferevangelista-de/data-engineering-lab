import importlib.util
from pathlib import Path
import sqlite3
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sales_example", ROOT / "python/day_001_sales.py")
sales = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sales)

class SalesTests(unittest.TestCase):
    def test_aggregation_and_trim(self):
        self.assertEqual(sales.totals_by_customer([(" Ana ", 1000), ("Ana", 250), ("Bia", 0)]), {"Ana": 1250, "Bia": 0})

    def test_empty(self):
        self.assertEqual(sales.totals_by_customer([]), {})

    def test_invalid_amounts(self):
        for amount in (-1, 1.5, True, "100"):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                sales.totals_by_customer([("Ana", amount)])

    def test_invalid_customers(self):
        for customer in ("", "  ", None):
            with self.subTest(customer=customer), self.assertRaises(ValueError):
                sales.totals_by_customer([(customer, 100)])

    def query(self, rows):
        with sqlite3.connect(":memory:") as con:
            con.execute("CREATE TABLE sales(customer TEXT NOT NULL, amount_cents INTEGER NOT NULL)")
            con.executemany("INSERT INTO sales VALUES (?, ?)", rows)
            return con.execute((ROOT / "sql/day_001_sales.sql").read_text(encoding="utf-8")).fetchall()

    def test_sql_matches_python(self):
        rows = [(" Bia ", 300), ("Ana", 1250), ("Bia", 100), ("Ana", 0)]
        self.assertEqual(self.query(rows), list(sales.totals_by_customer(rows).items()))

    def test_sql_empty(self):
        self.assertEqual(self.query([]), [])

if __name__ == "__main__":
    unittest.main()

