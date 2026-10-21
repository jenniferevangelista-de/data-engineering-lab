-- SQLite: executar após criar sales(customer TEXT, amount_cents INTEGER).
-- Pré-condição: customer não vazio e amount_cents inteiro >= 0.
SELECT TRIM(customer) AS customer, SUM(amount_cents) AS total_cents
FROM sales
GROUP BY TRIM(customer)
ORDER BY customer;

