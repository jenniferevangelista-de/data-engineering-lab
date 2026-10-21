-- Match normalization must be identical to the actual MERGE ON clause.
WITH normalized AS (
  SELECT upper(trim(col4)) AS k4, upper(col5) AS k5, upper(col6) AS k6
  FROM source_table
  WHERE col3 = 'A'
)
SELECT k4, k5, k6, count(*) AS n
FROM normalized
GROUP BY k4, k5, k6
HAVING count(*) > 1
