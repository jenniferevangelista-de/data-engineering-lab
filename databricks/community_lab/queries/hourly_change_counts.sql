-- Synthetic CDF-shaped input: changes(_commit_timestamp, _change_type).
-- One row per change is required. This query does not deduplicate replayed data.
-- Unknown operation names are counted explicitly instead of disappearing.
SELECT date_trunc('hour', _commit_timestamp) AS commit_hour,
  sum(CASE WHEN _change_type = 'insert' THEN 1 ELSE 0 END) AS inserts,
  sum(CASE WHEN _change_type = 'update_postimage' THEN 1 ELSE 0 END) AS updates,
  sum(CASE WHEN _change_type = 'delete' THEN 1 ELSE 0 END) AS deletes,
  sum(CASE WHEN _change_type IN ('insert', 'update_preimage', 'update_postimage', 'delete')
           THEN 0 ELSE 1 END) AS unknown_types
FROM changes
GROUP BY date_trunc('hour', _commit_timestamp)
