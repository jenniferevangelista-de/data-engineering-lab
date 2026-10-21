-- Synthetic map input: history(operationMetrics MAP<STRING, STRING>).
-- Preserve the original map. An absent metric remains NULL, never zero.
SELECT operationMetrics,
  cast(operationMetrics['executionTimeMs'] AS BIGINT) AS execution_time_ms,
  cast(operationMetrics['scanTimeMs'] AS BIGINT) AS scan_time_ms,
  cast(operationMetrics['numSourceRows'] AS BIGINT) AS source_rows
FROM history
