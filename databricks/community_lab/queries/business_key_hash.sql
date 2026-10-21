-- Input: source_keys(k1 STRING, k2 STRING).
-- Fixed field order and explicit nulls are part of this serialization contract.
SELECT sha2(to_json(named_struct(
  'k1', upper(trim(k1)),
  'k2', upper(trim(k2))
), map('ignoreNullFields', 'false')), 256) AS business_key_hash
FROM source_keys
