import copy
import importlib.util
import json
from pathlib import Path
import unittest

module_path = Path(__file__).resolve().parents[1] / "databricks/community_lab/zerobus_json_encoding.py"
spec = importlib.util.spec_from_file_location("variant_encoding_example", module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
encode = module.encode_variant_fields


class VariantEncodingTests(unittest.TestCase):
    def test_native_json_values_roundtrip(self):
        for value in [{"x": "ação", "items": [1, None]}, [True, 2], 5, 2.5, True, False, None]:
            with self.subTest(value=value):
                self.assertEqual(json.loads(encode({"ValueJson": value})["ValueJson"]), value)

    def test_strings_are_not_inferred_as_serialized_json(self):
        for value in ["5", "true", "null", '{"x":1}', "", "ação"]:
            with self.subTest(value=value):
                self.assertEqual(json.loads(encode({"ValueJson": value})["ValueJson"]), value)

    def test_only_designated_fields_are_encoded(self):
        other = {"nested": 2}
        result = encode({"a": 3, "other": other, "V1": "5", "V2": None}, {"V1", "V2"})
        self.assertIs(result["other"], other)
        self.assertEqual(result["a"], 3)
        self.assertEqual(result["V1"], '\"5\"')
        self.assertEqual(result["V2"], "null")

    def test_rejects_nonfinite_values_including_nested(self):
        for value in [float("nan"), float("inf"), float("-inf"), {"x": float("nan")}, [float("inf")]]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                encode({"ValueJson": value})

    def test_rejects_unsupported_json_values(self):
        for value in [{1, 2}, object(), 2j]:
            with self.subTest(value=type(value).__name__), self.assertRaises(TypeError):
                encode({"ValueJson": value})

    def test_does_not_mutate_input(self):
        record = {"ValueJson": {"x": [1, None]}, "id": 7}
        expected = copy.deepcopy(record)
        result = encode(record)
        self.assertEqual(record, expected)
        self.assertIsNot(result, record)

    def test_outer_transport_preserves_inner_value(self):
        payload = json.loads(json.dumps([encode({"id": 7, "ValueJson": "5"})]))
        self.assertEqual(payload[0]["id"], 7)
        self.assertIsInstance(payload[0]["ValueJson"], str)
        self.assertEqual(json.loads(payload[0]["ValueJson"]), "5")

    def test_missing_variant_field_is_not_added(self):
        self.assertEqual(encode({"id": 7}), {"id": 7})
        self.assertEqual(encode({}), {})


if __name__ == "__main__":
    unittest.main()
