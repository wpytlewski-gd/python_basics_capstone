from unittest.mock import patch

import pytest

from src.datagenerator import DataGenerator

params_data_type = [
    ("user_id", "str:rand", str, lambda x: len(x) > 0),
    ("status", "str:['active', 'inactive']", str, lambda x: x in ["active", "inactive"]),
    ("country", "str:Poland", str, lambda x: x == "Poland"),
    ("empty_str", "str:", str, lambda x: x == ""),
    ("score", "int:rand(1, 100)", int, lambda x: 1 <= x <= 100),
    ("negative_score", "int:rand(-50, -10)", int, lambda x: -50 <= x <= -10),
    ("fixed_int", "int:42", int, lambda x: x == 42),
    ("options", "int:[1, 2, 3]", int, lambda x: x in [1, 2, 3]),
    ("empty_int", "int:", type(None), lambda x: x is None),
    ("event_time", "timestamp:", int, lambda x: x > 1609459200),
]


@pytest.mark.parametrize("key, schema_value, expected_type, validation_func", params_data_type)
def test_data_type_generation(key, schema_value, expected_type, validation_func):
    """
    Tests that the generator produces the correct data type and that the value
    is valid for various schema source definitions.
    """

    schema = {key: schema_value}
    generator = DataGenerator(schema)
    assert generator.is_valid, "Generator should be valid for this test case"
    result = generator._generate_one_file()

    assert key in result, "The generated data should contain the specified key"
    value = result[key]
    assert isinstance(value, expected_type), f"Expected type {expected_type}, but got {type(value)}"
    assert validation_func(value), f"The value '{value}' did not pass its validation function."


params_schemas = [
    ({"id": "str:rand", "age": "int:rand(18, 65)"}, True, ["id", "age"]),
    (
        {
            "ts": "timestamp:",
            "user_type": "str:['guest', 'member']",
            "score": "int:rand(0, 1000)",
            "nickname": "str:CoolGuy123",
        },
        True,
        ["ts", "user_type", "score", "nickname"],
    ),
    ({"id": "string:rand"}, False, []),
    ({"age": "int:rand(10-20)"}, False, []),
    ({"name": "str:rand(1,10)"}, False, []),
]


@pytest.mark.parametrize("schema, expected_validity, expected_keys", params_schemas)
def test_different_schemas(schema, expected_validity, expected_keys):
    """
    Tests the DataGenerator's ability to correctly parse and validate
    entire schemas, both valid and invalid.
    """
    generator = DataGenerator(schema)

    assert generator.is_valid == expected_validity

    if expected_validity:
        result = generator._generate_one_file()
        assert isinstance(result, dict)
        assert all(key in result for key in expected_keys)


def test_timestamp_generation_uses_time_time():
    """
    Verifies that when 'timestamp:' is used, the generator internally
    calls the 'time.time()' function.
    """
    schema = {"event_time": "timestamp:"}
    generator = DataGenerator(schema)
    mock_time = 1234567890

    with patch("time.time", return_value=mock_time):
        result = generator._generate_one_file()
        assert result["event_time"] == mock_time


def test_str_rand_uses_uuid4():
    """
    Verifies that when 'str:rand' is used, the generator internally
    calls the 'uuid.uuid4()' function.
    """
    schema = {"user_id": "str:rand"}
    generator = DataGenerator(schema)
    mock_uuid = "a-b-c-d"

    with patch("uuid.uuid4", return_value=mock_uuid):
        result = generator._generate_one_file()
        assert result["user_id"] == str(mock_uuid)
