import json
import re

import pytest

from src.utils import (
    _generate_filenames,
    handle_clear_path,
    load_data_schema,
    save_results_to_files,
)


@pytest.fixture
def schema_file(tmp_path):
    """A pytest fixture that creates a temporary schema.json file and returns its path."""
    schema_content = {"user_id": "str:rand", "is_active": "int:[0, 1]"}
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema_content))
    return str(schema_path)


def test_load_data_schema_from_file(schema_file):
    """
    Tests that the schema is correctly loaded from a valid JSON file path.
    This directly addresses the 'temporary files' testing requirement.
    """
    expected_schema = {"user_id": "str:rand", "is_active": "int:[0, 1]"}
    loaded_schema = load_data_schema(schema_file)
    assert loaded_schema == expected_schema


def test_load_data_schema_from_string():
    """Tests that the schema is correctly loaded from a raw JSON string."""
    schema_string = '{"product_id": "str:rand", "price": "int:rand(10, 1000)"}'
    expected_schema = {"product_id": "str:rand", "price": "int:rand(10, 1000)"}
    loaded_schema = load_data_schema(schema_string)
    assert loaded_schema == expected_schema


def test_load_data_schema_invalid_input():
    """Tests that invalid input (not a path, not a JSON string) returns None."""
    invalid_input = "this is just a plain string, not json"
    result = load_data_schema(invalid_input)
    assert result is None


def test_handle_clear_path(tmp_path):
    """
    Tests that the clear_path action correctly deletes only the target files.
    """
    file_name_to_clear = "data_output"
    (tmp_path / f"{file_name_to_clear}_1.json").touch()
    (tmp_path / f"{file_name_to_clear}_2.json").touch()
    (tmp_path / "other_file.txt").touch()
    handle_clear_path(tmp_path, file_name_to_clear)
    assert not (tmp_path / f"{file_name_to_clear}_1.json").exists()
    assert not (tmp_path / f"{file_name_to_clear}_2.json").exists()
    assert (tmp_path / "other_file.txt").exists()


def test_save_single_file_to_disk(tmp_path):
    """
    Tests that a single file is created with the correct content.
    This covers the 'check saving file to the disk' requirement.
    """
    results_data = [{"id": 1, "data": "A"}, {"id": 2, "data": "B"}]
    file_name = "single_test"
    file_path = tmp_path / f"{file_name}.jsonl"

    save_results_to_files(
        results=results_data,
        file_count=1,
        data_lines=2,
        save_path=tmp_path,
        file_name=file_name,
        prefix="count",
    )
    assert file_path.exists()
    lines = file_path.read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1, "data": "A"}
    assert json.loads(lines[1]) == {"id": 2, "data": "B"}


def test_save_multiple_files_to_disk(tmp_path):
    """
    Tests that the correct number of files are created.
    This covers the 'check a number of created files' requirement.
    """
    results_data = [{"id": i} for i in range(10)]
    file_name = "multi_test"
    file_count = 5

    save_results_to_files(
        results=results_data,
        file_count=file_count,
        data_lines=2,
        save_path=tmp_path,
        file_name=file_name,
        prefix="count",
    )
    created_files = list(tmp_path.glob(f"{file_name}_*.jsonl"))
    assert len(created_files) == file_count


params = [
    ("count", 1, "test_name.jsonl"),
    ("count", 3, "test_name_\\d+.jsonl"),
    ("random", 5, "test_name_\\d+.jsonl"),
    ("uuid", 2, "test_name_[a-f0-9]{8}.jsonl"),
]


@pytest.mark.parametrize("prefix, count, expected_pattern", params)
def test_generate_filenames(prefix, count, expected_pattern):
    """
    Tests the internal filename generator for different prefixes.
    """
    base_name = "test_name"
    filenames = list(_generate_filenames(base_name, prefix, count))
    assert len(filenames) == count
    for fname in filenames:
        assert re.match(expected_pattern, fname)
