import argparse
import json
from pathlib import Path
from unittest.mock import patch

import pytest

import main as main_script


@pytest.fixture
def default_args(tmp_path):
    """A pytest fixture to provide a default, valid set of arguments."""
    args = argparse.Namespace(
        path_to_save_files=Path(tmp_path),
        file_count=2,
        file_name="test_output",
        prefix="count",
        data_schema='{"id": "int:rand(1,10)"}',
        data_lines=5,
        clear_path=False,
        workers=1,
    )
    return args


def test_main_calls_generator_with_correct_worker_count(default_args):
    """
    Tests that the 'workers' argument from the command line is correctly
    passed to the DataGenerator's generate_data method.
    """
    default_args.workers = 4
    with (
        patch("main.validate_save_path", return_value=default_args.path_to_save_files),
        patch("main.load_data_schema", return_value={"id": "int:rand(1,10)"}),
        patch("main.DataGenerator") as mock_generator_class,
        patch("main.save_results_to_files"),
        patch("sys.exit"),
    ):
        mock_generator_instance = mock_generator_class.return_value
        mock_generator_instance.is_valid = True
        main_script.main(default_args)

        mock_generator_instance.generate_data.assert_called()
        call_args, call_kwargs = mock_generator_instance.generate_data.call_args
        assert call_kwargs.get("num_workers") == 4


def test_main_calls_handle_clear_path_when_flag_is_set(default_args):
    """
    Tests that if the 'clear_path' argument is True, the handle_clear_path
    function from utils is called.
    """
    default_args.clear_path = True
    with (
        patch("main.validate_save_path", return_value=Path(default_args.path_to_save_files)),
        patch("main.load_data_schema", return_value={"id": "int:rand(1,10)"}),
        patch("main.DataGenerator"),
        patch("main.save_results_to_files"),
        patch("main.handle_clear_path") as mock_clear_path,
        patch("sys.exit"),
    ):
        main_script.main(default_args)
        mock_clear_path.assert_called_once()


def test_main_calls_save_results_with_correct_args(default_args):
    """
    Tests that the main function orchestrates the flow correctly and calls
    the final save_results_to_files function with the expected data.
    """
    mock_generated_data = [{"id": 1}, {"id": 2}]
    expected_path_obj = Path(default_args.path_to_save_files)
    with (
        patch("main.validate_save_path", return_value=expected_path_obj),
        patch("main.load_data_schema", return_value={"id": "int:rand(1,10)"}),
        patch("main.DataGenerator") as mock_generator_class,
        patch("main.save_results_to_files") as mock_save_func,
        patch("sys.exit"),
    ):
        mock_generator_instance = mock_generator_class.return_value
        mock_generator_instance.is_valid = True
        mock_generator_instance.generate_data.return_value = mock_generated_data

        main_script.main(default_args)

        mock_save_func.assert_called_with(
            mock_generated_data,
            file_count=default_args.file_count,
            data_lines=default_args.data_lines,
            save_path=expected_path_obj,
            file_name=default_args.file_name,
            prefix=default_args.prefix,
        )


def test_main_prints_to_console_when_file_count_is_zero(default_args, capfd):
    """
    Tests the specific logic branch for when file_count is 0, ensuring
    data is printed to the console instead of saved.
    """
    default_args.file_count = 0
    default_args.data_lines = 1
    mock_output = [{"message": "hello world"}]

    with (
        patch("main.validate_save_path", return_value=Path(default_args.path_to_save_files)),
        patch("main.load_data_schema", return_value={"message": "str:hello world"}),
        patch("main.DataGenerator") as mock_generator_class,
        patch("sys.exit"),
    ):

        mock_generator_instance = mock_generator_class.return_value
        mock_generator_instance.is_valid = True
        mock_generator_instance.generate_data.return_value = mock_output
        main_script.main(default_args)
        captured = capfd.readouterr()
        assert json.dumps(mock_output[0], indent=4) in captured.out


def test_main_exits_if_path_is_invalid(default_args):
    """
    Tests that if the path validation fails, the program exits with an error code.
    """
    with patch("src.utils.validate_save_path", return_value=None):
        with pytest.raises(SystemExit) as e:
            main_script.main(default_args)
        assert e.type is SystemExit
