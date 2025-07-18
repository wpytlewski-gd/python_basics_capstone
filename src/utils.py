import json
import logging
import random
from pathlib import Path
from uuid import uuid4


def validate_save_path(path_str: str) -> Path | None:
    """Validates that the provided path exists and is a directory."""
    if not path_str:
        logging.error("Error: The path to save files has not been specified.")
        return None

    logging.info(f"Validating save path: '{path_str}'")
    try:
        save_path = Path(path_str).resolve()

        if not save_path.exists():
            logging.error(f"Error: The specified path does not exist: '{save_path}'")
            return None

        if not save_path.is_dir():
            logging.error(f"Error: The specified path exists but is a file, not a directory: '{save_path}'")
            return None

        logging.info(f"Save path is valid: '{save_path}'")
        return save_path

    except Exception as e:
        logging.error(f"An unexpected error occurred while validating the path '{path_str}': {e}")
        return None


def read_from_path(path: Path) -> dict | None:
    """Internal function to read JSON from a file path with specific error handling."""
    try:
        with path.open("r") as file:
            return json.load(file)
    except FileNotFoundError:
        logging.debug(f"Path attempt failed: File not found at '{path}'.")
        return None
    except IsADirectoryError:
        logging.debug(f"Path attempt failed: Path '{path}' is a directory, not a file.")
        return None
    except json.JSONDecodeError:
        logging.warning(f"File '{path}' was found but does not contain valid JSON.")
        return None
    except OSError as e:
        logging.error(f"OS error reading file '{path}': {e}")
        return None


def read_from_string(schema_string: str) -> dict | None:
    """Internal function to parse a JSON string."""
    try:
        return json.loads(schema_string)
    except json.JSONDecodeError:
        logging.debug("String attempt failed: Input is not a valid JSON string.")
        return None


def load_data_schema(input_str: str | None) -> dict | None:
    """Loads a data schema from a file path or a raw string."""
    logging.info("Attempting to load data schema from input...")
    if not input_str:
        logging.error("Error: The data schema has not been specified.")
        return None

    loaded_from_path = read_from_path(Path(input_str))
    if loaded_from_path is not None:
        logging.info(f"Successfully loaded schema from file path: '{input_str}'")
        return loaded_from_path

    loaded_from_string = read_from_string(input_str)
    if loaded_from_string is not None:
        logging.info("Successfully loaded schema from raw string.")
        return loaded_from_string

    logging.error("Failed to load data schema. Input is not a valid path or a valid JSON string.")
    return None


def handle_clear_path(save_path: Path, file_name: str) -> None:
    """Deletes old files in the save directory that match the base file_name."""
    logging.info(f"Clear path is on. Deleting files matching '{file_name}*.json' in '{save_path}'...")
    deleted_count = 0
    for file_to_delete in save_path.glob(f"{file_name}*.json"):
        try:
            file_to_delete.unlink()
            logging.debug(f"Deleted file: {file_to_delete}")
            deleted_count += 1
        except OSError as e:
            logging.error(f"Could not delete file {file_to_delete}: {e}")
    logging.info(f"Deleted {deleted_count} file(s).")


def _generate_filenames(base_name: str, prefix: str, count: int):
    """A generator that yields filenames based on the prefix rule."""
    if count <= 1:
        yield f"{base_name}.jsonl"
        return

    for i in range(1, count + 1):
        if prefix == "count":
            yield f"{base_name}_{i}.jsonl"
        elif prefix == "random":
            yield f"{base_name}_{random.randint(10000, 99999)}.jsonl"  # noqa: S311
        elif prefix == "uuid":
            yield f"{base_name}_{uuid4().hex[:8]}.jsonl"


def save_results_to_files(
    results: list[dict], file_count: int, data_lines: int, save_path: Path, file_name: str, prefix: str
) -> None:
    """Saves the generated data into a specific number of files."""
    if not results:
        logging.warning("No data was generated, nothing to save.")
        return

    filename_gen = _generate_filenames(file_name, prefix, file_count)

    saved_count = 0
    # Group the flat list of results into chunks, one for each file
    for i in range(0, len(results), data_lines):
        file_end = i + data_lines
        chunk = results[i:file_end]

        try:
            file_path = save_path / next(filename_gen)
        except StopIteration:
            logging.warning("Generated more data chunks than the specified file_count. Some data will not be saved.")
            break

        try:
            with open(file_path, "w") as f:
                for item in chunk:
                    f.write(json.dumps(item) + "\n")

                logging.debug(f"Saved {len(chunk)} lines to {file_path}")
                saved_count += 1
        except OSError as e:
            logging.error(f"Failed to write to file {file_path}: {e}")

    logging.info(f"Successfully saved data to {saved_count} file(s).")
