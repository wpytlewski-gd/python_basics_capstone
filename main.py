import argparse
import configparser
import json
import logging
import os
import sys
from pathlib import Path

from src.datagenerator import DataGenerator
from src.utils import (
    handle_clear_path,
    load_data_schema,
    save_results_to_files,
    validate_save_path,
)


def setup_arguments() -> argparse.Namespace:
    """Sets up and parses configuration and command-line arguments."""
    config = configparser.ConfigParser()
    script_dir = Path(__file__).parent
    try:
        with open(script_dir / "default.ini") as f:
            config.read_file(f)
        defaults = {
            "path_to_save_files": config.get("DEFAULT", "path_to_save_files", fallback=None),
            "file_count": config.getint("DEFAULT", "file_count", fallback=10),
            "file_name": config.get("DEFAULT", "file_name", fallback="output"),
            "prefix": config.get("DEFAULT", "prefix", fallback="count"),
            "data_schema": config.get("DEFAULT", "data_schema", fallback=None),
            "data_lines": config.getint("DEFAULT", "data_lines", fallback=1),
            "clear_path": config.getboolean("DEFAULT", "clear_path", fallback=False),
            "workers": config.getint("DEFAULT", "workers", fallback=1),
        }
        logging.info("Successfully loaded default values from default.ini")
    except (FileNotFoundError, configparser.NoSectionError):
        logging.warning("default.ini not found or is empty. Using hardcoded defaults.")
        defaults = {
            "path_to_save_files": "results",
            "file_count": 10,
            "file_name": "output",
            "prefix": "count",
            "data_schema": "{}",
            "data_lines": 1,
            "clear_path": False,
            "workers": 1,
        }

    parser = argparse.ArgumentParser(
        prog="Data Generator",
        description="Generates mock data files based on a JSON schema.",
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "path_to_save_files",
        nargs="?",
        type=str,
        help="Directory to save the generated files. Optional.",
    )
    parser.add_argument(
        "-fc",
        "--file_count",
        type=int,
        help="Number of files to generate. If 0, prints to console.",
    )
    parser.add_argument(
        "-fn",
        "--file_name",
        type=str,
        help="Base name for the output files.",
    )
    parser.add_argument(
        "-fp",
        "--prefix",
        choices=["count", "random", "uuid"],
        help="Prefix for the output file names.",
    )
    parser.add_argument(
        "-ds",
        "--data_schema",
        type=str,
        help="Data schema as a JSON string or a path to a .json file.",
    )
    parser.add_argument("-dl", "--data_lines", type=int, help="Number of data lines per file.")
    parser.add_argument(
        "-cp", "--clear_path", action="store_true", help="Clear the save path directory before generating new files."
    )
    parser.add_argument("-w", "--workers", type=int, help="Number of worker processes to use.")

    parser.set_defaults(**defaults)

    return parser.parse_args()


def main(args: argparse.Namespace):
    # --- Argument Validation ---
    if args.file_count < 0:
        logging.critical("Error: file_count cannot be negative.")
        sys.exit(1)
    if args.workers < 0:
        logging.critical("Error: Number of workers cannot be negative.")
        sys.exit(1)
    cpu_cores = os.cpu_count() or 1
    if args.workers > cpu_cores:
        logging.warning(f"Number of workers ({args.workers}) exceeds CPU cores ({cpu_cores}). Capping at {cpu_cores}.")
        args.workers = cpu_cores

    # --- Path Validation ---
    save_path = validate_save_path(args.path_to_save_files)
    if not save_path:
        logging.critical("Exiting program due to invalid save path.")
        sys.exit(1)

    # --- Schema Validation ---
    data_schema = load_data_schema(args.data_schema)
    if not data_schema:
        logging.critical("Exiting: Could not load data schema.")
        sys.exit(1)

    # --- Clear Path Handling ---
    if args.clear_path and args.file_count > 0:
        handle_clear_path(save_path, args.file_name)

    # --- Generation ---
    generator = DataGenerator(data_schema)
    if not generator.is_valid:
        logging.critical("Exiting: The provided data schema is invalid for the generator.")
        sys.exit(1)

    if args.file_count == 0:
        logging.info(f"file_count is 0. Generating {args.data_lines} line(s) for console output.")
        results = generator.generate_data(num_data=args.data_lines, num_workers=args.workers)

        print("--- Generated Data ---")
        for result in results:
            print(json.dumps(result, indent=4))
        sys.exit(0)

    # --- Generate the total number of items needed across all files ---
    total_items_to_generate = args.file_count * args.data_lines
    logging.info(
        f"Schema is valid. Proceeding to generate {total_items_to_generate} item(s) for {args.file_count} file(s)."
    )
    results = generator.generate_data(num_data=total_items_to_generate, num_workers=args.workers)

    logging.info(f"Successfully generated {total_items_to_generate} item(s). Saving to {args.file_count} file(s).")
    # --- Saving ---
    save_results_to_files(
        results,
        file_count=args.file_count,
        data_lines=args.data_lines,
        save_path=save_path,
        file_name=args.file_name,
        prefix=args.prefix,
    )
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    arguments = setup_arguments()
    main(arguments)
