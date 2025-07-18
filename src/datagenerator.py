import ast
import logging
import random
import re
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from functools import partial


class DataGenerator:
    RAND_INT_PATTERN = re.compile(r"^rand\(\s*(-?\d+)\s*,\s*(-?\d+)\s*\)$")
    LIST_PATTERN = re.compile(r"^\[.*\]$")

    def __init__(self, data_schema: dict) -> None:
        self.possible_types = ["timestamp", "str", "int"]
        self.generation_plan = {}
        self.is_valid = self._parse_and_compile_schema(data_schema)

    # --- Internal Helper Methods for Generation (Wrappers) ---
    def _generate_timestamp(self) -> int:
        """Wrapper for timestamp generation."""
        return int(time.time())

    def _generate_rand_int(self, from_val: int, to_val: int) -> int:
        """Wrapper for random integer generation."""
        return random.randint(from_val, to_val)  # noqa: S311

    def _generate_from_list(self, options: list):
        """Wrapper for choosing from a list."""
        return random.choice(options)  # noqa: S311

    def _generate_uuid(self) -> str:
        """Wrapper for UUID generation."""
        return str(uuid.uuid4())

    def _generate_static_value(self, value):
        """Returns a pre-configured static value."""
        return value

    def _generate_none(self) -> None:
        """Returns None."""
        return None

    def _generate_empty_string(self) -> str:
        """Returns an empty string."""
        return ""

    # --- Helper Methods for Parsing ---
    def _compile_list_source(self, key: str, val_source: str, item_type: type) -> partial | None:
        """Parses and validates a list source string for a given type."""
        try:
            parsed_list = ast.literal_eval(val_source)
            if isinstance(parsed_list, list) and all(isinstance(i, item_type) for i in parsed_list):
                return partial(self._generate_from_list, parsed_list)
            else:
                logging.error(
                    f"Validation Error for key '{key}': '{val_source}' is not a valid list of {item_type.__name__}s."
                )
                return None
        except (ValueError, SyntaxError):
            logging.error(f"Validation Error for key '{key}': Malformed list string '{val_source}'.")
            return None

    def _compile_standalone_source(self, key: str, val_source: str, val_type: type) -> partial | None:
        """Parses and validates a stand-alone source string for a given type."""
        try:
            static_value = val_type(val_source)
            return partial(self._generate_static_value, static_value)
        except ValueError:
            logging.error(f"Validation Error for key '{key}': '{val_source}' is not a valid {val_type.__name__}.")
            return None

    def _parse_and_compile_schema(self, data_schema: dict) -> bool:
        """
        Parses the raw schema, performs deep validation, and builds the
        generation_plan. Returns True on success, False on failure.
        """
        if not isinstance(data_schema, dict):
            logging.error("Validation Error: Schema must be a dictionary.")
            return False

        for key, value in data_schema.items():
            if not isinstance(key, str) or not isinstance(value, str) or ":" not in value:
                logging.error(f"Validation Error for key '{key}': Invalid format. Must be a string like 'type:source'.")
                return False

            val_type, val_source = value.split(":", 1)

            if val_type not in self.possible_types:
                logging.error(f"Validation Error for key '{key}': Type '{val_type}' is not allowed.")
                return False

            generator_func = None

            if val_type == "timestamp":
                if val_source:
                    logging.warning(f"For key '{key}', timestamp type ignores source value '{val_source}'.")
                generator_func = self._generate_timestamp

            elif val_type == "int":
                rand_int_match = self.RAND_INT_PATTERN.match(val_source)
                if not val_source:
                    generator_func = self._generate_none
                elif val_source == "rand":
                    generator_func = partial(self._generate_rand_int, 0, 10000)
                elif rand_int_match:
                    from_val, to_val = map(int, rand_int_match.groups())
                    generator_func = partial(self._generate_rand_int, min(from_val, to_val), max(from_val, to_val))
                elif self.LIST_PATTERN.match(val_source):
                    generator_func = self._compile_list_source(key, val_source, int)
                else:
                    generator_func = self._compile_standalone_source(key, val_source, int)

            elif val_type == "str":
                if not val_source:
                    generator_func = self._generate_empty_string
                elif val_source == "rand":
                    generator_func = self._generate_uuid
                elif self.RAND_INT_PATTERN.match(val_source):
                    logging.error(f"Validation Error for key '{key}': 'rand(from, to)' is only for 'int' type.")
                    return False
                elif self.LIST_PATTERN.match(val_source):
                    generator_func = self._compile_list_source(key, val_source, str)
                else:
                    generator_func = partial(self._generate_static_value, val_source)

            if generator_func is None:
                return False

            self.generation_plan[key] = generator_func

        return True

    def _generate_one_file(self, _=None) -> dict:
        """Generates a single dictionary by executing the pre-compiled plan."""
        return {key: func() for key, func in self.generation_plan.items()}

    def generate_data(self, num_data: int, num_workers: int = 1) -> list[dict]:
        """Generates a dataset, using multiprocessing if num_workers > 1"""
        if not self.is_valid:
            logging.error("Cannot generate data: The DataGenerator was initialized with an invalid schema.")
            return []

        logging.info(f"Generating {num_data} file(s) with {num_workers} worker(s)...")

        if num_workers <= 1:
            return [self._generate_one_file() for _ in range(num_data)]

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(self._generate_one_file, range(num_data)))

        return results
