import json
import os
from typing import List, Dict, Any


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    if os.path.isdir(path):
        raise IsADirectoryError(f"The provided path '{path}' is a directory, not a file.")
    try:
        # Write JSON content to the file
        with open(path, "w", encoding="utf-8") as f:
            json.dump(json_content, f, indent=4)  # Serialize data into JSON format with indentation

        print(f"Data successfully saved to {path}")

    except FileNotFoundError as e:
        print(f"Error: File not found or directory does not exist: {e}")
    except PermissionError as e:
        print(f"Error: Permission denied while trying to save to file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
