import pandas as pd
import json
import sys


def convert_json_to_csv(json_file, csv_file):
    """Converts a JSON file to a CSV file, handling potential errors gracefully.

    Args:
        json_file (str): Path to the input JSON file.
        csv_file (str): Path to the output CSV file.
    """

    try:
        # Read the JSON data, handling potential encoding issues
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Create a pandas DataFrame from the JSON data
        df = pd.DataFrame(data)
        df = df.sort_values("sort_date")

        # Write the DataFrame to a CSV file, suppressing index
        df.to_csv(csv_file, index=False)

        print(f"JSON data successfully converted to CSV and saved to {csv_file}")

    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_file}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_file}")
    except pd.errors.ParserError:
        print(f"Error: Failed to parse JSON data into a DataFrame.")
    except Exception as e:  # Catch any other unexpected errors
        print(f"Error: An unexpected error occurred: {e}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python json_to_csv.py <input_json_file> <output_csv_file>")
        sys.exit(1)

    json_file = sys.argv[1]
    csv_file = json_file.replace(".json", ".csv")

    try:
        convert_json_to_csv(json_file, csv_file)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
