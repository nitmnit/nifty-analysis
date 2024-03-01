import os
import pandas as pd

# Function to fix datetime column
def fix_datetime_column(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    if df.empty:
        return
    # Convert the 'date' column to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Update rows where the second's value is equal to 1
    df.loc[df['date'].dt.second == 1, 'date'] -= pd.Timedelta(seconds=1)

    # Save the updated DataFrame back to the CSV file
    df.to_csv(file_path, index=False)

# Function to traverse through subfolders and fix datetime column in CSV files
def traverse_and_fix(root_dir):
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            file_path = os.path.join(subdir, file)
            if file_path.endswith('.csv'):
                print(f"Fixing datetime column in {file_path}")
                fix_datetime_column(file_path)


# Function to process CSV files
def process_csv_file(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)
    if df.empty:
        return
    # Convert the 'date' column to datetime and localize it to the desired timezone
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)  # Remove timezone information

    # Save the updated DataFrame back to the CSV file
    df.to_csv(file_path, index=False)

# Function to traverse through subfolders and process CSV files
def traverse_and_process(root_dir):
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            file_path = os.path.join(subdir, file)
            if file_path.endswith('.csv'):
                print(f"Processing {file_path}")
                process_csv_file(file_path)


# Main function
if __name__ == "__main__":
    root_directory = "data/NIFTY 50/"
    #traverse_and_fix(root_directory)
    #fix_datetime_column("data/NIFTY 50/NSE/minute/2015-06-24.csv")
    traverse_and_process(root_directory)
