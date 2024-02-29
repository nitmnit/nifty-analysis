import os
import shutil

data_folder = "data"  # Replace with the actual path to your data folder
files_copied = []

count = 0
for filename in os.listdir(data_folder):
    if filename.endswith(".csv"):
        parts = filename.split("-NSE-")
        symbol = parts[0]
        date_part = f"{parts[-1].split('.')[0]}"  # Extract the date without extension

        subfolder_path = os.path.join(new_folder, symbol)
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)  # Create the subfolder if it doesn't exist

        new_filename = f"{date_part}.csv"
        old_filepath = os.path.join(data_folder, filename)
        new_filepath = os.path.join(subfolder_path, new_filename)

        shutil.copy2(old_filepath, new_filepath)  # Copy the file
        files_copied.append(new_filepath)  # Keep track of copied files
        count += 1
print(f"Count: {count}")
exit()
# Verification
if all(os.path.exists(filepath) for filepath in files_copied):
    print("All files copied successfully!")

    # Delete original files
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            os.remove(filepath)
    print("Original files deleted successfully.")
else:
    print("Error: Some files could not be copied.")

