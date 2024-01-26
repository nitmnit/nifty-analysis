import pandas as pd
import matplotlib.pyplot as plt

# from sklearn.model_selection import train_test_split
# Load data from CSV file into a DataFrame
file_name = "announcements.csv"
df = pd.read_csv(file_name)

# Convert 'sort_date' column to datetime
df["sort_date"] = pd.to_datetime(df["sort_date"])

# Group by date and count number of entries
# grouped_data = df.groupby(df["sort_date"].dt.date).size().reset_index(name="count")

df_shuffled = df.sample(frac=1, random_state=42)  # random_state for reproducibility

# Calculate the number of rows for each set (e.g., 70% for training and 30% for testing)
train_size = int(0.5 * len(df_shuffled))

# Divide the shuffled DataFrame into two sets
df_train = df_shuffled.iloc[:train_size]
df_train = df_train.sort_values(by="sort_date")


grouped_data = df_train.groupby(df["desc"]).size().reset_index(name="count")

# Plot data
plt.figure(figsize=(10, 6))
plt.bar(grouped_data["desc"], grouped_data["count"], color="skyblue")
plt.title("Number of Entries by Date")
plt.xlabel("desc")
plt.ylabel("Number of Entries")
plt.xticks(rotation=45)
# plt.tight_layout()
plt.show()
