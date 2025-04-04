import os
import pandas as pd

DATA_DIR = "data"
FILENAME = "20250404-Canary-SensorCloud.csv"  # Replace with your actual file name
FILEPATH = os.path.join(DATA_DIR, FILENAME)

# Load with the correct header row (row 18 is index 17, so skiprows=18 loads row 19 as header)
df = pd.read_csv(FILEPATH, skiprows=18)
df.columns = df.columns.str.strip()  # Clean column headers

# Display column names
print("🧩 Column names:", df.columns.tolist())

# Display types of key columns
for col in ['Time', 'ch1', 'ch3']:
    if col in df.columns:
        print(f"\n🔎 {col} dtype:", df[col].dtype)
        print(df[col].head(5))
    else:
        print(f"\n🚫 Column '{col}' not found in file.")
