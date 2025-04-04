import pandas as pd
import numpy as np
import os

input_filename = "min_tube_strain_margin_summary_by_element_subcase_pairs.csv"
output_filename = "converted_min_tube_strain_margin_to_max_microstrain_at_dll.csv"

# Check if the file exists
if not os.path.exists(input_filename):
    print(f"❌ Error: {input_filename} not found.")
    exit(1)

print("✅ Reading CSV file...")
df = pd.read_csv(input_filename)

print("✅ File loaded. Performing conversion...")

# Fix header: replace "Min Margin" with "Max Microstrain", preserving rest of text
df.columns = [col.replace("Min Margin", "Max Microstrain") for col in df.columns]

# Convert all rows from column B (index 1) onward to numeric
numeric_data = pd.to_numeric(df.iloc[:, 1:].stack(), errors='coerce').unstack()

# Apply your formula
converted_data = (0.00227 / (numeric_data * 1.9 + 1)) * 1_000_000

# Replace original data in df
df.iloc[:, 1:] = converted_data

print("✅ Conversion complete. Saving clean numeric CSV...")

# Save to CSV without index; this keeps Excel happy
df.to_csv(output_filename, index=False, float_format='%.3f')

print(f"✅ Done! Cleaned file saved as: {output_filename}")
