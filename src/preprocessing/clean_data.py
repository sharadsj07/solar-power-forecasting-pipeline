import pandas as pd

# ============================
# File Paths
# ============================
INPUT_FILE = "data/processed/solar_master_data.csv"
OUTPUT_FILE = "data/processed/solar_cleaned_data.csv"

# ============================
# Load Dataset
# ============================
print("=" * 50)
print("Loading Master Dataset...")
print("=" * 50)

df = pd.read_csv(INPUT_FILE)

print("Dataset loaded successfully.")

# ============================
# Convert DATE_TIME to datetime
# ============================
df["DATE_TIME"] = pd.to_datetime(df["DATE_TIME"])

# ============================
# Basic Dataset Information
# ============================
print("\nDataset Information")
print("-" * 50)

print("\nFirst 5 Rows:")
print(df.head())

print("\nDataset Shape:")
print(df.shape)

print("\nColumn Names:")
print(df.columns.tolist())

print("\nData Types:")
print(df.dtypes)

# ============================
# Missing Values
# ============================
print("\nMissing Values:")
print(df.isnull().sum())

# ============================
# Duplicate Records
# ============================
print("\nChecking Duplicate Rows...")

duplicate_count = df.duplicated().sum()

print(f"Duplicate Rows Found: {duplicate_count}")

if duplicate_count > 0:
    df = df.drop_duplicates()
    print("Duplicate rows removed successfully.")
else:
    print("No duplicate rows found.")

print("\nDataset Shape After Removing Duplicates:")
print(df.shape)

# ============================
# Statistical Summary
# ============================
print("\nStatistical Summary:")
print(df.describe())

# ============================
# Outlier Analysis
# ============================
print("\nOutlier Analysis (IQR Method)")

numeric_columns = [
    "DC_POWER",
    "AC_POWER",
    "DAILY_YIELD",
    "TOTAL_YIELD",
    "AMBIENT_TEMPERATURE",
    "MODULE_TEMPERATURE",
    "IRRADIATION"
]

for column in numeric_columns:
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    outliers = df[(df[column] < lower) | (df[column] > upper)]

    print(f"{column}: {len(outliers)} outliers")

# ============================
# Save Cleaned Dataset
# ============================
df.to_csv(OUTPUT_FILE, index=False)

print("\nCleaned dataset saved successfully.")
print(f"Location: {OUTPUT_FILE}")

print("\nDay 3 - Data Cleaning Completed Successfully!")