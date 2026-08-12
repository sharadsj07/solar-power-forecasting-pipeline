import pandas as pd
from sklearn.preprocessing import LabelEncoder

INPUT_FILE = "data/processed/solar_cleaned_data.csv"
OUTPUT_FILE = "data/processed/solar_featured_data.csv"

print("="* 50)
print("Loading cleaned Dataset...")
print("="*50)

df = pd.read_csv(INPUT_FILE)

print("Dataset loaded Successfully")

df["DATE_TIME"] =  pd.to_datetime(df["DATE_TIME"])

# ============================
# Create Time-Based Features
# ============================

print("\nCreating Time-Based Features...")

# Extract Hour
df["HOUR"] = df["DATE_TIME"].dt.hour

# Extract Day
df["DAY"] = df["DATE_TIME"].dt.day

# Extract Month
df["MONTH"] = df["DATE_TIME"].dt.month

# Extract Day of Week
df["DAY_OF_WEEK"] = df["DATE_TIME"].dt.day_name()

# Create Weekend Flag
df["IS_WEEKEND"] = df["DAY_OF_WEEK"].isin(["Saturday", "Sunday"]).astype(int)

print("Time-based features created successfully.")

print("\nNew Features Preview:")
print(df[["DATE_TIME", "HOUR", "DAY", "MONTH", "DAY_OF_WEEK", "IS_WEEKEND"]].head())

# ============================
# Encode SOURCE_KEY
# ============================

print("\nEncoding SOURCE_KEY...")

label_encoder = LabelEncoder()

df["SOURCE_KEY_ENCODED"] = label_encoder.fit_transform(df["SOURCE_KEY"])

print("SOURCE_KEY encoded successfully.")

print("\nSOURCE_KEY Preview:")
print(df[["SOURCE_KEY", "SOURCE_KEY_ENCODED"]].head(10))
# ============================
# Save Featured Dataset
# ============================

print("\nSaving featured dataset...")

df.to_csv(OUTPUT_FILE, index=False)

print(f"Featured dataset saved successfully to:\n{OUTPUT_FILE}")

print("\nDay 4 - Feature Engineering Completed Successfully!")