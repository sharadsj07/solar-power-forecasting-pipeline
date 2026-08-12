import os
import joblib
import pandas as pd

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ============================================================
# RANDOM FOREST SOLAR POWER FORECASTING
# ============================================================

print("=" * 70)
print("SOLAR POWER FORECASTING - RANDOM FOREST")
print("=" * 70)


# ============================================================
# 1. LOAD DATA
# ============================================================

DATA_PATH = "data/processed/solar_featured_data_final.csv"

df = pd.read_csv(DATA_PATH)

print(f"Total records loaded: {len(df)}")


# ============================================================
# 2. CONVERT DATE_TIME
# ============================================================

df["DATE_TIME"] = pd.to_datetime(df["DATE_TIME"])

# Sort chronologically
df = df.sort_values("DATE_TIME").reset_index(drop=True)


# ============================================================
# 3. SELECT FEATURES
# ============================================================

features = [
    "DC_POWER",
    "AC_POWER",
    "DAILY_YIELD",
    "TOTAL_YIELD",
    "AMBIENT_TEMPERATURE",
    "MODULE_TEMPERATURE",
    "IRRADIATION",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "DAY_OF_WEEK"
]

target = "POWER_GENERATED"


# ============================================================
# 4. REMOVE MISSING VALUES
# ============================================================

df = df.dropna(subset=features + [target])

print(f"Records after cleaning: {len(df)}")


X = df[features]
y = df[target]


# ============================================================
# 5. TIME-BASED TRAIN / TEST SPLIT
# ============================================================

split_index = int(len(df) * 0.80)

X_train = X.iloc[:split_index]
X_test = X.iloc[split_index:]

y_train = y.iloc[:split_index]
y_test = y.iloc[split_index:]

print()
print("Training records:", len(X_train))
print("Testing records :", len(X_test))


# ============================================================
# 6. RANDOM FOREST MODEL
# ============================================================

print()
print("Training Random Forest model...")

model = RandomForestRegressor(
    n_estimators=100,
    max_depth=15,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

print("Model training completed.")


# ============================================================
# 7. PREDICTION
# ============================================================

y_pred = model.predict(X_test)


# ============================================================
# 8. MODEL EVALUATION
# ============================================================

mae = mean_absolute_error(y_test, y_pred)

rmse = mean_squared_error(
    y_test,
    y_pred
) ** 0.5

r2 = r2_score(y_test, y_pred)


print()
print("=" * 70)
print("MODEL PERFORMANCE")
print("=" * 70)

print(f"MAE  : {mae:.4f}")
print(f"RMSE : {rmse:.4f}")
print(f"R²   : {r2:.4f}")


# ============================================================
# 9. SAVE PREDICTIONS
# ============================================================

results = df.iloc[split_index:][
    ["DATE_TIME", "PLANT_ID", "SOURCE_KEY"]
].copy()

results["ACTUAL_POWER"] = y_test.values
results["PREDICTED_POWER"] = y_pred

os.makedirs("data/processed", exist_ok=True)

prediction_path = "data/processed/random_forest_predictions.csv"

results.to_csv(
    prediction_path,
    index=False
)

print()
print(f"Predictions saved to: {prediction_path}")


# ============================================================
# 10. SAVE MODEL
# ============================================================

os.makedirs("models", exist_ok=True)

model_path = "models/random_forest_solar_power.pkl"

joblib.dump(model, model_path)

print(f"Model saved to: {model_path}")


# ============================================================
# 11. FEATURE IMPORTANCE
# ============================================================

importance = pd.DataFrame({
    "FEATURE": features,
    "IMPORTANCE": model.feature_importances_
})

importance = importance.sort_values(
    "IMPORTANCE",
    ascending=False
)

print()
print("=" * 70)
print("FEATURE IMPORTANCE")
print("=" * 70)

print(importance.to_string(index=False))

print()
print("=" * 70)
print("RANDOM FOREST FORECASTING COMPLETED")
print("=" * 70)#random forest model
