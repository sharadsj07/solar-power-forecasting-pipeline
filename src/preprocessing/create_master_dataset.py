import pandas as pd


# --------------------------------
# File paths
# --------------------------------

generation_path = "data/interim/generation_data.csv"
weather_path = "data/interim/weather_data.csv"


# --------------------------------
# Load datasets
# --------------------------------

generation_data = pd.read_csv(generation_path)
weather_data = pd.read_csv(weather_path)


print("Generation Data:")
print(generation_data.shape)

print("\nWeather Data:")
print(weather_data.shape)


# --------------------------------
# Convert DATE_TIME format
# --------------------------------

generation_data["DATE_TIME"] = pd.to_datetime(
    generation_data["DATE_TIME"],
    format="mixed"
)

weather_data["DATE_TIME"] = pd.to_datetime(
    weather_data["DATE_TIME"],
    format="mixed"
)


print("\nDatetime conversion completed")


# --------------------------------
# Convert generation time to hourly
# --------------------------------

generation_data["DATE_TIME"] = generation_data["DATE_TIME"].dt.floor("h")


# --------------------------------
# Aggregate weather data hourly
# --------------------------------

weather_data["DATE_TIME"] = weather_data["DATE_TIME"].dt.floor("h")


weather_data = weather_data.groupby(
    ["DATE_TIME", "PLANT_ID"],
    as_index=False
).agg(
    {
        "AMBIENT_TEMPERATURE": "mean",
        "MODULE_TEMPERATURE": "mean",
        "IRRADIATION": "mean"
    }
)


print("\nWeather after hourly aggregation:")
print(weather_data.shape)


# --------------------------------
# Merge generation and weather
# --------------------------------

solar_master_data = pd.merge(
    generation_data,
    weather_data,
    on=["DATE_TIME", "PLANT_ID"],
    how="inner"
)


print("\nMaster Dataset:")
print(solar_master_data.shape)

print("\nFirst 5 rows:")
print(solar_master_data.head())


# --------------------------------
# Save processed dataset
# --------------------------------

output_path = "data/processed/solar_master_data.csv"


solar_master_data.to_csv(
    output_path,
    index=False
)


print("\nsolar_master_data.csv created successfully!")