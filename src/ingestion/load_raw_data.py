import pandas as pd
import numpy as np
import os
#print("Libraries imported successfully")
# Define raw data path
raw_path = "D:/project_sj/data/raw"

# File paths

plant1_generation = os.path.join(raw_path, "Plant_1_Generation_Data.csv")
plant2_generation = os.path.join(raw_path, "Plant_2_Generation_Data.csv")

plant1_weather = os.path.join(raw_path, "Plant_1_Weather_Sensor_Data.csv")
plant2_weather = os.path.join(raw_path, "Plant_2_Weather_Sensor_Data.csv")

# Loading CSV files

df_plant1_generation = pd.read_csv(plant1_generation)
df_plant2_generation = pd.read_csv(plant2_generation)

df_plant1_weather = pd.read_csv(plant1_weather)
df_plant2_weather = pd.read_csv(plant2_weather)

# Display dataset information

print("Plant 1 Generation:")
print(df_plant1_generation.shape)

print("\nPlant 2 Generation:")
print(df_plant2_generation.shape)

print("\nPlant 1 Weather:")
print(df_plant1_weather.shape)

print("\nPlant 2 Weather:")
print(df_plant2_weather.shape)

#Display first record in the datasets
print("\nPlant 1 Generation Columns:")
print(df_plant1_generation.columns)

print("\nPlant 1 Generation Sample:")
print(df_plant1_generation.head())


print("\nPlant 1 Weather Columns:")
print(df_plant1_weather.columns)

print("\nPlant 1 Weather Sample:")
print(df_plant1_weather.head())

# Combine generation datasets

generation_data = pd.concat(
    [df_plant1_generation, df_plant2_generation],
    ignore_index=True
)

print("\nCombined Generation Data:")
print(generation_data.shape)
print(generation_data.head())

# Save combined generation data

output_path = "../../data/interim/generation_data.csv"

generation_data.to_csv(
    output_path,
    index=False
)

print("\nGeneration data saved successfully!")

# Combine weather datasets

weather_data = pd.concat(
    [df_plant1_weather, df_plant2_weather],
    ignore_index=True
)

print("\nCombined Weather Data:")
print(weather_data.shape)
print(weather_data.head())


# Save combined weather data

weather_output_path = "../../data/interim/weather_data.csv"

weather_data.to_csv(
    weather_output_path,
    index=False
)

print("\nWeather data saved successfully!")