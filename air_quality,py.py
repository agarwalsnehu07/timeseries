import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import numpy as np

# STEP 1: Load Dataset
df = pd.read_csv("air_quality.csv", sep=';', decimal=',')
df = df.rename(columns={"Date": "date", "Time": "time", "CO(GT)": "value"})
df["timestamp"] = pd.to_datetime(df["date"] + " " + df["time"], errors='coerce')
df = df[["timestamp", "value"]].dropna()

# STEP 2: Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["time_series_db"]

# STEP 3: Create Time Series Collection (only once)
try:
    db.create_collection(
        "sensor_data",
        timeseries={"timeField": "timestamp", "metaField": "metadata", "granularity": "minutes"}
    )
except Exception as e:
    print("Collection may already exist:", e)

# STEP 4: Insert Data
documents = [{"timestamp": row["timestamp"], "value": row["value"], "metadata": {"source": "air_quality"}} for _, row in df.iterrows()]
db.sensor_data.insert_many(documents)

# STEP 5: Plot-A
values = list(db.sensor_data.find())
timestamps = [doc["timestamp"] for doc in values]
readings = [doc["value"] for doc in values]

plt.plot(timestamps, readings)
plt.title("Original Time Series (Plot A)")
plt.xlabel("Time")
plt.ylabel("CO Level")
plt.show()

# STEP 6: Statistics
print("Mean:", np.mean(readings))
print("Median:", np.median(readings))
print("Std Dev:", np.std(readings))

# STEP 7: Weekly Resample and Plot-B
df.set_index("timestamp", inplace=True)
weekly_avg = df["value"].resample("W").mean()

plt.plot(weekly_avg.index, weekly_avg.values)
plt.title("Weekly Average (Plot B)")
plt.xlabel("Week")
plt.ylabel("Avg CO Level")
plt.show()

# STEP 8: Store Weekly Averages in New Collection
weekly_docs = [{"week": str(date), "avg_value": val} for date, val in weekly_avg.items()]
db.weekly_avg.insert_many(weekly_docs)

# STEP 9: Final Combined Plot
plt.plot(timestamps, readings, label="Original", alpha=0.5)
plt.plot(weekly_avg.index, weekly_avg.values, label="Weekly Avg", color='red')
plt.legend()
plt.title("Combined Time Series")
plt.xlabel("Time")
plt.ylabel("CO Level")
plt.show()