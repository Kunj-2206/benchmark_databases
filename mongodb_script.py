from pymongo import MongoClient
import json
import time

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")  # Update URI if needed
db = client["customer_db"]
collection = db["customers"]

# Load data
with open("sample_customers.json") as f:
    data = json.load(f)

# Optional: Clear existing data to avoid duplicates
collection.delete_many({})

# Measure time for insertion
start_time = time.perf_counter()
collection.insert_many(data)
end_time = time.perf_counter()

print(f"\n✅ Inserted {len(data)} records into MongoDB.")
print(f"⏱️ Time taken: {end_time - start_time:.4f} seconds") # 0.0270
