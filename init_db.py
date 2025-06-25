from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import os

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["proximity_dispatch"]

# Step 1: Drop existing collections
for name in ["customers", "technicians", "assignments"]:
    db[name].drop()
    print(f"🗑️ Dropped collection: {name}")

# Step 2: Load CSVs
customer_df = pd.read_csv("customer.csv")
technician_df = pd.read_csv("Technician.csv")

# ✅ Rename for consistency in DB
customer_df.rename(columns={"customer_id": "id"}, inplace=True)
technician_df.rename(columns={"technician_id": "id"}, inplace=True)

# ✅ Clean: Remove rows with missing or duplicate ids
customer_df.dropna(subset=["id"], inplace=True)
technician_df.dropna(subset=["id"], inplace=True)

customer_df.drop_duplicates(subset=["id"], inplace=True)
technician_df.drop_duplicates(subset=["id"], inplace=True)

# Ensure IDs are integers
customer_df["id"] = customer_df["id"].astype(int)
technician_df["id"] = technician_df["id"].astype(int)

# Step 3: Insert clean data into MongoDB
db.customers.insert_many(customer_df.to_dict(orient="records"))
db.technicians.insert_many(technician_df.to_dict(orient="records"))

# Step 4: Create indexes
db.customers.create_index("id", unique=True)
db.technicians.create_index("id", unique=True)
db.assignments.create_index("tech_id")
db.assignments.create_index("customer_id")

print("✅ MongoDB initialized with cleaned customer and technician data.")
