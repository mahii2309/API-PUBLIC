from fastapi import FastAPI, HTTPException, Body
from pymongo import MongoClient
from dotenv import load_dotenv
import openrouteservice
import os
import logging
import requests
from fastapi import Body

# Load environment variables
load_dotenv()

app = FastAPI()
logging.basicConfig(level=logging.INFO)

# MongoDB Setup
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
db = client["proximity_dispatch"]
customers = db["customers"]
technicians = db["technicians"]
assignments = db["assignments"]

# ORS Setup
ORS_API_KEY = os.getenv("ORS_API_KEY")

# Groq Setup
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = "mixtral-8x7b-32768"

# ORS Distance
def compute_distance_ors(coord1, coord2):
    try:
        ors_client = openrouteservice.Client(key=ORS_API_KEY)
        route = ors_client.directions(
            coordinates=[coord1, coord2],
            profile='driving-car',
            format='geojson'
        )
        return route['features'][0]['properties']['segments'][0]['distance'] / 1000, "ORS"
    except Exception as e:
        logging.error(f"\u274c ORS failed to compute distance: {e}")
        return None, "ORS Failed"

# LLM via Groq API
def llm_recommend_best_technician(customer_id, top3):
    try:
        prompt = (
            f"Customer ID: {customer_id}\n"
            f"Here are the top 3 technicians based on proximity (from ORS):\n"
        )
        for t in top3:
            prompt += f"- Technician {t['name']} (ID: {t['technician_id']}): {t['distance_km']} km via {t['method']}\n"
        prompt += "Choose the best technician to assign (based on shortest distance). Only reply with the technician ID."

        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }

        body = {
            "model": GROQ_MODEL,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }

        response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=body)
        result = response.json()
        tech_id_str = result["choices"][0]["message"]["content"].strip()
        tech_id = int(''.join(filter(str.isdigit, tech_id_str)))
        return tech_id

    except Exception as e:
        logging.warning(f"LLM fallback: {e}")
        return top3[0]["technician_id"]

# Recommend Technician Endpoint
@app.get("/recommend")
def recommend_technician(customer_id: int):
    customer = customers.find_one({"id": customer_id})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    customer_coord = [customer["longitude"], customer["latitude"]]
    free_techs = list(technicians.find({"is_free": True}))
    if not free_techs:
        raise HTTPException(status_code=404, detail="No available technicians")

    distances = []
    for tech in free_techs:
        tech_coord = [tech["longitude"], tech["latitude"]]
        distance, method = compute_distance_ors(customer_coord, tech_coord)
        if distance is not None:
            logging.info(f"Customer {customer_id} → Technician {tech['id']} ({tech['name']}) → {distance:.2f} km via {method}")
            distances.append({
                "technician_id": tech["id"],
                "name": tech["name"],
                "distance_km": round(distance, 2),
                "method": method
            })
        else:
            logging.warning(f"❌ ORS distance failed for technician {tech['id']}")

    if not distances:
        raise HTTPException(status_code=500, detail="No distances could be computed")

    top3 = sorted(distances, key=lambda x: x["distance_km"])[:3]
    best_id = llm_recommend_best_technician(customer_id, top3)
    best = next(t for t in top3 if t["technician_id"] == best_id)

    technicians.update_one(
        {"id": best["technician_id"]},
        {"$set": {
            "is_free": False,
            "assigned_customer": customer_id,
            "availability_status": "assigned"
        }}
    )

    assignments.insert_one({
        "tech_id": best["technician_id"],
        "customer_id": customer_id,
        "distance_km": best["distance_km"],
        "method": best["method"],
        "status": "assigned"
    })

    return {
        "top3": top3,
        "best_id": best["technician_id"],
        "reason": f"Groq LLM recommended technician {best['name']} as the best match using ORS distance."
    }

# Complete Assignment Endpoint



@app.post("/complete-assignment")
def complete_assignment(technician_id: int = Body(...)):
    technician = technicians.find_one({"id": technician_id})
    if not technician:
        raise HTTPException(status_code=404, detail="Technician not found")

    if technician.get("is_free", True):
        return {
            "message": f"Technician {technician_id} is already marked as available.",
            "status": "noop"
        }

    # 1. Mark technician as free and available
    technicians.update_one(
        {"id": technician_id},
        {"$set": {
            "is_free": True,
            "assigned_customer": None,  # or float("nan") if exporting to pandas
            "availability_status": "available"
        }}
    )

    # 2. Mark any ongoing assignments as completed
    result = assignments.update_many(
        {"tech_id": technician_id, "status": {"$ne": "completed"}},
        {"$set": {"status": "completed"}}
    )

    return {
        "message": f"✅ Technician {technician_id} marked as available and {result.modified_count} assignment(s) closed.",
        "status": "success"
    }


# Health check
@app.get("/ping")
def ping():
    return {"status": "online"}
