import os
import json
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from geopy.distance import geodesic

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
ORS_API_KEY = os.getenv("ORS_API_KEY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

client = MongoClient(MONGO_URI)
db = client.proximity_dispatch

def load_customer_agent(state):
    ticket = state.ticket
    cust_id = ticket.get("id")
    if cust_id is None:
        raise Exception("Missing customer ID in ticket")

    customer = db.customers.find_one({"customer_id": cust_id})
    if not customer:
        raise Exception(f"Customer with ID {cust_id} not found")

    state.customer = customer
    return state

def compute_distance(customer_coord, tech_coord):
    try:
        # 1. Try OpenRouteService
        ors_url = "https://api.openrouteservice.org/v2/directions/driving-car"
        headers = {"Authorization": ORS_API_KEY, "Content-Type": "application/json"}
        body = {"coordinates": [[customer_coord[1], customer_coord[0]], [tech_coord[1], tech_coord[0]]]}

        res = requests.post(ors_url, json=body, headers=headers, timeout=10)
        print("\n🚀 ORS status:", res.status_code)

        if res.status_code != 200:
            print("❌ ORS Error:", res.text)
            raise Exception("ORS failed")

        data = res.json()
        meters = data["features"][0]["properties"]["segments"][0]["distance"]
        distance_km = round(meters / 1000.0, 2)
        print(f"✅ ORS Distance: {distance_km} km")
        return distance_km, "ORS"

    except Exception as e:
        print(f"⚠️ ORS failed: {e}")
        try:
            # 2. Try OSM (OSRM)
            osm_url = f"http://router.project-osrm.org/route/v1/driving/{customer_coord[1]},{customer_coord[0]};{tech_coord[1]},{tech_coord[0]}?overview=false"
            osm_res = requests.get(osm_url, timeout=10)
            print("🚀 OSM status:", osm_res.status_code)

            if osm_res.status_code == 200:
                osm_data = osm_res.json()
                meters = osm_data["routes"][0]["distance"]
                distance_km = round(meters / 1000.0, 2)
                print(f"✅ OSM Distance: {distance_km} km")
                return distance_km, "OSRM"
            else:
                print("❌ OSM Error:", osm_res.text)
        except Exception as e2:
            print(f"⚠️ OSM fallback failed: {e2}")

        try:
            # 3. Try Geopy
            distance_km = round(geodesic(customer_coord, tech_coord).km, 2)
            print(f"✅ Geopy Distance: {distance_km} km")
            return distance_km, "Geopy"
        except Exception as e3:
            print(f"❌ Geopy failed: {e3}")
            return None, "Failed"

def compute_proximity_agent(state):
    customer = state.customer
    candidates = list(db.technicians.find({"is_free": True}))

    if not candidates:
        raise Exception("No free technicians available")

    for tech in candidates:
        try:
            if not all([
                customer.get("latitude"), customer.get("longitude"),
                tech.get("latitude"), tech.get("longitude")
            ]):
                raise ValueError("Missing coordinates")

            customer_coord = (float(customer["latitude"]), float(customer["longitude"]))
            tech_coord = (float(tech["latitude"]), float(tech["longitude"]))

            distance, method = compute_distance(customer_coord, tech_coord)
            tech["distance_km"] = distance
            tech["distance_method"] = method
            print(f"🧭 Tech {tech['technician_id']} distance: {distance} km via {method}")

        except Exception as e:
            print(f"⚠️ Failed to compute distance for tech {tech.get('technician_id')}: {e}")
            tech["distance_km"] = None
            tech["distance_method"] = "Failed"

    sorted_techs = sorted(
        [t for t in candidates if t["distance_km"] is not None],
        key=lambda t: t["distance_km"]
    )

    top3 = sorted_techs[:3]
    if not top3:
        raise Exception("No technicians with valid distance computed")

    prompt = f"""
You are a smart dispatcher.
Here are the 3 nearest technicians to a customer based on driving distance.
Choose the best technician and write the reason.

Technicians:
{json.dumps([{ "id": t["technician_id"], "name": t["name"], "distance_km": t["distance_km"] } for t in top3], indent=2)}

Respond in JSON:
{{"id": <best_id>, "reason": "<why>"}}
"""

    try:
        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama3-8b-8192",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3
            },
            timeout=30
        )

        print("📡 LLM status:", res.status_code)
        print("📡 LLM response:", res.text)

        if res.status_code != 200 or not res.text.strip():
            raise ValueError("LLM returned empty or error response")

        content = res.json()["choices"][0]["message"]["content"]
        result = json.loads(content)

        chosen = next((t for t in top3 if t["technician_id"] == result.get("id")), top3[0])
        state.best = chosen
        state.llm_reason = result.get("reason", "Chosen based on proximity")
    except Exception as e:
        print("❌ LLM selection failed:", e)
        chosen = top3[0]
        state.best = chosen
        state.llm_reason = "LLM failed, assigned first nearest technician"

    return state

def assign_agent(state):
    best = state.best
    customer = state.customer

    tech_id = best.get("technician_id")
    cust_id = customer.get("customer_id")

    tech_doc = db.technicians.find_one({"technician_id": tech_id})
    cust_doc = db.customers.find_one({"customer_id": cust_id})

    db.assignments.update_many({
        "$or": [
            {"tech_id": tech_id, "status": "assigned"},
            {"customer_id": cust_id, "status": "assigned"}
        ]
    }, {"$set": {"status": "completed"}})

    db.technicians.update_one({"_id": tech_doc["_id"]}, {
        "$set": {
            "is_free": False,
            "availability_status": "assigned",
            "assigned_customer": cust_id
        }
    })

    db.assignments.insert_one({
        "customer_id": cust_id,
        "tech_id": tech_id,
        "customer_object_id": cust_doc["_id"],
        "tech_object_id": tech_doc["_id"],
        "customer_name": cust_doc["name"],
        "tech_name": tech_doc["name"],
        "distance_km": best.get("distance_km"),
        "distance_method": best.get("distance_method"),
        "llm_reason": state.llm_reason,
        "status": "assigned"
    })

    state.best = tech_doc
    state.customer = cust_doc
    return state
