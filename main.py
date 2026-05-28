import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Literal

import httpx
import json
import asyncio
from fastapi import FastAPI, HTTPException, status, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from redis import asyncio as aioredis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ========================= CONFIGURATION =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("shipment_analysis.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Environment Variables
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080")
API_TIMEOUT = float(os.getenv("API_TIMEOUT", "30.0"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost")

# Initialize Redis with proper URL
redis = aioredis.from_url(REDIS_URL, decode_responses=True)

# FastAPI App
app = FastAPI(title="Transport Insights - Shipment Analysis API")

# CORS Middleware (Security Fix)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # Change to your frontend domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate Limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Locks for concurrent safety
fetch_vehicles_lock = asyncio.Lock()
fetch_shipments_lock = asyncio.Lock()

PERIOD_DAYS = {
    "week": 7,
    "month": 30,
    "year": 365
}

# ========================= HELPER FUNCTIONS =========================
def validate_numeric(v):
    if v is None or v == "":
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def validate_shipment_status(v):
    if not v or not isinstance(v, str) or v.strip() == "":
        return "PENDING"
    return v.upper()

# ========================= DATA FETCHING =========================
async def fetch_vehicles(transporter_id: int) -> List[Dict]:
    cache_key = f"vehicles:{transporter_id}"
    cached_data = await redis.get(cache_key)
    
    if cached_data:
        return json.loads(cached_data)

    async with fetch_vehicles_lock:
        try:
            async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
                url = f"{API_BASE_URL}/api/vehicles/getVehicle/{transporter_id}"
                response = await client.get(url)
                response.raise_for_status()
                vehicles_data = response.json()
                
                processed_vehicles = [
                    {
                        "vehicle_id": vehicle.get("vehicleId"),
                        "vehicle_number": vehicle.get("vehicleNumber", "Unknown"),
                        "transporter_id": transporter_id
                    }
                    for vehicle in vehicles_data
                ]
                
                await redis.set(cache_key, json.dumps(processed_vehicles), ex=600)
                return processed_vehicles
        except Exception as e:
            logger.error(f"Error fetching vehicles for transporter {transporter_id}: {e}")
            raise HTTPException(status_code=404, detail="Failed to fetch vehicles")

async def fetch_shipments(vehicle_id: int, period: str) -> List[Dict]:
    days = PERIOD_DAYS[period]
    cache_key = f"shipments:{vehicle_id}:{period}"
    cached_data = await redis.get(cache_key)
    
    if cached_data:
        shipments_data = json.loads(cached_data)
        for shipment in shipments_data:
            if "updated_at" in shipment and isinstance(shipment["updated_at"], str):
                shipment["updated_at"] = datetime.fromisoformat(shipment["updated_at"].replace("Z", "+00:00"))
        return shipments_data

    async with fetch_shipments_lock:
        try:
            async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
                url = f"{API_BASE_URL}/assignShipments/assigned-last-{days}-days/{vehicle_id}"
                response = await client.get(url)
                response.raise_for_status()
                assignments = response.json()
                
                processed_shipments = []
                for assignment in assignments:
                    if assignment.get("shipment"):
                        shipment = assignment.get("shipment", {})
                        processed_shipment = {
                            "shipment_id": shipment.get("shipmentId"),
                            "quoted_price": validate_numeric(shipment.get("quotedPrice")),
                            "distance": validate_numeric(shipment.get("distance")),
                            "shipment_status": validate_shipment_status(shipment.get("shipmentStatus")),
                            "updated_at": datetime.fromisoformat(assignment.get("updatedAt").replace("Z", "+00:00"))
                        }
                        processed_shipments.append(processed_shipment)
                
                # Cache serializable version
                serializable = [{**s, "updated_at": s["updated_at"].isoformat()} for s in processed_shipments]
                await redis.set(cache_key, json.dumps(serializable), ex=600)
                
                return processed_shipments
        except Exception as e:
            logger.error(f"Error fetching shipments for vehicle {vehicle_id}: {e}")
            return []

def process_shipment_metrics(shipment: dict) -> dict:
    status = shipment["shipment_status"].upper()
    metrics = {
        "distance": shipment["distance"],
        "income": 0.0,
        "delivered": 0,
        "pending": 0,
        "in_transit": 0,
    }
    
    if status == "DELIVERED":
        metrics["income"] = shipment["quoted_price"]
        metrics["delivered"] = 1
    elif status == "PENDING":
        metrics["pending"] = 1
    elif status == "IN_TRANSIT":
        metrics["in_transit"] = 1
    
    return metrics

# ========================= MAIN ANALYSIS =========================
async def analyze_shipments(transporter_id: int, period: str):
    vehicles = await fetch_vehicles(transporter_id)
    if not vehicles:
        raise HTTPException(status_code=404, detail="No vehicles found for this transporter")

    total_metrics = {"distance": 0.0, "income": 0.0, "delivered": 0, "pending": 0, "in_transit": 0}
    vehicles_data = []

    for vehicle in vehicles:
        shipments = await fetch_shipments(vehicle["vehicle_id"], period)
        vehicle_metrics = {
            "vehicle_id": vehicle["vehicle_id"],
            "vehicle_number": vehicle["vehicle_number"],
            "distance": 0.0,
            "income": 0.0,
            "delivered": 0,
            "pending": 0,
            "in_transit": 0,
        }

        for shipment in shipments:
            metrics = process_shipment_metrics(shipment)
            for key in total_metrics:
                vehicle_metrics[key] += metrics[key]
                total_metrics[key] += metrics[key]

        vehicles_data.append(vehicle_metrics)

    result = {
        "transporter_id": transporter_id,
        "total_vehicles": len(vehicles),
        "total_distance": round(total_metrics["distance"], 2),
        "total_income": round(total_metrics["income"], 2),
        "delivered_shipments": int(total_metrics["delivered"]),
        "pending_shipments": int(total_metrics["pending"]),
        "in_transit_shipments": int(total_metrics["in_transit"]),
        "start_date": (datetime.utcnow() - timedelta(days=PERIOD_DAYS[period])).isoformat(),
        "end_date": datetime.utcnow().isoformat(),
        "vehicles_data": vehicles_data,
        "period": period
    }
    
    return result

# ========================= API ENDPOINT =========================
@app.get("/analyze/{transporter_id}/{period}")
@limiter.limit("5/minute")
async def analyze_shipments_endpoint(
    request: Request,
    transporter_id: int,
    period: Literal["week", "month", "year"],
    background_tasks: BackgroundTasks
):
    try:
        result = await analyze_shipments(transporter_id, period)
        background_tasks.add_task(logger.info, f"Analysis completed for transporter {transporter_id}")
        return result
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Critical error in analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

# Health Check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "Transport Insights - Shipment Analysis"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
