import os

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB Atlas connection string
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "fraud_detection"
COLLECTION_NAME = "predictions"

app = FastAPI(title="Fraud Detection API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
client = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION_NAME]


@app.get("/predictions")
def get_predictions(
    since: str | None = Query(default=None),
    limit: int = 1000
):

    # --------------------------------------------------
    # If dashboard sends a start time,
    # return ONLY records created after that time.
    # --------------------------------------------------

    query = {}

    if since:

        try:
            start_time = datetime.fromisoformat(
                since.replace("Z", "+00:00")
            )

            query["created_at"] = {
                "$gte": start_time
            }

        except ValueError:

            return []


    # Get records from MongoDB
    docs = (
        collection
        .find(query)
        .sort("_id", -1)
        .limit(limit)
    )

    result = []

    for d in docs:

        d["_id"] = str(d["_id"])

        # Convert datetime to JSON-friendly string
        if isinstance(d.get("created_at"), datetime):

            d["created_at"] = d["created_at"].isoformat()

        result.append(d)

    return result


@app.get("/stats")
def get_stats(
    since: str | None = Query(default=None)
):

    # --------------------------------------------------
    # Dashboard start time provided
    # --------------------------------------------------

    query = {}

    if since:

        try:
            start_time = datetime.fromisoformat(
                since.replace("Z", "+00:00")
            )

            query["created_at"] = {
                "$gte": start_time
            }

        except ValueError:

            return {
                "total_transactions": 0,
                "fraud_flagged": 0
            }


    # Count ONLY dashboard-session transactions
    total = collection.count_documents(query)

    fraud_query = {
        **query,
        "predicted_fraud": 1
    }

    fraud = collection.count_documents(fraud_query)

    return {
        "total_transactions": total,
        "fraud_flagged": fraud
    }