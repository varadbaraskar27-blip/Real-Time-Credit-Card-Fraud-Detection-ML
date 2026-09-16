import json
import os
import pickle
import pandas as pd
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError(
        "MONGO_URI not found. "
        "Make sure your .env file exists in the project root "
        "and contains MONGO_URI=..."
    )


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_TOPIC = "raw-transactions"
OUTPUT_TOPIC = "fraud-predictions"

BOOTSTRAP_SERVERS = "localhost:9092"

MODEL_FILE = "fraud_model.pkl"
SCALER_FILE = "scaler.pkl"

GROUP_ID = "fraud-detection-group"

DB_NAME = "fraud_detection"
COLLECTION_NAME = "predictions"


# ============================================================
# MAIN
# ============================================================

def main():

    print("Starting Fraud Detection Consumer...")

    # --------------------------------------------------------
    # Load ML model
    # --------------------------------------------------------

    print("Loading ML model...")

    with open(MODEL_FILE, "rb") as f:
        model = pickle.load(f)

    print("ML model loaded successfully.")


    # --------------------------------------------------------
    # Load scaler
    # --------------------------------------------------------

    print("Loading scaler...")

    with open(SCALER_FILE, "rb") as f:
        scaler = pickle.load(f)

    print("Scaler loaded successfully.")


    # --------------------------------------------------------
    # Connect to MongoDB Atlas
    # --------------------------------------------------------

    print("Connecting to MongoDB Atlas...")

    mongo_client = MongoClient(MONGO_URI)

    # Test MongoDB connection
    mongo_client.admin.command("ping")

    print("MongoDB Atlas connected successfully.")

    collection = mongo_client[DB_NAME][COLLECTION_NAME]


    # --------------------------------------------------------
    # Kafka Consumer
    # --------------------------------------------------------

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(
            v.decode("utf-8")
        ),
    )


    # --------------------------------------------------------
    # Kafka Producer
    # --------------------------------------------------------

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode(
            "utf-8"
        ),
    )


    print()
    print("=" * 60)
    print("FRAUD DETECTION CONSUMER RUNNING")
    print("=" * 60)

    print(
        f"Listening on topic     : {INPUT_TOPIC}"
    )

    print(
        f"Consumer group         : {GROUP_ID}"
    )

    print(
        f"Output topic           : {OUTPUT_TOPIC}"
    )

    print(
        f"MongoDB database       : {DB_NAME}"
    )

    print(
        f"MongoDB collection     : {COLLECTION_NAME}"
    )

    print("=" * 60)
    print()


    # ========================================================
    # PROCESS KAFKA MESSAGES
    # ========================================================

    for message in consumer:

        try:

            # ------------------------------------------------
            # Read Kafka event
            # ------------------------------------------------

            event = message.value


            # ------------------------------------------------
            # Remove Class before prediction
            # ------------------------------------------------

            row = {
                k: v
                for k, v in event.items()
                if k != "Class"
            }


            # ------------------------------------------------
            # Create DataFrame
            # ------------------------------------------------

            df_row = pd.DataFrame([row])


            # ------------------------------------------------
            # Scale Time and Amount
            # ------------------------------------------------

            df_row[["Time", "Amount"]] = scaler.transform(
                df_row[["Time", "Amount"]]
            )


            # ------------------------------------------------
            # ML Prediction
            # ------------------------------------------------

            prediction = int(
                model.predict(df_row)[0]
            )


            # ------------------------------------------------
            # Fraud Probability
            # ------------------------------------------------

            probability = float(
                model.predict_proba(df_row)[0][1]
            )


            # ------------------------------------------------
            # Actual class
            # ------------------------------------------------

            actual = event.get("Class")


            # ------------------------------------------------
            # Result for Kafka output
            # ------------------------------------------------

            result = {
                "original_event": event,
                "predicted_fraud": prediction,
                "fraud_probability": round(
                    probability,
                    4
                ),
            }


            # ------------------------------------------------
            # Send prediction to Kafka
            # ------------------------------------------------

            producer.send(
                OUTPUT_TOPIC,
                value=result
            )

            producer.flush()


            # ------------------------------------------------
            # Current UTC timestamp
            # ------------------------------------------------

            created_at = datetime.now(timezone.utc)


            # ------------------------------------------------
            # Save prediction to MongoDB
            # ------------------------------------------------

            collection.insert_one({

                "time": row.get("Time"),

                "amount": row.get("Amount"),

                "predicted_fraud": prediction,

                "probability": probability,

                "actual_class": actual,

                "created_at": created_at,

            })


            # ------------------------------------------------
            # Console output
            # ------------------------------------------------

            if prediction == 1:

                print(
                    f"🚨 FRAUD ALERT | "
                    f"Probability: {probability:.2%} | "
                    f"Actual Class: {actual} | "
                    f"Amount: {row.get('Amount')}"
                )

            else:

                print(
                    f"✅ OK | "
                    f"Probability: {probability:.2%} | "
                    f"Actual Class: {actual} | "
                    f"Amount: {row.get('Amount')}"
                )


        except Exception as e:

            print()
            print("❌ Error processing transaction:")
            print(e)
            print()


# ============================================================
# RUN PROGRAM
# ============================================================

if __name__ == "__main__":
    main()