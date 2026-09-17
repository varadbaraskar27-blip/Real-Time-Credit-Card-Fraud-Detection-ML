# Real-Time Fraud Detection Pipeline using Apache Kafka


https://github.com/user-attachments/assets/bca3d6d3-2582-40f4-85c5-f22c30789fed


A real-time machine learning pipeline that streams credit card transactions through Apache Kafka, scores each one for fraud using a trained model, and displays live predictions on a dashboard.

## Architecture

```
creditcard.csv
      |
      v
 producer.py  ---->  Kafka topic: raw-transactions
                              |
                              v
                       consumer.py
                (loads trained model + scaler,
                   runs real-time inference)
                        /          \
                       v            v
           Kafka topic:         MongoDB Atlas
        fraud-predictions       (predictions)
                                     |
                                     v
                                  api.py
                              (FastAPI server)
                                     |
                                     v
                             dashboard.html
                        (live auto-refreshing UI)
```

## Tech stack

- **Python** — core language
- **Apache Kafka** (KRaft mode, via Docker) — real-time event streaming
- **scikit-learn** — Random Forest classifier for fraud detection
- **MongoDB Atlas** — stores prediction results
- **FastAPI** — REST API serving predictions to the dashboard
- **HTML/JS** — lightweight live dashboard (polling-based auto-refresh)
- **Docker Compose** — local Kafka broker setup

## Dataset

[Credit Card Fraud Detection dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) from Kaggle — ~284,807 transactions, of which ~0.17% are fraudulent. Not included in this repo due to size; download it and place it at `data/creditcard.csv`.

## Model performance

Trained a Random Forest classifier with `class_weight="balanced"` to handle the severe class imbalance (0.17% fraud rate):

| Metric | Score |
|---|---|
| Precision (fraud) | 90.6% |
| Recall (fraud) | 78.6% |
| F1-score (fraud) | ~84.2% |

## Setup & run

1. Clone the repo and create a virtual environment:
   ```bash
   python -m venv venv
   venv\Scripts\activate   # Windows
   pip install -r requirements.txt
   ```

2. Download the dataset from Kaggle and place it at `data/creditcard.csv`.

3.Create a `.env` file in the project root and add your MongoDB Atlas connection string:

MONGO_URI=mongodb+srv://<username>:<password>@<cluster-url>/


4. Start Kafka locally:
   ```bash
   docker-compose up -d
   ```

5. Create the Kafka topics:
   ```bash
   docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic raw-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic fraud-predictions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

6. Train the model (only needs to be run once):
   ```bash
   python train_model.py
   ```

7. Run the pipeline (each in its own terminal):
   ```bash
   python consumer.py          # listens for events, runs inference, saves to MongoDB
   uvicorn api:app --reload    # serves predictions via REST API
   python producer.py          # streams transactions into Kafka
   ```

8. Open the dashboard:
   ```bash
   python -m http.server 5500
   ```
   Then visit `http://localhost:5500/dashboard.html`

## Future improvements

- Replace dashboard polling with WebSockets for true push-based updates
- Add multiple Kafka partitions and consumer instances for higher throughput
- Automate periodic model retraining as new data arrives
- Add authentication to the API before any public deployment
