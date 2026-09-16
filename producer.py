import json
import time
import pandas as pd
from kafka import KafkaProducer

# ---- Config ----
CSV_FILE = "data/creditcard.csv"
TOPIC = "raw-transactions"
BOOTSTRAP_SERVERS = "localhost:9092"
DELAY_SECONDS = 1  # har event ke beech gap


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    df = pd.read_csv(CSV_FILE)

    # Jaan-boojh kar kuch real fraud rows mix karte hain taaki pipeline actually test ho
    fraud_rows = df[df["Class"] == 1].sample(5, random_state=1)
    normal_rows = df[df["Class"] == 0].sample(15, random_state=1)
    test_batch = (
        pd.concat([fraud_rows, normal_rows])
        .sample(frac=1, random_state=2)
        .reset_index(drop=True)
    )

    print(f"Sending {len(test_batch)} rows ({len(fraud_rows)} fraud, {len(normal_rows)} normal, shuffled)")

    for i, row in test_batch.iterrows():
        event = row.to_dict()
        producer.send(TOPIC, value=event)
        print(f"Sent row {i} (actual Class={int(event['Class'])})")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    print("All rows sent.")


if __name__ == "__main__":
    main()