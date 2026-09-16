import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

# ---- Config ----
CSV_FILE = "data/creditcard.csv"
MODEL_FILE = "fraud_model.pkl"
SCALER_FILE = "scaler.pkl"


def main():
    print("Loading data...")
    df = pd.read_csv(CSV_FILE)
    print(f"Loaded {len(df)} rows")
    print(df["Class"].value_counts())

    # V1-V28 pehle se PCA-scaled hain; Time aur Amount ko alag se scale karna hai
    scaler = StandardScaler()
    df[["Time", "Amount"]] = scaler.fit_transform(df[["Time", "Amount"]])

    X = df.drop(columns=["Class"])
    y = df["Class"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print("Training model... (thoda time lagega, ~284k rows hain sabar kr lala )")
    model = RandomForestClassifier(
        n_estimators=100, class_weight="balanced", n_jobs=-1, random_state=42
    )
    model.fit(X_train, y_train)

    print("\nEvaluating on held-out test set:")
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=["Not Fraud", "Fraud"]))
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    with open(MODEL_FILE, "wb") as f:
        pickle.dump(model, f)
    with open(SCALER_FILE, "wb") as f:
        pickle.dump(scaler, f)

    print(f"\nModel saved to {MODEL_FILE}")
    print(f"Scaler saved to {SCALER_FILE}")


if __name__ == "__main__":
    main()