"""
Mock UPI / banking data generator.
Generates realistic CSV bank statement dumps and seeds the mock API server.
Run once before starting the pipeline: python data/mock_data_generator.py
"""
import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

# ── Reference data ─────────────────────────────────────────────────────────────────────────────
BANKS = ["okaxis", "oksbi", "okhdfcbank", "okicici", "paytm", "ybl", "upi"]
STATUS_WEIGHTS = ["SUCCESS"] * 75 + ["FAILED"] * 15 + ["PENDING"] * 8 + ["REVERSED"] * 2
DEVICE_TYPES = ["Android", "iOS", "Web"]
CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
          "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow"]


def random_vpa():
    name = random.choice(["rahul", "priya", "amit", "sneha", "raj", "anita",
                          "vikas", "pooja", "suresh", "kavya", "arjun", "divya"])
    bank = random.choice(BANKS)
    return f"{name}{random.randint(1, 999)}@{bank}"


def random_amount():
    # realistic UPI amount distribution
    tier = random.random()
    if tier < 0.50:  return round(random.uniform(10, 500), 2)       # small
    if tier < 0.80:  return round(random.uniform(500, 5000), 2)     # medium
    if tier < 0.95:  return round(random.uniform(5000, 50000), 2)   # large
    return round(random.uniform(50000, 200000), 2)                   # high-value


def make_transaction(date: datetime) -> dict:
    hour_weights = ([0] * 6 + [2, 3, 4, 5, 6, 7, 8, 8, 8, 7, 6, 5]
                    + [4, 4, 3, 3, 2, 1])   # peak 9 AM–11 AM, 1 PM–5 PM
    hour = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    ts = date.replace(hour=hour, minute=minute, second=second)

    sender = random_vpa()
    receiver = random_vpa()
    while receiver == sender:
        receiver = random_vpa()

    return {
        "txn_id":        str(uuid.uuid4()),
        "upi_ref":       f"UPI{random.randint(100000000000, 999999999999)}",
        "sender_vpa":    sender,
        "receiver_vpa":  receiver,
        "amount":        random_amount(),
        "currency":      "INR",
        "status":        random.choice(STATUS_WEIGHTS),
        "txn_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "device_type":   random.choice(DEVICE_TYPES),
        "device_id":     str(uuid.uuid4())[:16],
        "ip_address":    f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}",
        "city":          random.choice(CITIES),
        "remarks":       random.choice(["Food", "Rent", "Transfer", "Bill", "", "Shopping", "Travel"]),
        "ingested_at":   datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ── CSV generator (bank statement dumps) ────────────────────────────────────────────────
def generate_csv_dumps(output_dir: Path, days: int = 7, rows_per_day: int = 500):
    output_dir.mkdir(parents=True, exist_ok=True)
    base_date = datetime.today() - timedelta(days=days)

    for i in range(days):
        date = base_date + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        filepath = output_dir / f"bank_statement_{date_str}.csv"

        rows = [make_transaction(date) for _ in range(rows_per_day)]
        # inject a few duplicates (realistic scenario)
        duplicates = random.sample(rows, k=5)
        rows.extend(duplicates)
        random.shuffle(rows)

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"  CSV: {filepath.name}  ({len(rows)} rows)")


# ── API seed data ────────────────────────────────────────────────────────────────────────
def generate_api_seed(output_dir: Path, days: int = 7, rows_per_day: int = 300):
    output_dir.mkdir(parents=True, exist_ok=True)
    base_date = datetime.today() - timedelta(days=days)
    all_records = []

    for i in range(days):
        date = base_date + timedelta(days=i)
        records = [make_transaction(date) for _ in range(rows_per_day)]
        all_records.extend(records)

    seed_file = output_dir / "api_seed_data.json"
    with open(seed_file, "w") as f:
        json.dump(all_records, f, indent=2)

    print(f"  API seed: {seed_file.name}  ({len(all_records)} records)")


if __name__ == "__main__":
    base = Path(__file__).parent
    print("Generating mock data...")
    generate_csv_dumps(base / "mock_csv", days=24, rows_per_day=500)
    generate_api_seed(base / "mock_api", days=24, rows_per_day=300)
    print("Done. Mock data ready in data/mock_csv/ and data/mock_api/")
