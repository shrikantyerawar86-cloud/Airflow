import json
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import fastavro

# Function to generate a random transaction record
def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "amount": round(random.uniform(10, 1000), 2),
        "transaction_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S"),
        "currency": random.choice(["USD", "EUR", "GBP", "INR"]),
        "status": random.choice(["completed", "pending", "failed"])
    }

# Generate 1000 transaction records
transactions = [generate_transaction() for _ in range(1000)]

# Convert the list of transactions to a DataFrame
df = pd.DataFrame(transactions)

# Define the schema for the AVRO file
avro_schema = {
    "type": "record",
    "name": "transaction",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "transaction_date", "type": "string"},
        {"name": "currency", "type": "string"},
        {"name": "status", "type": "string"}
    ]
}

# Convert the DataFrame to a list of dictionaries for AVRO
records = df.to_dict(orient='records')

# Save to an AVRO file
avro_file_path = 'transactions.avro'
with open(avro_file_path, 'wb') as out:
    fastavro.writer(out, avro_schema, records)

print(f"AVRO file saved to {avro_file_path}")
