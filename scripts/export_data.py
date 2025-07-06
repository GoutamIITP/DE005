import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from config.db_config import get_engine
from fastavro import writer, parse_schema
import pyarrow.parquet as pq
import pyarrow as pa


def export_joined_table():
    engine = get_engine()

    query = """
    SELECT 
        users.id AS user_id,
        users.name AS user_name,
        users.email,
        orders.id AS order_id,
        orders.amount
    FROM users
    JOIN orders ON users.id = orders.user_id
    """
    df = pd.read_sql(query, engine)

    os.makedirs("output", exist_ok=True)

    base_name = "joined_users_orders"

    # Export to CSV
    df.to_csv(f"output/{base_name}.csv", index=False)

    # Export to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"output/{base_name}.parquet")

    # Export to Avro
    schema = {
        "doc": "Joined users and orders data",
        "name": "joined_record",
        "namespace": "example.avro",
        "type": "record",
        "fields": [{"name": col, "type": "string"} for col in df.columns]
    }

    records = df.astype(str).to_dict(orient="records")
    with open(f"output/{base_name}.avro", "wb") as out:
        writer(out, parse_schema(schema), records)

    print(f"{base_name} exported to CSV, Parquet, and Avro!")

# Run
export_joined_table()