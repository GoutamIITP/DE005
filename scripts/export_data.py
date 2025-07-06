import os
import sys
# Allow importing from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import pandas as pd
from config.db_config import get_engine
from fastavro import writer, parse_schema
import pyarrow.parquet as pq
import pyarrow as pa



def export_joined_table():
    print("Starting export_joined_table function...")
    
    try:
        engine = get_engine()
        print("Database engine obtained successfully")

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

        print("Executing database query...")
        df = pd.read_sql(query, engine)
        print(f"Query executed successfully. Retrieved {len(df)} rows")

        # Add timestamp to file names
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"joined_users_orders_{timestamp}"
        
        # Get absolute path for output directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, "..", "output")
        output_dir = os.path.abspath(output_dir)  # Convert to absolute path
        
        print(f"Script directory: {script_dir}")
        print(f"Output directory: {output_dir}")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory created/verified: {output_dir}")

        # Export to CSV
        csv_path = os.path.join(output_dir, f"{base_name}.csv")
        df.to_csv(csv_path, index=False)
        print(f"CSV exported to: {csv_path}")

        # Export to Parquet
        parquet_path = os.path.join(output_dir, f"{base_name}.parquet")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)
        print(f"Parquet exported to: {parquet_path}")

        # Export to Avro
        avro_path = os.path.join(output_dir, f"{base_name}.avro")
        
        # Handle different data types for Avro schema
        schema_fields = []
        for col in df.columns:
            if df[col].dtype == 'int64':
                schema_fields.append({"name": col, "type": ["null", "long"], "default": None})
            elif df[col].dtype == 'float64':
                schema_fields.append({"name": col, "type": ["null", "double"], "default": None})
            else:
                schema_fields.append({"name": col, "type": ["null", "string"], "default": None})
        
        schema = {
            "doc": "Joined users and orders data",
            "name": "joined_record",
            "namespace": "example.avro",
            "type": "record",
            "fields": schema_fields
        }

        # Convert data for Avro (handle null values)
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                if pd.isna(value):
                    record[col] = None
                elif df[col].dtype == 'int64':
                    record[col] = int(value)
                elif df[col].dtype == 'float64':
                    record[col] = float(value)
                else:
                    record[col] = str(value)
            records.append(record)

        with open(avro_path, "wb") as out:
            writer(out, parse_schema(schema), records)
        print(f"Avro exported to: {avro_path}")

        print(f"{base_name} exported successfully to CSV, Parquet, and Avro in: {output_dir}")
        
        # List files in output directory for verification
        files_in_output = os.listdir(output_dir)
        print(f"Files in output directory: {files_in_output}")
        
    except Exception as e:
        print(f"Error in export_joined_table: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    export_joined_table()