import os
import sys
import traceback
# Allow importing from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import create_engine, inspect, text
import pandas as pd

def copy_all_tables():
    # Define database paths
    source_db_path = "dummy.db"
    target_db_path = "copied_dummy.db"
    
    # Make paths absolute to avoid path issues
    source_db_path = os.path.abspath(source_db_path)
    target_db_path = os.path.abspath(target_db_path)
    
    print(f"Source database: {source_db_path}")
    print(f"Target database: {target_db_path}")
    
    # Check if source database exists
    if not os.path.exists(source_db_path):
        print(f" Error: Source database not found at {source_db_path}")
        print("Please ensure the dummy.db file exists in the current directory")
        return False
    
    try:
        # Create database connections
        source = create_engine(f"sqlite:///{source_db_path}")
        target = create_engine(f"sqlite:///{target_db_path}")
        
        # Test source database connection
        try:
            with source.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(" Source database connection successful")
        except Exception as e:
            print(f"Error connecting to source database: {e}")
            return False
        
        # Test target database connection
        try:
            with target.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Target database connection successful")
        except Exception as e:
            print(f"Error connecting to target database: {e}")
            return False
        
        # Get all tables from source database
        inspector = inspect(source)
        tables = inspector.get_table_names()
        
        if not tables:
            print(" No tables found in source database")
            return True
        
        print(f"Found {len(tables)} tables to copy: {tables}")
        
        # Copy each table
        copied_count = 0
        failed_count = 0
        
        for table in tables:
            try:
                print(f"Copying table: {table}")
                
                # Read data from source table
                df = pd.read_sql(f"SELECT * FROM {table}", source)
                print(f"  - Read {len(df)} rows from {table}")
                
                # Write data to target table
                df.to_sql(table, target, if_exists='replace', index=False)
                print(f"Successfully copied {table} ({len(df)} rows)")
                
                copied_count += 1
                
            except Exception as e:
                print(f" Failed to copy table {table}: {e}")
                failed_count += 1
                continue
        
        # Summary
        print(f"\n Copy Summary:")
        print(f" Successfully copied: {copied_count} tables")
        print(f"Failed to copy: {failed_count} tables")
        
        if copied_count > 0:
            print(f"Database copied successfully to {target_db_path}")
            
            # Verify the target database
            target_inspector = inspect(target)
            target_tables = target_inspector.get_table_names()
            print(f"Target database now contains {len(target_tables)} tables")
            
        return failed_count == 0
        
    except Exception as e:
        print(f"Unexpected error during copy operation: {e}")
        traceback.print_exc()
        return False

# Run the function
if __name__ == "__main__":
    print("Starting database copy operation...")
    success = copy_all_tables()
    
    if success:
        print("\n Database copy completed successfully!")
    else:
        print("\n Database copy failed. Please check the errors above.")