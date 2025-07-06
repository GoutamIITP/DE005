import os
import sys
import traceback
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, inspect, text
import pandas as pd

def copy_selective_users():
    """
    Copy specific users and their related data from source to target database.
    Only copies users: Goutam, Priya Singh, Ravi
    """
    
    # Database paths (adjust these based on your actual file locations)
    source_db_path = os.path.join(os.path.dirname(__file__), "..", "dummy.db")
    target_db_path = os.path.join(os.path.dirname(__file__), "..", "copied_dummy.db")
    
    # Convert to absolute paths
    source_db_path = os.path.abspath(source_db_path)
    target_db_path = os.path.abspath(target_db_path)
    
    print("=== Selective Database Copy Operation ===")
    print(f"Source: {source_db_path}")
    print(f"Target: {target_db_path}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if source database exists
    if not os.path.exists(source_db_path):
        print(f"Error: Source database not found at {source_db_path}")
        return False
    
    try:
        # Create database engines
        source = create_engine(f"sqlite:///{source_db_path}")
        target = create_engine(f"sqlite:///{target_db_path}")
        
        # Test database connections
        print("\n Testing database connections...")
        with source.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Source database connection successful")
        
        with target.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Target database connection successful")
        
        # Define the specific users we want to copy
        target_users = ["Goutam", "Priya Singh", "Ravi"]
        print(f"\n Target users: {target_users}")
        
        # Define what tables and columns to copy
        tables_to_copy = {
            "users": ["name", "email"],  # Copy specific columns from users table
            "orders": ["amount"]         # Copy specific columns from orders table
        }
        
        # Step 1: Copy filtered users table
        print(f"\n Step 1: Copying users table with filters...")
        
        # Create WHERE clause for user filtering
        user_filter = "', '".join(target_users)  # Creates: Goutam', 'Priya Singh', 'Ravi
        where_clause = f"WHERE name IN ('{user_filter}')"
        
        # Get users columns
        users_columns = ", ".join(tables_to_copy["users"])
        users_query = f"SELECT {users_columns} FROM users {where_clause}"
        
        print(f"Query: {users_query}")
        
        # Execute the query
        users_df = pd.read_sql(users_query, source)
        print(f"Found {len(users_df)} matching users:")
        if len(users_df) > 0:
            print(users_df.to_string(index=False))
        else:
            print(" No users found matching the criteria")
            return False
        
        # Save filtered users to target database
        users_df.to_sql("users_partial", target, if_exists='replace', index=False)
        print(f"Copied {len(users_df)} users to 'users_partial' table")
        
        # Step 2: Copy related orders (if orders table exists and has user relationship)
        print(f"\n Step 2: Checking for related orders...")
        
        try:
            # First check if orders table exists and has user relationship
            inspector = inspect(source)
            available_tables = inspector.get_table_names()
            
            if "orders" in available_tables:
                # Check if orders table has user_id or similar column
                orders_columns = [col['name'] for col in inspector.get_columns('orders')]
                print(f"Orders table columns: {orders_columns}")
                
                # Try to find user relationship column
                user_relation_col = None
                for col in orders_columns:
                    if 'user' in col.lower() and 'id' in col.lower():
                        user_relation_col = col
                        break
                
                if user_relation_col:
                    print(f"Found user relation column: {user_relation_col}")
                    
                    # First, get user IDs from our filtered users
                    user_ids_query = f"SELECT id, name FROM users {where_clause}"
                    user_ids_df = pd.read_sql(user_ids_query, source)
                    
                    if len(user_ids_df) > 0:
                        user_ids = user_ids_df['id'].tolist()
                        user_ids_str = ", ".join(map(str, user_ids))
                        
                        # Get orders for these users
                        orders_columns_str = ", ".join(tables_to_copy["orders"])
                        orders_query = f"SELECT {orders_columns_str}, {user_relation_col} FROM orders WHERE {user_relation_col} IN ({user_ids_str})"
                        
                        print(f"Orders query: {orders_query}")
                        
                        orders_df = pd.read_sql(orders_query, source)
                        print(f"Found {len(orders_df)} related orders")
                        
                        if len(orders_df) > 0:
                            print(orders_df.to_string(index=False))
                            orders_df.to_sql("orders_partial", target, if_exists='replace', index=False)
                            print(f"Copied {len(orders_df)} orders to 'orders_partial' table")
                        else:
                            print("No orders found for the selected users")
                    else:
                        print("No user IDs found for orders lookup")
                else:
                    print(" No user relationship column found in orders table")
                    # Copy all orders (without filtering)
                    orders_columns_str = ", ".join(tables_to_copy["orders"])
                    orders_query = f"SELECT {orders_columns_str} FROM orders"
                    orders_df = pd.read_sql(orders_query, source)
                    orders_df.to_sql("orders_partial", target, if_exists='replace', index=False)
                    print(f"Copied all {len(orders_df)} orders to 'orders_partial' table (no user filtering)")
            else:
                print("Orders table not found in source database")
                
        except Exception as e:
            print(f"Error processing orders table: {e}")
            print("Continuing with users table only...")
        
        # Step 3: Verification
        print(f"\n Step 3: Verification...")
        
        # Check what tables were created in target
        target_inspector = inspect(target)
        target_tables = target_inspector.get_table_names()
        print(f"Tables in target database: {target_tables}")
        
        # Show summary of copied data
        for table in target_tables:
            if table.endswith('_partial'):
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                count_df = pd.read_sql(count_query, target)
                count = count_df.iloc[0]['count']
                print(f"{table}: {count} rows")
        
        print(f"\n Selective copy operation completed successfully!")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except Exception as e:
        print(f"Error during selective copy operation: {e}")
        traceback.print_exc()
        return False

def view_copied_data():
    """
    View the data that was copied to verify the operation
    """
    target_db_path = os.path.join(os.path.dirname(__file__), "..", "copied_dummy.db")
    target_db_path = os.path.abspath(target_db_path)
    
    if not os.path.exists(target_db_path):
        print("Target database not found. Run the copy operation first.")
        return
    
    try:
        target = create_engine(f"sqlite:///{target_db_path}")
        
        print("\n=== Copied Data Review ===")
        
        # Show users_partial table
        try:
            users_df = pd.read_sql("SELECT * FROM users_partial", target)
            print(f"\n Users Partial Table ({len(users_df)} rows):")
            print(users_df.to_string(index=False))
        except Exception as e:
            print(f"No users_partial table found: {e}")
        
        # Show orders_partial table
        try:
            orders_df = pd.read_sql("SELECT * FROM orders_partial", target)
            print(f"\n Orders Partial Table ({len(orders_df)} rows):")
            print(orders_df.to_string(index=False))
        except Exception as e:
            print(f"No orders_partial table found: {e}")
            
    except Exception as e:
        print(f" Error viewing copied data: {e}")

if __name__ == "__main__":
    print("Starting Selective Database Copy...")
    success = copy_selective_users()
    
    if success:
        print("\n" + "="*50)
        view_copied_data()
    else:
        print("\n Copy operation failed. Please check the errors above.")