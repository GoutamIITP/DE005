import sqlite3

conn = sqlite3.connect("dummy.db")
cursor = conn.cursor()

# Create tables if not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    amount REAL
)
""")

# users
cursor.executemany("INSERT INTO users (name, email) VALUES (?, ?)", [
    ("Goutam", "goutam@gmail.com"),
    ("Ravi", "ravi@gmail.com"),
    ("Sneha", "sneha.k@gmail.com"),
    ("Amit Kumar", "amitkumar123@yahoo.com"),
    ("Priya Singh", "priya.singh@gmail.com"),
    ("Deepak Sharma", "deepak.s@gmail.in"),
    ("Anjali Mehta", "anjali.mehta@gmail.com"),
    ("Rahul Verma", "rahul.verma11@gmail.com")
])

# orders
cursor.executemany("INSERT INTO orders (user_id, amount) VALUES (?, ?)", [
    (1, 250.5),
    (2, 430.0),
    (3, 180.75),
    (4, 560.0),
    (5, 299.99),
    (6, 125.25),
    (7, 475.5),
    (8, 650.0),
    (2, 320.25),
    (1, 199.0),
    (5, 89.5),
    (7, 399.99)
])

conn.commit()
conn.close()
print("Dummy database with  data created successfully!")
