import requests
import sqlite3
import time

def init_db():
    conn = sqlite3.connect('db/rides.db')
    c = conn.cursor()
    # Create tables if they do not exist
    c.execute('''
        CREATE TABLE IF NOT EXISTS rides (
            ride_uuid TEXT PRIMARY KEY,
            user_uuid TEXT,
            driver_uuid TEXT,
            distance REAL,
            price REAL
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS drivers (
            driver_uuid TEXT PRIMARY KEY,
            name TEXT,
            surname TEXT,
            car_uuid TEXT,
            effective_from TEXT,
            expiry_date TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_uuid TEXT PRIMARY KEY,
            name TEXT,
            surname TEXT,
            is_driver BOOLEAN
        )
    ''')
    conn.commit()
    return conn

def insert_data(conn, data):
    c = conn.cursor()
    ride = data['ride']
    driver = data['driver']
    user = data['user']
    
    c.execute('''
        INSERT OR REPLACE INTO rides (ride_uuid, user_uuid, driver_uuid, distance, price)
        VALUES (?, ?, ?, ?, ?)
    ''', (ride['ride_uuid'], ride['user_uuid'], ride['driver_uuid'], ride['distance'], ride['price']))
    
    c.execute('''
        INSERT OR REPLACE INTO drivers (driver_uuid, name, surname, car_uuid, effective_from, expiry_date)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (driver['driver_uuid'], driver['name'], driver['surname'], driver['car_uuid'], driver['effective_from'], driver['expiry_date']))
    
    c.execute('''
        INSERT OR REPLACE INTO users (user_uuid, name, surname, is_driver)
        VALUES (?, ?, ?, ?)
    ''', (user['user_uuid'], user['name'], user['surname'], user['is_driver']))
    
    conn.commit()

def poll_api(conn):
    while True:
        try:
            # The URL uses the Docker service name "flask_api"
            response = requests.get('http://flask_api:8081/ride')
            if response.status_code == 200:
                data = response.json()
                insert_data(conn, data)
                print("Data inserted:", data)
            else:
                print("Failed to get data from API. Status code:", response.status_code)
        except Exception as e:
            print("Error while fetching data:", e)
        time.sleep(5)  # Poll every 5 seconds

if __name__ == '__main__':
    conn = init_db()
    poll_api(conn)
