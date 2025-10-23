import requests
import random
import time
from datetime import datetime

# API endpoint
API_URL = "http://localhost:8000/data"

# Configuration
SENSOR_IDS = ["SENSOR001", "SENSOR002", "SENSOR003", "SENSOR004"]
DEVICE_IDS = ["DEVICE123", "DEVICE456", "DEVICE789"]
TEMP_RANGE = (15.0, 35.0)  # Temperature range in Celsius

def generate_sensor_data():
    """Generate random sensor data"""
    data = {
        "sensor_id": random.choice(SENSOR_IDS),
        "device_id": random.choice(DEVICE_IDS),
        "timestamp": datetime.now().isoformat(),
        "temp_value": round(random.uniform(*TEMP_RANGE), 2)
    }
    return data

def send_data(data):
    """Send data to the FastAPI endpoint"""
    try:
        response = requests.post(API_URL, json=data)
        
        if response.status_code == 201:
            print(f"✓ Data sent successfully: {data['sensor_id']} - {data['temp_value']}°C")
            print(f"  Response: {response.json()['message']}\n")
        else:
            print(f"✗ Failed to send data. Status code: {response.status_code}")
            print(f"  Response: {response.text}\n")
            
    except requests.exceptions.ConnectionError:
        print("✗ Connection error. Make sure the FastAPI server is running on http://localhost:8000\n")
    except Exception as e:
        print(f"✗ Error: {str(e)}\n")

def main():
    """Main function to generate and send data"""
    print("=" * 60)
    print("SENSOR DATA GENERATOR")
    print("=" * 60)
    print("Sending data to:", API_URL)
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            # Generate and send data
            sensor_data = generate_sensor_data()
            send_data(sensor_data)
            
            # Wait before sending next data point
            time.sleep(2)  # Send data every 2 seconds
            
    except KeyboardInterrupt:
        print("\n\nStopped by user. Goodbye!")

if __name__ == "__main__":
    main()