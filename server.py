from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import paho.mqtt.client as mqtt
import json
import threading
import os
from typing import List, Optional

app = FastAPI(title="Sensor Data API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration - Use environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'root'),
    'database': os.getenv('DB_NAME', 'default'),
    'port': int(os.getenv('DB_PORT', '3306'))
}

# MQTT Configuration - Use environment variables
MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'sensor/data')
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'sensor_server')

# Pydantic model for sensor data
class SensorData(BaseModel):
    sensor_id: str = Field(..., description="Unique identifier for the sensor")
    device_id: str = Field(..., description="Unique identifier for the device")
    timestamp: datetime = Field(..., description="Timestamp of the reading")
    temp_value: float = Field(..., description="Temperature value")

    class Config:
        json_schema_extra = {
            "example": {
                "sensor_id": "SENSOR001",
                "device_id": "DEVICE123",
                "timestamp": "2025-10-22T10:30:00",
                "temp_value": 23.5
            }
        }

# Pydantic model for sensor reading response (includes record ID)
class SensorReading(BaseModel):
    id: int = Field(..., description="Database record ID")
    sensor_id: str = Field(..., description="Unique identifier for the sensor")
    device_id: str = Field(..., description="Unique identifier for the device")
    timestamp: datetime = Field(..., description="Timestamp of the reading")
    temp_value: float = Field(..., description="Temperature value")

def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def insert_sensor_data(data: SensorData):
    """Insert sensor data into MySQL database"""
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO sensor_data (sensor_id, device_id, timestamp, temp_value)
            VALUES (%s, %s, %s, %s)
        """
        values = (data.sensor_id, data.device_id, data.timestamp, data.temp_value)
        cursor.execute(query, values)
        connection.commit()
        
        record_id = cursor.lastrowid
        cursor.close()
        connection.close()
        
        return record_id
        
    except Error as e:
        print(f"Error inserting data: {e}")
        if connection:
            connection.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

def get_latest_readings(sensor_id: str, limit: int = 10):
    """Retrieve latest N readings for a specific sensor"""
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = connection.cursor(dictionary=True)
        query = """
            SELECT id, sensor_id, device_id, timestamp, temp_value
            FROM sensor_data
            WHERE sensor_id = %s
            ORDER BY timestamp DESC, id DESC
            LIMIT %s
        """
        cursor.execute(query, (sensor_id, limit))
        results = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return results
        
    except Error as e:
        print(f"Error retrieving data: {e}")
        if connection:
            connection.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

def get_all_sensors():
    """Retrieve list of unique sensor IDs"""
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = connection.cursor()
        query = """
            SELECT DISTINCT sensor_id
            FROM sensor_data
            ORDER BY sensor_id
        """
        cursor.execute(query)
        results = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
        
        return results
        
    except Error as e:
        print(f"Error retrieving sensors: {e}")
        if connection:
            connection.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    if rc == 0:
        print("\n✓ Connected to MQTT Broker successfully!")
        # Subscribe to the sensor data topic
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"✓ Subscribed to topic: {MQTT_TOPIC}\n")
    else:
        print(f"✗ Failed to connect to MQTT Broker. Return code: {rc}")

def on_message(client, userdata, msg):
    """Callback when a message is received from MQTT"""
    try:
        # Parse JSON payload
        payload = json.loads(msg.payload.decode())
        
        # Convert timestamp string to datetime object
        payload['timestamp'] = datetime.fromisoformat(payload['timestamp'])
        
        # Create SensorData object
        sensor_data = SensorData(**payload)
        
        # Insert into database
        record_id = insert_sensor_data(sensor_data)
        
        # Print received data to console
        print("\n" + "="*50)
        print("SENSOR DATA RECEIVED VIA MQTT:")
        print("="*50)
        print(f"Record ID:    {record_id}")
        print(f"Sensor ID:    {sensor_data.sensor_id}")
        print(f"Device ID:    {sensor_data.device_id}")
        print(f"Timestamp:    {sensor_data.timestamp}")
        print(f"Temperature:  {sensor_data.temp_value}°C")
        print("="*50 + "\n")
        
    except json.JSONDecodeError as e:
        print(f"✗ Error decoding JSON: {e}")
    except Exception as e:
        print(f"✗ Error processing message: {e}")

def on_disconnect(client, userdata, rc):
    """Callback when disconnected from MQTT broker"""
    if rc != 0:
        print("✗ Unexpected disconnection from MQTT Broker")

def start_mqtt_client():
    """Initialize and start MQTT client in a separate thread"""
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    
    # Set callbacks
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect
    
    try:
        print(f"Connecting to MQTT Broker at {MQTT_BROKER}:{MQTT_PORT}...")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        # Start the MQTT loop in a separate thread
        mqtt_client.loop_start()
        
    except Exception as e:
        print(f"✗ Failed to connect to MQTT Broker: {e}")

# POST endpoint to receive sensor data (REST API fallback)
@app.post("/data", status_code=201)
async def submit_sensor_data(data: SensorData):
    """
    Submit new sensor data and store in MySQL database (REST API endpoint)
    """
    # Insert data into database
    record_id = insert_sensor_data(data)
    
    # Print received data to console
    print("\n" + "="*50)
    print("SENSOR DATA RECEIVED VIA REST API:")
    print("="*50)
    print(f"Record ID:    {record_id}")
    print(f"Sensor ID:    {data.sensor_id}")
    print(f"Device ID:    {data.device_id}")
    print(f"Timestamp:    {data.timestamp}")
    print(f"Temperature:  {data.temp_value}°C")
    print("="*50 + "\n")
    
    return {
        "status": "success",
        "message": "Sensor data received and stored in database",
        "record_id": record_id,
        "data": {
            "sensor_id": data.sensor_id,
            "device_id": data.device_id,
            "timestamp": data.timestamp.isoformat(),
            "temp_value": data.temp_value
        }
    }

# GET endpoint to retrieve latest readings for a specific sensor
@app.get("/sensors/{sensor_id}/readings", response_model=List[SensorReading])
async def get_sensor_readings(
    sensor_id: str,
    limit: int = Query(default=10, ge=1, le=100, description="Number of readings to retrieve")
):
    """
    Get the latest N readings for a specific sensor (default: 10, max: 100)
    
    Example: /sensors/sensor1/readings?limit=10
    """
    readings = get_latest_readings(sensor_id, limit)
    
    if not readings:
        raise HTTPException(
            status_code=404, 
            detail=f"No readings found for sensor: {sensor_id}"
        )
    
    return readings

# GET endpoint to retrieve list of all sensors
@app.get("/sensors", response_model=List[str])
async def list_sensors():
    """
    Get a list of all unique sensor IDs that have recorded data
    """
    sensors = get_all_sensors()
    
    if not sensors:
        return []
    
    return sensors

@app.get("/")
async def root():
    """Root endpoint to check if API is running"""
    return {
        "message": "Sensor Data API is running",
        "version": "1.0.0",
        "endpoints": {
            "POST /data": "Submit sensor data",
            "GET /sensors": "List all sensors",
            "GET /sensors/{sensor_id}/readings": "Get latest readings for a sensor",
            "GET /docs": "API documentation"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    # Check database connection
    db_status = "connected"
    try:
        connection = get_db_connection()
        if connection:
            connection.close()
        else:
            db_status = "disconnected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "database": db_status,
        "mqtt_broker": MQTT_BROKER,
        "mqtt_port": MQTT_PORT
    }

@app.on_event("startup")
async def startup_event():
    """Start MQTT client when FastAPI starts"""
    print("\n" + "="*60)
    print("SENSOR DATA API SERVER STARTING")
    print("="*60)
    print(f"Database Host: {DB_CONFIG['host']}")
    print(f"Database Name: {DB_CONFIG['database']}")
    print(f"Database User: {DB_CONFIG['user']}")
    print(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print("="*60 + "\n")
    start_mqtt_client()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
