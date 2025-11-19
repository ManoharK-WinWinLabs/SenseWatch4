from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import paho.mqtt.client as mqtt
import json
import threading
import os
from typing import List, Optional
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

app = FastAPI(
    title="Sensor Data API", 
    version="1.0.0",
    description="API for collecting and retrieving sensor data via REST and MQTT"
)

# Enhanced CORS middleware for Coolify deployment
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

# Email Configuration - Use environment variables
EMAIL_ENABLED = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME', '')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')
ALERT_EMAIL = os.getenv('ALERT_EMAIL', '')  # Recipient email address

# Global MQTT client variable
mqtt_client = None
mqtt_connected = False

# Pydantic model for sensor data
class SensorData(BaseModel):
    sensor_id: str = Field(..., description="Unique identifier for the sensor")
    device_id: str = Field(..., description="Unique identifier for the device")
    timestamp: datetime = Field(..., description="Timestamp of the reading")
    temp_value: float = Field(..., description="Temperature value")
    humidity: float = Field(..., description="Humidity value")

    class Config:
        json_schema_extra = {
            "example": {
                "sensor_id": "SENSOR001",
                "device_id": "DEVICE123",
                "timestamp": "2025-11-18T10:30:00",
                "temp_value": 23.5,
                "humidity": 65.2
            }
        }

# Pydantic model for sensor reading response (includes record ID)
class SensorReading(BaseModel):
    id: int = Field(..., description="Database record ID")
    sensor_id: str = Field(..., description="Unique identifier for the sensor")
    device_id: str = Field(..., description="Unique identifier for the device")
    timestamp: datetime = Field(..., description="Timestamp of the reading")
    temp_value: float = Field(..., description="Temperature value")
    humidity: float = Field(..., description="Humidity value")

# Pydantic model for threshold configuration
class ThresholdConfig(BaseModel):
    sensor_id: str = Field(..., description="Sensor ID")
    temp_min: float = Field(..., description="Minimum temperature threshold")
    temp_max: float = Field(..., description="Maximum temperature threshold")
    humidity_min: float = Field(..., description="Minimum humidity threshold")
    humidity_max: float = Field(..., description="Maximum humidity threshold")
    email: Optional[str] = Field(None, description="Email address for alerts")

    class Config:
        json_schema_extra = {
            "example": {
                "sensor_id": "SENSOR001",
                "temp_min": 32.0,
                "temp_max": 100.0,
                "humidity_min": 20.0,
                "humidity_max": 80.0,
                "email": "user@example.com"
            }
        }

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
            INSERT INTO sensor_data (sensor_id, device_id, timestamp, temp_value, humidity)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (data.sensor_id, data.device_id, data.timestamp, data.temp_value, data.humidity)
        cursor.execute(query, values)
        connection.commit()
        
        record_id = cursor.lastrowid
        cursor.close()
        connection.close()
        
        # Check thresholds and send alert if needed
        thresholds = get_sensor_thresholds(data.sensor_id)
        if thresholds:
            reading = {
                'temp_value': data.temp_value,
                'humidity': data.humidity,
                'timestamp': data.timestamp.isoformat()
            }
            violations = check_thresholds(data.sensor_id, reading, thresholds)
            
            if violations:
                print(f"⚠️ Threshold violations detected for {data.sensor_id}: {violations}")
                # Send email alert
                recipient = thresholds.get('email') or ALERT_EMAIL
                if recipient:
                    send_email_alert(data.sensor_id, violations, reading, recipient)
        
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
            SELECT id, sensor_id, device_id, timestamp, temp_value, humidity
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

def send_email_alert(sensor_id: str, violations: list, reading: dict, recipient_email: str):
    """Send email alert for threshold violations"""
    if not EMAIL_ENABLED or not ALERT_EMAIL:
        print("⚠️ Email alerts not configured. Skipping email.")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = SMTP_USERNAME
        msg['To'] = recipient_email or ALERT_EMAIL
        msg['Subject'] = f"🚨 ALERT: Sensor {sensor_id} Threshold Violation"
        
        # Create email body
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <h2 style="color: #FB7A7C;">⚠️ Sensor Alert: {sensor_id}</h2>
            <p style="font-size: 16px;">One or more thresholds have been violated.</p>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h3>Current Readings:</h3>
                <ul>
                    <li><strong>Temperature:</strong> {reading.get('temp_value', 'N/A')}°F</li>
                    <li><strong>Humidity:</strong> {reading.get('humidity', 'N/A')}%</li>
                    <li><strong>Timestamp:</strong> {reading.get('timestamp', 'N/A')}</li>
                </ul>
            </div>
            
            <div style="background-color: #fed7d7; padding: 15px; border-radius: 5px; border-left: 4px solid #FB7A7C;">
                <h3 style="color: #742a2a;">Violations Detected:</h3>
                <ul style="color: #742a2a;">
                    {''.join([f'<li>{v}</li>' for v in violations])}
                </ul>
            </div>
            
            <p style="margin-top: 20px; color: #666;">
                <em>This is an automated alert from your SensorWatch system.</em><br>
                <em>Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
            </p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Send email
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Email alert sent to {recipient_email or ALERT_EMAIL} for {sensor_id}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send email alert: {e}")
        return False

def check_thresholds(sensor_id: str, reading: dict, thresholds: dict) -> list:
    """Check if reading violates thresholds and return list of violations"""
    violations = []
    
    temp_value = reading.get('temp_value')
    humidity = reading.get('humidity')
    
    if temp_value is not None:
        if temp_value < thresholds['temp_min']:
            violations.append(f"Temperature too low: {temp_value}°F < {thresholds['temp_min']}°F")
        if temp_value > thresholds['temp_max']:
            violations.append(f"Temperature too high: {temp_value}°F > {thresholds['temp_max']}°F")
    
    if humidity is not None:
        if humidity < thresholds['humidity_min']:
            violations.append(f"Humidity too low: {humidity}% < {thresholds['humidity_min']}%")
        if humidity > thresholds['humidity_max']:
            violations.append(f"Humidity too high: {humidity}% > {thresholds['humidity_max']}%")
    
    return violations

def get_sensor_thresholds(sensor_id: str):
    """Retrieve threshold configuration for a sensor from database"""
    connection = get_db_connection()
    if connection is None:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        query = """
            SELECT sensor_id, temp_min, temp_max, humidity_min, humidity_max, email
            FROM sensor_thresholds
            WHERE sensor_id = %s
        """
        cursor.execute(query, (sensor_id,))
        result = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        return result
        
    except Error as e:
        print(f"Error retrieving thresholds for {sensor_id}: {e}")
        if connection:
            connection.close()
        return None

def save_sensor_thresholds(config: ThresholdConfig):
    """Save or update threshold configuration for a sensor"""
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO sensor_thresholds 
            (sensor_id, temp_min, temp_max, humidity_min, humidity_max, email, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
            temp_min = VALUES(temp_min),
            temp_max = VALUES(temp_max),
            humidity_min = VALUES(humidity_min),
            humidity_max = VALUES(humidity_max),
            email = VALUES(email),
            updated_at = NOW()
        """
        values = (
            config.sensor_id,
            config.temp_min,
            config.temp_max,
            config.humidity_min,
            config.humidity_max,
            config.email
        )
        cursor.execute(query, values)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        return True
        
    except Error as e:
        print(f"Error saving thresholds: {e}")
        if connection:
            connection.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        print("✅ Connected to MQTT Broker successfully!")
        # Subscribe to the sensor data topic
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"✅ Subscribed to topic: {MQTT_TOPIC}")
    else:
        mqtt_connected = False
        print(f"❌ Failed to connect to MQTT Broker. Return code: {rc}")

def on_message(client, userdata, msg):
    """Callback when a message is received from MQTT"""
    try:
        # Parse JSON payload
        payload = json.loads(msg.payload.decode())
        
        # Convert timestamp string to datetime object
        if isinstance(payload['timestamp'], str):
            payload['timestamp'] = datetime.fromisoformat(payload['timestamp'].replace('Z', '+00:00'))
        
        # Create SensorData object
        sensor_data = SensorData(**payload)
        
        # Insert into database
        record_id = insert_sensor_data(sensor_data)
        
        # Print received data to console
        print(f"\n📡 MQTT DATA RECEIVED - Record ID: {record_id}")
        print(f"   Sensor: {sensor_data.sensor_id} | Device: {sensor_data.device_id}")
        print(f"   Temp: {sensor_data.temp_value}°C | Humidity: {sensor_data.humidity}%")
        
    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON: {e}")
    except Exception as e:
        print(f"❌ Error processing MQTT message: {e}")

def on_disconnect(client, userdata, rc):
    """Callback when disconnected from MQTT broker"""
    global mqtt_connected
    mqtt_connected = False
    if rc != 0:
        print("❌ Unexpected disconnection from MQTT Broker")

def start_mqtt_client():
    """Initialize and start MQTT client"""
    global mqtt_client
    
    try:
        mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)
        
        # Set callbacks
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message
        mqtt_client.on_disconnect = on_disconnect
        
        print(f"🔌 Connecting to MQTT Broker at {MQTT_BROKER}:{MQTT_PORT}...")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        # Start the MQTT loop in a separate thread
        mqtt_client.loop_start()
        
    except Exception as e:
        print(f"❌ Failed to connect to MQTT Broker: {e}")

# Mount static files directory (if it exists)
static_path = Path("/app/static")
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Root endpoint - Serve index.html or API info
@app.get("/")
async def root():
    """Root endpoint - Serve dashboard or API info"""
    # Check if index.html exists in static folder
    index_file = Path("/app/static/index.html")
    if index_file.exists():
        return FileResponse(str(index_file))
    
    # Fallback to API info
    return {
        "status": "healthy",
        "message": "Sensor Data API is running",
        "version": "1.0.0",
        "endpoints": {
            "POST /data": "Submit single sensor data record",
            "POST /data/bulk": "Submit multiple sensor data records from JSON array",
            "GET /sensors": "List all sensors",
            "GET /sensors/{sensor_id}/readings": "Get latest readings for a sensor",
            "GET /health": "Detailed health check",
            "GET /docs": "API documentation"
        }
    }

@app.get("/health")
async def health_check():
    """Detailed health check endpoint"""
    # Check database connection
    db_status = "connected"
    try:
        connection = get_db_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
        else:
            db_status = "disconnected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy" if db_status == "connected" else "degraded",
        "database": db_status,
        "mqtt": {
            "broker": MQTT_BROKER,
            "port": MQTT_PORT,
            "connected": mqtt_connected
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/hc")
async def hc():
    """Healthcheck endpoint for Coolify"""
    print("check hc end point hit")
    return "healthy"

# POST endpoint to receive sensor data (REST API fallback)
@app.post("/data", status_code=201)
async def submit_sensor_data(data: SensorData):
    """Submit new sensor data and store in MySQL database"""
    try:
        # Insert data into database
        record_id = insert_sensor_data(data)
        
        # Print received data to console
        print(f"\n🌐 REST API DATA RECEIVED - Record ID: {record_id}")
        print(f"   Sensor: {data.sensor_id} | Device: {data.device_id}")
        print(f"   Temp: {data.temp_value}°C | Humidity: {data.humidity}%")
        
        return {
            "status": "success",
            "message": "Sensor data received and stored successfully",
            "record_id": record_id,
            "data": {
                "sensor_id": data.sensor_id,
                "device_id": data.device_id,
                "timestamp": data.timestamp.isoformat(),
                "temp_value": data.temp_value,
                "humidity": data.humidity
            }
        }
    
    except Exception as e:
        print(f"❌ Error in /data endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# POST endpoint to receive bulk sensor data from JSON file
@app.post("/data/bulk", status_code=201)
async def submit_bulk_sensor_data(data_list: List[SensorData]):
    """Submit multiple sensor data records in bulk"""
    
    if not data_list:
        raise HTTPException(status_code=400, detail="Empty data list provided")
    
    try:
        inserted_records = []
        failed_records = []
        
        print(f"\n📦 BULK DATA RECEIVED: {len(data_list)} records")
        
        for idx, data in enumerate(data_list, 1):
            try:
                # Insert data into database
                record_id = insert_sensor_data(data)
                
                inserted_records.append({
                    "record_id": record_id,
                    "sensor_id": data.sensor_id,
                    "device_id": data.device_id,
                    "timestamp": data.timestamp.isoformat()
                })
                
                print(f"   ✅ Record {idx}: ID {record_id} - {data.sensor_id}")
                
            except Exception as e:
                failed_records.append({
                    "index": idx,
                    "sensor_id": data.sensor_id,
                    "error": str(e)
                })
                print(f"   ❌ Record {idx}: FAILED - {str(e)}")
        
        print(f"📦 BULK COMPLETE: {len(inserted_records)} success, {len(failed_records)} failed")
        
        return {
            "status": "completed",
            "message": f"Bulk insert completed: {len(inserted_records)} successful, {len(failed_records)} failed",
            "summary": {
                "total_received": len(data_list),
                "successful": len(inserted_records),
                "failed": len(failed_records)
            },
            "inserted_records": inserted_records,
            "failed_records": failed_records if failed_records else None
        }
    
    except Exception as e:
        print(f"❌ Error in /data/bulk endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# GET endpoint to retrieve latest readings for a specific sensor
@app.get("/sensors/{sensor_id}/readings", response_model=List[SensorReading])
async def get_sensor_readings(
    sensor_id: str,
    limit: int = Query(default=10, ge=1, le=100, description="Number of readings to retrieve")
):
    """Get the latest N readings for a specific sensor"""
    try:
        readings = get_latest_readings(sensor_id, limit)
        
        if not readings:
            raise HTTPException(
                status_code=404, 
                detail=f"No readings found for sensor: {sensor_id}"
            )
        
        return readings
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting readings for {sensor_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# GET endpoint to retrieve list of all sensors
@app.get("/sensors", response_model=List[str])
async def list_sensors():
    """Get a list of all unique sensor IDs that have recorded data"""
    try:
        sensors = get_all_sensors()
        return sensors
    
    except Exception as e:
        print(f"❌ Error listing sensors: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# POST endpoint to save threshold configuration
@app.post("/thresholds", status_code=201)
async def save_thresholds(config: ThresholdConfig):
    """Save or update threshold configuration for a sensor"""
    try:
        save_sensor_thresholds(config)
        
        print(f"✅ Thresholds saved for {config.sensor_id}")
        print(f"   Temp: {config.temp_min}°F - {config.temp_max}°F")
        print(f"   Humidity: {config.humidity_min}% - {config.humidity_max}%")
        if config.email:
            print(f"   Alert Email: {config.email}")
        
        return {
            "status": "success",
            "message": f"Thresholds saved for {config.sensor_id}",
            "config": {
                "sensor_id": config.sensor_id,
                "temp_min": config.temp_min,
                "temp_max": config.temp_max,
                "humidity_min": config.humidity_min,
                "humidity_max": config.humidity_max,
                "email": config.email
            }
        }
    
    except Exception as e:
        print(f"❌ Error saving thresholds: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# GET endpoint to retrieve threshold configuration for a sensor
@app.get("/thresholds/{sensor_id}")
async def get_thresholds(sensor_id: str):
    """Get threshold configuration for a specific sensor"""
    try:
        thresholds = get_sensor_thresholds(sensor_id)
        
        if not thresholds:
            # Return default thresholds if none configured
            return {
                "sensor_id": sensor_id,
                "temp_min": 32.0,
                "temp_max": 100.0,
                "humidity_min": 20.0,
                "humidity_max": 80.0,
                "email": None,
                "configured": False
            }
        
        thresholds['configured'] = True
        return thresholds
    
    except Exception as e:
        print(f"❌ Error getting thresholds for {sensor_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def startup_event():
    """Initialize services when FastAPI starts"""
    print("\n" + "="*60)
    print("🚀 SENSOR DATA API SERVER STARTING")
    print("="*60)
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print("="*60)
    
    # Start MQTT client
    start_mqtt_client()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup when FastAPI shuts down"""
    global mqtt_client
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    print("🛑 Server shutdown complete")

# For local development
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=int(os.getenv('PORT', '8000')),
        log_level="info"
    )
