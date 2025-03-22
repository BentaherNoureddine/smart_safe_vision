from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer
import json
import threading
import asyncio
import base64
import uvicorn

app = FastAPI()
KAFKA_BROKER_URL = "localhost:9092"

# Dictionary to store active WebSocket connections
active_connections = []


class ConnectionManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()


def kafka_consumer():
    """Consumes processed detections from Kafka and broadcasts them via WebSocket."""
    topics = [f"camera_{i}_detections" for i in range(6)]  # Adjust range as needed
    consumer = KafkaConsumer(*topics, bootstrap_servers=KAFKA_BROKER_URL,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                             auto_offset_reset='latest',
                             group_id="websocket_consumer",
                             enable_auto_commit=True)

    for message in consumer:
        data = message.value
        print(f"Received from Kafka: {data}")

        # Convert frame from hex to base64 for web compatibility
        if "frame" in data:
            data["frame"] = base64.b64encode(bytes.fromhex(data["frame"])).decode('utf-8')

        # Broadcast message to all WebSocket clients
        if manager.active_connections:
            asyncio.run(manager.broadcast(json.dumps(data)))


# Start Kafka consumer in a separate thread
threading.Thread(target=kafka_consumer, daemon=True).start()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # Keeping the connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/")
def status():
    return {"message": "Frame processor is running...."}

if __name__ == '__main__':
    # Start FastAPI properly
    uvicorn.run(app, host="0.0.0.0", port=8000)