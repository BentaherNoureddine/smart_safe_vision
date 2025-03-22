from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
import asyncio
import json
from contextlib import asynccontextmanager
import uvicorn 

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
CAMERA_TOPICS = [f"frames_processed_{i}" for i in range(19)] 

# Dictionary to store Kafka consumers
consumers = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Kafka consumers on startup and clean up on shutdown."""
    global consumers
    try:
        print("Initializing Kafka consumers...")
        consumers = {
            topic: AIOKafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset="latest",
                group_id=f"consumer_{topic}"
            )
            for topic in CAMERA_TOPICS
        }

        print("Starting Kafka consumers...")
        for consumer in consumers.values():
            await consumer.start()

        yield
    except Exception as e:
        print(f"Error during lifespan initialization: {e}")
    finally:
        print("Stopping Kafka consumers...")
        for consumer in consumers.values():
            await consumer.stop()

app = FastAPI(lifespan=lifespan)





@app.websocket("/ws/stream/{frame_processed_id}")
async def websocket_stream(websocket: WebSocket, frame_processed_id: str):
    """WebSocket endpoint to stream video frames."""
    topic = f"frames_processed_{frame_processed_id}"
    if topic not in consumers:
        await websocket.close(reason=f"Topic {topic} not found")
        return

    consumer = consumers[topic]
    await websocket.accept()

    try:
        async for message in consumer:
            frame = message.value.get("frame", None)
            if not frame:
                continue

            # Send the frame as binary data over WebSocket
            frame_bytes = bytes.fromhex(frame)
            await websocket.send_bytes(frame_bytes)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for topic: {topic}")
    except Exception as e:
        print(f"Error streaming topic {topic}: {e}")

# Main function to run the FastAPI app
if __name__ == '__main__':
    print("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8003)