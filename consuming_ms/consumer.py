import threading
import json
from fastapi import requests
from kafka import KafkaConsumer
import time
import cv2
import numpy as np
import os
import requests


KAFKA_BROKER_URL = "localhost:9092"


camera_ids = [] 


cameras = requests.get("http://127.0.0.1:8001/cameras").json()

for camera in cameras:
    camera_ids.append(camera["id"])



for i in range(10):
    print(cameras)



def consume_results(camera_id):
    """Consumes detection results from Kafka and prints them."""
    topic = f"frame_processed_{camera_id}"

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id=f"consumer_group_{camera_id}"
    )

    print(f"[*] Listening for detections on {topic}...")

    output_dir = f"camera_{camera_id}_frames"
    os.makedirs(output_dir, exist_ok=True)  # Ensure the folder exists
    for message in consumer:
        data = message.value
        print(f"[Camera {camera_id}] Time: {time.time() -data['timestamp']}, Detections: {data['detections']}, Processing Time: {data['processing_time']:.2f}s")

        frame = message.value["frame"]

        # Step 1: Convert hex string back to binary bytes
        frame_bytes = bytes.fromhex(frame)

        # Step 2: Convert binary bytes to NumPy array
        nparr = np.frombuffer(frame_bytes, np.uint8)

        # Step 3: Decode NumPy array to OpenCV image
        frame_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)


        # Save the processed frame to disk
        output_filename = os.path.join(output_dir, f"processed_frame_camera_{camera_id}_{time.time()}.jpg")
        cv2.imwrite(output_filename, frame_np)

# Start a separate thread for each consumer
threads = []
for camera_id in camera_ids:
    t = threading.Thread(target=consume_results, args=(camera_id,), daemon=True)
    t.start()
    threads.append(t)

# Keep the main thread alive
for t in threads:
    t.join()