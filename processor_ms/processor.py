import cv2
from ultralytics import YOLO
import json
import time
import torch
import threading
import psutil
import numpy as np
from fastapi import FastAPI
import requests
from kafka import KafkaConsumer, KafkaProducer
import uvicorn




app = FastAPI()


#KAFKA CONFIG

KAFKA_BROKER_URL = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)



# Fetch cameras from the reader service

cameras = requests.get("http://127.0.0.1:8001/cameras").json()




online_cameras= cameras

# select the device to use

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


    
# Load the YOLO model
model = YOLO('yolov8s').to(device)


# Function to process frames

def process_frames(camera_id):
    " Consumes frames from the Kafka topic , processes them using YOLOv8 and publishes results . "
    topic_in = f"frame_consumed_{camera_id}"
    topic_out = f"frames_processed_{camera_id}"

    consumer = KafkaConsumer(topic_in, bootstrap_servers=KAFKA_BROKER_URL,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                             auto_offset_reset='latest',
                             group_id=f"processor_group_{camera_id}")

    for message in consumer:
        capture_time = message.value["timestamp"]
        frame = message.value["frame"]
        
        

        # Step 1: Convert hex string back to binary bytes
        frame_bytes = bytes.fromhex(frame)

        # Step 2: Convert binary bytes to NumPy array
        nparr = np.frombuffer(frame_bytes, np.uint8)

        # Step 3: Decode NumPy array to OpenCV image
        frame_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        processing_start_time = time.time()

        # Yolo inference
        results = model(frame_np)

        processing_end_time = time.time()
        torch.cuda.empty_cache()

        # compute processing time (latency)
        processing_time = processing_end_time - processing_start_time

        print(f"Processing time for camera {camera_id} is: {processing_time:.2f}s")
        print(torch.cuda.memory_allocated() / 1024 ** 2 if device.type == "cuda" else None)

        # extract detections
        for r in results:
            names = [r.names[cls.item()] for cls in r.boxes.cls.int()]
            # Draw bounding boxes on the frame
            for box in r.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())  # Get the bounding box coordinates
                label = r.names[int(box.cls)]  # Get the label of the detected object
                confidence = box.conf[0]  # Get the confidence of the detection

                # Draw rectangle and label on the frame
                frame_np = cv2.rectangle(frame_np, (x1, y1), (x2, y2), (0, 255, 0), 2)
                frame_np = cv2.putText(frame_np, f"{label} {confidence:.2f}", (x1, y1 - 10),
                                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)



        # detections = r.boxes.cls.tolist()
        print(f"Detected objects for camera {camera_id} are: {names}")

        # encode the frame as only binary Raw Binary (JPEG/WebP)	✅ Minimal	✅ Fastest	✅ Easy	Real-time streaming, efficient Kafka use more than base 64 encoding
        ret, buffer = cv2.imencode('.jpg', frame_np)
        # frame_encoded = base64.b64encode(buffer).decode('utf-8')
        if not ret:
            print("Failed to encode frame")
            continue

        # publish detections to Kafka topic
        producer.send(topic_out, value=json.dumps({
            "camera_id": camera_id,
            "frame": buffer.tobytes().hex(),
            "timestamp": capture_time,
            "detections": names,
            "processing_time": processing_time
        }).encode('utf-8'))




# start a processor for each camera
for camera in cameras:
    t = threading.Thread(target=process_frames, args=(camera["id"],), daemon=True).start()



# start processing a new camera
@app.get("/process/{camera_id}")
def process(camera_id: int):
    threading.Thread(target=process_frames, args=(camera_id,), daemon=True).start()
    return {"message": f"Processing started for camera {camera_id}"}




@app.get("/")
def status():
    return {"message": "Frame processor is running...."}


print(torch.version.cuda)

if __name__ == '__main__':
    # Start FastAPI properly
    uvicorn.run(app, host="0.0.0.0", port=8002)
