import cv2
from ultralytics import YOLO
import json
import time
import torch

import threading
import psutil
import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import logging

from kafka import KafkaConsumer, KafkaProducer
import uvicorn

app = FastAPI()
KAFKA_BROKER_URL = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)


camera_urls = {
    0: 0,
    1: "vid1.mp4",
    2: "rtsp://192.168.1.12:8080/h264_ulaw.sdp",
    3: "rtsp://192.168.1.12:8080/h264_ulaw.sdp",
    4: "rtsp://192.168.1.12:8080/h264.sdp",
    5: "vid9.mp4",
    6: "vid6.mp4",
    7: "vid7.mp4",
    8: "vid8.mp4",
    9: "vid9.mp4",
}

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

for i in range(10):
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")
    print(f"CUDA device count: {torch.cuda.device_count()}")
    print(f"CUDA device name: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'No GPU'}")
    print(f"CUDA version: {torch.version.cuda}")


@app.get("/device")
def get_device():
    for i in range(10):
     return {"device:":str(device)}
     
print(torch.cuda.is_available())   # Should return True
print(torch.cuda.device_count())   # Should be > 0
print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else "No GPU")

get_device()     

    



model = YOLO('yolov8s').to(device)


def get_ressource_usage():
    return {
        "cpu": psutil.cpu_percent(),
        "memory": psutil.virtual_memory().percent,
        "gpu": torch.cuda.memory_allocated() / 1024 ** 2 if device == "cuda" else None
    }


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
for camera_id in camera_urls:
    t = threading.Thread(target=process_frames, args=(camera_id,), daemon=True).start()


for i in range(10):
    print(device)



@app.get("/")
def status():
    return {"message": "Frame processor is running...."}


print(torch.version.cuda)

if __name__ == '__main__':
    # Start FastAPI properly
    uvicorn.run(app, host="0.0.0.0", port=8002)
