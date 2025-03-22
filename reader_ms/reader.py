print("i'm the reader")
import cv2
import base64
import json
import time
import threading
from fastapi import FastAPI
from kafka import KafkaProducer
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
KAFKA_BROKER_URL = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Ensure JSON is sent as bytes
)


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


def read_and_send_frames(camera_id, camera_url):
    cap = cv2.VideoCapture(camera_url)
    topic = f"frame_consumed_{camera_id}"
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            print(f"Failed to read frame from camera {camera_id}")
            break
        #print(f"success to read frame from camera {camera_id}")

        capture_time = time.time()
        # encode the frame as only binary Raw Binary (JPEG/WebP)	✅ Minimal	✅ Fastest	✅ Easy	Real-time streaming, efficient Kafka use more than base 64 encoding
        ret, buffer = cv2.imencode('.jpg', frame)
        # frame_encoded = base64.b64encode(buffer).decode('utf-8')
        if not ret:
            print("Failed to encode frame")
            continue
        message = {
            "timestamp": capture_time,
            "frame": buffer.tobytes().hex() # format of sending frame as bytes in hex
        }
        # print(f"Sending frame to topic {topic} with message {message}")

        producer.send(topic, value=message)
        print(f"success to send frame from camera {camera_id}")


        # Break the loop if the user presses 'q' to close the window
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
        # send 1 frame per second
        time.sleep(29/30)
    cap.release()


for camera_id, camera_url in camera_urls.items():
    threading.Thread(target=read_and_send_frames, args=(camera_id, camera_url), daemon=True).start()

@app.get("/")
def status():
    return {"message": "Frame reader and splitter is runing..."}
if __name__ == '__main__':
    # Start FastAPI properly
    uvicorn.run(app, host="0.0.0.0", port=8001)