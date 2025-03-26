import cv2
import json
import time
import threading
import requests
from fastapi import Depends, FastAPI, HTTPException
from kafka import KafkaProducer
from requests import Session
import uvicorn
from database.database import get_db
from model.camera import Camera




app = FastAPI()



KAFKA_BROKER_URL = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Ensure JSON is sent as bytes
)


# Fetch cameras function inside startup event
@app.on_event("startup")
def start_camera_threads():
    db = next(get_db())  # Get DB session
    cameras = db.query(Camera).all()  # Fetch cameras using the session
    for camera in cameras:
        threading.Thread(target=read_and_send_frames, args=(camera.id, camera.url), daemon=True).start()




def read_and_send_frames(camera_id, camera_url):
    cap = cv2.VideoCapture(camera_url)
    topic = f"frame_consumed_{camera_id}"
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            print(f"Failed to read frame from camera {camera_id}")
            break
        capture_time = time.time()
        ret, buffer = cv2.imencode('.jpg', frame)
        if not ret:
            print("Failed to encode frame")
            continue
        message = {
            "timestamp": capture_time,
            "frame": buffer.tobytes().hex(),
            "camera_id": camera_id
        }

        producer.send(topic, value=message)
        print(f"success to send frame from camera {camera_id}")

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
        time.sleep(29/30)
    cap.release()











#______________________________________ENDPOINTS______________________________________#




# get all cameras (FastAPI route)
@app.get("/cameras")
def get_cameras(db: Session = Depends(get_db)):
    cameras = db.query(Camera).all()
    return cameras




# get camera by id
@app.get("/camera/{camera_id}")
def get_camera(camera_id: int, db: Session = Depends(get_db)):
    camera = db.query(Camera).filter(Camera.id == camera_id).first()
    if not camera:
        return {"error": "Camera not found"}
    return camera




# delete camera by id
@app.delete("/camera/{camera_id}")
def delete_camera(camera_id: int, db: Session = Depends(get_db)):
    camera = db.query(Camera).filter(Camera.id == camera_id).first()
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found")
    db.delete(camera)
    db.commit()
    return {"message": "Camera deleted successfully"}






# create new camera
@app.post("/camera/create")
def create_camera(url: str, db: Session = Depends(get_db)):
    camera = db.query(Camera).filter(Camera.url == url).first()
    if camera:
        raise HTTPException(status_code=409, detail="Camera already exists")
    new_camera = Camera(url=url)
    db.add(new_camera)
    db.commit()
    return {"message": "Camera created successfully"}




#start new camera
@app.get("/camera/{camera_id}/start")
def start_camera(camera_id: int, db: Session = Depends(get_db)):
    camera = db.query(Camera).filter(Camera.id == camera_id).first()
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found")
    threading.Thread(target=read_and_send_frames, args=(camera.id, camera.url), daemon=True).start()
    time.sleep(2)
    response = requests.get(f"http://127.0.0.1:8002/process/{camera_id}")
    
    for i in range(10):
        print("RESPONSE :",response.status_code)
        
    return {"message": "Camera started successfully"}









@app.get("/")
def status():
    return {"message": "Frame reader and splitter is running..."}

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8001)
