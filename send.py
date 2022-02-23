import pika
import cv2
import numpy as np
import sys
import time
import threading
import queue

# rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='video-frames')

# bufferless VideoCapture
class VideoCapture:
    def __init__(self, name):
        self.cap = cv2.VideoCapture(name)
        self.q = queue.Queue()
        t = threading.Thread(target=self._reader)
        t.daemon = True
        t.start()

    # read frames as soon as they are available, keeping only most recent one
    def _reader(self):
        while True:
            ret, frame = self.cap.read()
            if not ret:
                break
            if not self.q.empty():
                try:
                    self.q.get_nowait()   # discard previous (unprocessed) frame
                except queue.Empty:
                    pass
            self.q.put(frame)

    def read(self):
        return self.q.get()

RTSPUrl = sys.argv[1]


channel.queue_declare(queue='video-frames-' + str(RTSPUrl))

print("Using camera at " + RTSPUrl)


cap = VideoCapture("rtsp://cgroge:mnr0spr2@"+RTSPUrl+":554/h264Preview_01_main")
while True:
    
    time.sleep(5)   # simulate time between events
    frame = cap.read()
    image = cv2.resize(frame,None,fx=0.4,fy=0.4)  # 1024 x 768

    send_data = cv2.imencode('.jpg', image)[1].tobytes()
    properties = pika.BasicProperties(message_id=RTSPUrl)

    channel.basic_publish(exchange='',
                    properties=properties,
                    routing_key='video-frames',
                    body=send_data)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    print("Sent::frame data from " + str(RTSPUrl) + " at " + str(timestr))
    
    if chr(cv2.waitKey(1)&255) == 'q':
        break


cap.release()

connection.close()



