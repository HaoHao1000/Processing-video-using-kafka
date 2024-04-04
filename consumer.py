import cv2
from os.path import join
from kafka import KafkaProducer, KafkaConsumer
import numpy as np
import threading

def consume_video_from_kafka(consumer, topic):
    for message in consumer:
        frame_data = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv2.imdecode(frame_data, cv2.IMREAD_COLOR)

        cv2.imshow(topic, frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

def consume_video_from_kafka2(consumer, topic):
    last_gray = None
    kernel = None
    backgroundObject = cv2.createBackgroundSubtractorMOG2(detectShadows=True)
    for message in consumer:
        frame_data = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv2.imdecode(frame_data, cv2.IMREAD_COLOR)
        gray_frame = cv2.GaussianBlur(frame, (5,5), 0)

        if last_gray is None:
            last_gray = gray_frame
            continue

        diff = cv2.absdiff(gray_frame, last_gray)

        _, difference = cv2.threshold(diff, 25, 255, cv2.THRESH_BINARY)

        foreground_mask = backgroundObject.apply(difference)
        foreground_mask = cv2.erode(foreground_mask, kernel, iterations = 1)
        foreground_mask = cv2.dilate(foreground_mask, kernel, iterations=2)

        contours, _ = cv2.findContours(foreground_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        # loop over each contour found in the frame.
        for con in contours:
            # Accessing the x, y and height, width of the cars
            x, y, width, height = cv2.boundingRect(con)

            # Here we will be drawing the bounding box on the cars
            cv2.rectangle(difference, (x, y), (x + width, y + height), (0, 0, 255), 1)


        last_gray = gray_frame

        cv2.imshow(topic, difference)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

def main():
    topic1 = 'topic1'
    topic2 = 'topic2'

    consumer1 = KafkaConsumer(
        topic1,
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10)
    )

    consumer2 = KafkaConsumer(
        topic2,
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10)
    )

    #consumer_thread1 = threading.Thread(target=consume_video_from_kafka, args=(consumer1, topic1))
    consumer_thread2 = threading.Thread(target=consume_video_from_kafka2, args=(consumer2, topic2))

    #consumer_thread1.start()
    consumer_thread2.start()

    #consumer_thread1.join()
    consumer_thread2.join()

if __name__ == "__main__":
    main()