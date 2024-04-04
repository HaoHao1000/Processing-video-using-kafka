import cv2
from kafka import KafkaProducer
import threading

#Khoi tao ham doc video, gui thong diep len topic
def publish_video_to_kafka(producer, topic, video_path):
    cap = cv2.VideoCapture(video_path)

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        #Convert frame to bytes
        _, buffer = cv2.imencode('.jpg', frame)
        data = buffer.tobytes()

        #Publish frame to Kafka topic
        producer.send(topic, value= data)

    cap.release()

def main():
    #Configure Kafka producer
    bootstrap_servers = 'localhost:9092'
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        metadata_max_age_ms=600000,
        api_version = (0,10,1)

    )

    #Create and run Kafka producer for topic 1 in a separate thread
    topic1 = 'topic1'
    video_path1 = 'road.mp4'
    producer_thread1 = threading.Thread(target=publish_video_to_kafka, args=(producer, topic1, video_path1))
    producer_thread1.start()

    #Create and run Kafka producer for topic2 in a separate thread
    topic2 = 'topic2'
    video_path2 = 'road.mp4'
    producer_thread2 = threading.Thread(target=publish_video_to_kafka, args=(producer, topic2, video_path2))
    producer_thread2.start()

    producer_thread1.join()
    producer_thread2.join()

if __name__ == "__main__":
    main()