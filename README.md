# Processing-video-using-kafka

> SET UP
1. Khởi động máy chủ Zookeeper: .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
2. Khởi động máy chủ Kafka: .\bin\windows\kafka-server-start.bat .\config\server.properties
3. Tạo Topic mới trong môi trường Kafka:
   kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partition 1 --topic “topic_name”
4. Mở một ‘producer console’ cho phép gửi các message tới Topic trong Kafka:
   kafka-console-producer.bat --broker-list localhost:9092 --topic “topic_name”
5. Mở một “consumer console” và cho phép đọc tin nhắn từ Topic:
   kafka-console-consumer.bat --topic “topic_name” --bootstrap-server localhost:9092 --from-beginning
6. Kiểm tra các Topic đã khởi tạo:
   kafka-topics.bat --list --bootstrap-server localhost:9092
