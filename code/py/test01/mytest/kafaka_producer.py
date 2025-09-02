from confluent_kafka import Producer
import json
import time

# 配置 Kafka 服务器
conf = {'bootstrap.servers': '192.168.231.131:9092'}

# 创建生产者
producer = Producer(conf)

# 发送消息
for i in range(20):
    message = {"number": i, "message": f"Hello Kafka {i}"}
    producer.produce(
        'test-topic',
        key=str(i)+'-key',
        value=json.dumps(message)
    )
    print(f"Sent: {message}")
    time.sleep(1)

# 确保所有消息都已发送
producer.flush()