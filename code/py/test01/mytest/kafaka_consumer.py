from confluent_kafka import Consumer, KafkaException
import json

# 配置 Kafka 服务器和 Consumer Group
conf = {
    'bootstrap.servers': '192.168.231.131:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'  # 从最早的消息开始读取
}

# 创建消费者
consumer = Consumer(conf)

# 订阅 Topic
consumer.subscribe(['test-topic'])

# 消费消息
print("Waiting for messages...")
try:
    while True:
        msg = consumer.poll()  # 等待 1 秒获取消息
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(f"Received: {msg.key()}")
        print(f"Received: {msg.value()}")
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()