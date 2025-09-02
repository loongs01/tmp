import queue
import threading
import time
import random


def producer(q, producer_id):
    for i in range(5):
        task = f"Producer-{producer_id}-Task-{i}"
        time.sleep(random.uniform(0.1, 0.5))  # 模拟生产耗时
        q.put(task)
        print(f"📥 {task} 放入队列")


def consumer(q, consumer_id):
    while True:
        task = q.get()
        if task is None:  # 终止信号
            q.task_done()
            break
        time.sleep(random.uniform(0.2, 1.0))  # 模拟处理耗时
        print(f"🛠️ Consumer-{consumer_id} 处理: {task}")
        q.task_done()


# 创建队列
q = queue.Queue(maxsize=10)  # 限制队列大小

# 启动生产者线程
producers = []
for i in range(3):
    t = threading.Thread(target=producer, args=(q, i))
    t.start()
    producers.append(t)

# 启动消费者线程
consumers = []
for i in range(2):
    t = threading.Thread(target=consumer, args=(q, i))
    t.daemon = True  # 守护线程
    t.start()
    consumers.append(t)

# 等待所有生产者完成
for t in producers:
    t.join()

# 发送终止信号（每个消费者一个None）
for _ in range(len(consumers)):
    q.put(None)

# 等待队列清空
q.join()
print("🎉 所有任务处理完成！")
