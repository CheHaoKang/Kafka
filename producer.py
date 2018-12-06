import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
i = 0
while True:
	ts = int(time.time() * 1000)
	producer.send("test", key=bytes(str(i), encoding='utf-8'), value=bytes(str(i), encoding='utf-8'))
	producer.flush()
	print(i)
	i += 1
	time.sleep(1)
