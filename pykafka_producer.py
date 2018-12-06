from pykafka import KafkaClient
import queue

host = '127.0.0.1'
client = KafkaClient(hosts="%s:9092" % host)

print(client.topics)

# Producer
log_file = open('producer_log.txt', 'w')
topic = client.topics['my.test']
with topic.get_producer(delivery_reports=True, min_queued_messages=10000, max_queued_messages=100000) as producer:
	count = 0
	while count < 100000:
		count += 1
		# print(count)
		producer.produce(('test msg '+str(count)).encode(), partition_key=str(count).encode()) # partition_key: Value used to assign this message to a particular partition.
		log_file.write('test msg '+str(count) + "\n")
		# if count % 10 ** 5 == 0:
		# 	while True:
		# 		print('123')
		# 		try:
		# 			msg, exc = producer.get_delivery_report(block=False)
		# 			if exc is not None:
		# 				print('Failed to deliver msg {}: {}'.format(msg.partition_key, repr(exc)))
		# 			else:
		# 				print('Successfully delivered msg {}'.format(msg.partition_key))
		# 		except queue.Empty:
		# 			print('456')
		# 			break
		# 	print(count)
			# break

# producer = topicdocu.get_producer()
# for i in range(4):
# 	print(i)
# 	producer.produce(bytes('test message ' + str(i**2), encoding='utf-8'))
# producer.stop()

# topic = client.topics['']