from pykafka import KafkaClient

host = '127.0.0.1'
client = KafkaClient(hosts="%s:9092" % host)

print(client.topics)

topic = client.topics['my.test']

consumer = topic.get_simple_consumer()
# balanced_consumer = topic.get_balanced_consumer(
# 					consumer_group='testgroup',
# 					auto_commit_enable=True)

log_file = open('log.txt', 'w')
for message in consumer:
# for message in balanced_consumer:
	if message is not None:
		log_file.write(str(message.offset) + str(message.value))
		# print(message.offset, message.value)
log_file.close()