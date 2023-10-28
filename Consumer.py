from __future__ import print_function
from kafka import KafkaConsumer
topic = '2153599_WordCount'
broker_uri = '<broker_id>:<port>'
consumer_group = 'PYTHON_EXAMPLE'

word = []
counter = []
distinct_word = 0

def consumer_from_offset(topic, group_id, offset):
    # return the consumer from a certain offset
    consumer = KafkaConsumer(bootstrap_servers=[broker_uri], group_id=group_id)
    tp = TopicPartition(topic=topic, partition=0)
    consumer.assign([tp])
    consumer.seek(tp, offset)

    return consumer

consumer = consumer_from_offset(WC_topic, consumer_group, 0) # Read from the beginning
# Read from the newest message
# consumer = KafkaConsumer(WC_topic, group_id=consumer_group, bootstrap_servers=[broker_uri])

flag = 1
for message in consumer:
        input_message = message.value.decode('utf-8').strip("'")
        print("New message: %s" % input_message)
        words = input_message.split() # Split words
        for current_word in words:
                if flag == 1:
                        # First initialize
                        flag = -1
                        word.append(current_word)
                        counter.append(1)
                        distinct_word += 1
                else:
                        # Idea: New word -> append to list -> break
                        for i in range(len(word)):
                                if current_word != word[i]:
                                        if(i == len(word) - 1):
                                                # Case: New word, different from the last element
                                                word.append(current_word)
                                                counter.append(1)
                                                distinct_word += 1
                                                break
                                elif current_word == word[i]:
                                        # Case: Same word
                                        counter[i] += 1
                                        break
        for i in range(distinct_word):
                print("Word: %s, Counter: " % word[i], counter[i])
        print("=================================================")
