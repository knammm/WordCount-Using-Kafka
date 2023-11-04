from __future__ import print_function
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition

WC_topic = '2153599_WordCount'
topicLog = '2153599_Log'
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

def appendWord(consumer_, array, countArray, distinctNum)
        initial_flag = 1
        for message in consumer_:
                input_word = message.value.decode('utf-8').strip("'")
                word_split = input_word.split()
                for current in word_split:
                        if initial_flag == 1:
                                initial_flag = -1
                                array.append(current)
                                countArray.append(1)
                                distinctNum += 1
                        else:
                                for i in range(len(array)):
                                        if current != array[i]:
                                                if(i == len(word) - 1):
                                                        # Case: New word, different from the last element
                                                        array.append(current_word)
                                                        countArray.append(1)
                                                        distinctNum += 1
                                                        break
                                        elif current == array[i]:
                                                # Case: Same word
                                                countArray[i] += 1
                                                break
                

                        

consumer_log = consumer_from_offset(topicLog, consumer_group, 0) # Read from the beginning
appendWord(consumer_log, word, counter, distinct_word)

producer = KafkaProducer(bootstrap_servers=[broker_uri])
# Read from the newest message
consumer = KafkaConsumer(WC_topic, group_id=consumer_group, bootstrap_servers=[broker_uri])

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
                # Send word to LogChange topic
                producer.send(topicLog, value = current_word.encode('utf-8'))

        for i in range(distinct_word):
                print("Word: %s, Counter: " % word[i], counter[i])
        print("=================================================")
