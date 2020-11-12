from confluent_kafka import Producer
from csv import reader

my_topic = "test"

p = Producer({'bootstrap.servers': 'localhost:9092'})

with open('adult_set.csv', 'r') as csv_file:
    header = next(csv_file)
    for row in csv_file:
        if row != header:
            p.produce(my_topic, row.encode('utf-8'))

p.flush()
