from confluent_kafka import Consumer

header = ["x","age","workclass","fnlwgt","education","educational_num","marital_status","occupation","relationship","race","gender","capital_gain","capital_loss","hours_per_week","native_country","income"]
my_topic = "test"

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'testing',
    'auto.offset.reset': 'earliest'
})

c.subscribe([my_topic])

number_of_rows = 4#48842
cont = 0
batch_size = 2
batch = []

while True and cont < number_of_rows:
    msg = c.poll(1.0)
    if msg is None:
        continue
    elif msg.error():
        print("Consumer error: {0}".format(msg.error()))
        continue
    else:
        msg_val = msg.value().decode('utf-8').strip().split(",")
        print(msg_val)
        batch.append(msg_val)
        if len(batch) == 2:
            print(batch)
            #predictions(batch)
            batch = []
    cont += 1

c.close()


def predictions(data):
    return None
