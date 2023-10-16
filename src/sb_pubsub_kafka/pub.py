from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .hard_serializer import HardSerializer


class Pub(object):
    def __init__(self, serializer: HardSerializer, topic: str, servers):
        self.serializer = serializer
        self.running = True
        conf = {"bootstrap.servers": servers}
        self.servers = servers
        self.producer = Producer(conf)
        self.topic = topic

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def create_topic(self, num_partitions):
        admin_client = AdminClient({"bootstrap.servers": self.servers})
        topic_list = []
        topic_list.append(NewTopic(self.topic, num_partitions, 1))
        admin_client.create_topics(topic_list)

    def publish(self, obj):
        json_str = self.serializer.serialize(obj, False)
        self.producer.produce(self.topic, key="key", value=json_str)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
