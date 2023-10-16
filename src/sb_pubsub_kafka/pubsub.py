import json
import sys
from threading import Thread

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .envelope_mapper import EnvelopeMapper
from .hard_serializer import HardSerializer


class PubSub(Thread):
    def __init__(
            self, serializer: HardSerializer, topic: str, group: str, servers, context=None, offset:str="smallest"
    ):
        self.serializer = serializer
        self.running = True
        conf = {
            "bootstrap.servers": servers,
            "group.id": group,
            "auto.offset.reset": offset,
        }
        self.servers = servers
        self.consumer = Consumer(conf)
        self.producer = Producer(conf)
        self.consumer.subscribe([topic])
        self.bindings = {}
        self.catch_all_binding = None
        self.topic = topic
        self.context = context
        Thread.__init__(self)

    def create_topic(self, num_partitions):
        admin_client = AdminClient({"bootstrap.servers": self.servers})
        topic_list = []
        topic_list.append(NewTopic(self.topic, num_partitions, 1))
        admin_client.create_topics(topic_list)

    def bind(self, cls, callback):
        self.bindings[cls] = callback

    def bind_everything(self, callback):
        self.catch_all_binding = callback

    def publish(self, obj):
        json_str = self.serializer.serialize(obj, False)
        self.producer.produce(self.topic, key="key", value=json_str)
        self.producer.flush()

    def run(self) -> None:
        try:
            while self.running:
                msg = self.consumer.poll(timeout=0.5)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write(
                            "%% %s [%d] reached end at offset %d\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    raw_data = json.loads(msg.value())
                    if "Identifier" in raw_data:
                        identifier = raw_data["Identifier"]
                    elif "identifier" in raw_data:
                        identifier = raw_data["identifier"]
                    else:
                        continue

                    type = EnvelopeMapper.type_from_identifier(identifier)
                    try:
                        obj = self.serializer.map_to_object(raw_data, type)
                        if type in self.bindings:
                            callback = self.bindings[type]
                            callback(self, obj, self.context)
                        else:
                            if self.catch_all_binding is not None:
                                try:
                                    self.catch_all_binding(self, obj, self.context)
                                except:
                                    pass
                            else:
                                print("binding not found")
                    except Exception as e:
                        print(e)
        finally:
            self.consumer.close()

    def shutdown(self):
        self.running = False
