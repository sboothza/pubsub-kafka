import json
import sys
from threading import Thread

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sb_serializer import HardSerializer

from .envelope_mapper import EnvelopeMapper


class PubSub(Thread):
    def __init__(
            self, serializer: HardSerializer, topic: str, group: str, servers, context=None, offset: str = "smallest"
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

    def create_topic(self, topic: str, num_partitions):
        admin_client = AdminClient({"bootstrap.servers": self.servers})
        topic_list = [NewTopic(topic, num_partitions, 1)]
        for _, future in admin_client.create_topics(topic_list).items():
            future.result(timeout=30)

    def delete_topic(self, topic: str):
        admin_client = AdminClient({"bootstrap.servers": self.servers})
        for _, future in admin_client.delete_topics([topic]).items():
            future.result(timeout=30)

    def bind(self, cls, callback):
        self.bindings[cls] = callback

    def bind_everything(self, callback):
        self.catch_all_binding = callback

    def publish(self, obj):
        json_str = self.serializer.serialize(obj, False)
        self.producer.produce(self.topic, key="key", value=json_str)

    def flush(self):
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
                    value = msg.value()
                    if value is None:
                        sys.stderr.write(
                            "%% Null value at %s [%d] offset %d, skipping\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                        continue

                    try:
                        raw_data = json.loads(value)
                    except json.JSONDecodeError as e:
                        sys.stderr.write(
                            "%% Invalid JSON at %s [%d] offset %d: %s\n"
                            % (msg.topic(), msg.partition(), msg.offset(), e)
                        )
                        continue

                    if not isinstance(raw_data, dict):
                        sys.stderr.write(
                            "%% Expected JSON object at %s [%d] offset %d, got %s\n"
                            % (msg.topic(), msg.partition(), msg.offset(), type(raw_data).__name__)
                        )
                        continue

                    if "Identifier" in raw_data:
                        identifier = raw_data["Identifier"]
                    elif "identifier" in raw_data:
                        identifier = raw_data["identifier"]
                    else:
                        continue

                    if not isinstance(identifier, str):
                        sys.stderr.write(
                            "%% Identifier must be string at %s [%d] offset %d, got %s\n"
                            % (msg.topic(), msg.partition(), msg.offset(), type(identifier).__name__)
                        )
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
