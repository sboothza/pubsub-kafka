import uuid


class Envelope(object):
    Identifier: str = ""
    CorrelationId: uuid.UUID = uuid.UUID(int=0)

    def __init__(self, correlation_id: uuid.UUID = uuid.uuid4()):
        self.CorrelationId = correlation_id


class EnvelopeMapper:
    identifiers = {}

    @classmethod
    def type_from_identifier(cls, identifier: str):
        if identifier in EnvelopeMapper.identifiers:
            return EnvelopeMapper.identifiers[identifier]
        raise Exception("Unknown type")

    @classmethod
    def register(cls, identifier: str, type: type):
        EnvelopeMapper.identifiers[identifier] = type
