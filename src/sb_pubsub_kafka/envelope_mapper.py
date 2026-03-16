import uuid


class Envelope(object):
    Identifier: str = ""
    CorrelationId: uuid.UUID = uuid.UUID(int=0)

    def __init__(self, correlation_id: uuid.UUID = None):
        if not correlation_id:
            correlation_id = uuid.uuid4()
        self.CorrelationId = correlation_id


class EnvelopeMapper:
    identifiers = {}

    @classmethod
    def type_from_identifier(cls, identifier: str):
        if not isinstance(identifier, str):
            raise TypeError("identifier must be str, got %s" % type(identifier).__name__)
        if identifier in EnvelopeMapper.identifiers:
            return EnvelopeMapper.identifiers[identifier]
        raise ValueError("Unknown type: %r" % identifier)

    @classmethod
    def register(cls, identifier: str, type: type):
        EnvelopeMapper.identifiers[identifier] = type
