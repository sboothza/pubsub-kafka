# My Kafka PubSub Library

Wraps kafka into nice little simple classes  
One for publishing `pub` and one for both publish and subscribe `pubsub`  
The system transfers messages wrapped in an independent json format.  
This is the structure of a sample envelope:  
```
class TestEnvelope(Envelope):
    Identifier = "TestEnvelope"
    msg: str

    def __init__(self, msg: str = "", correlation_id: uuid.UUID = uuid.uuid4()):
        super().__init__(correlation_id)
        self.msg = msg

    def __str__(self):
        return f"msg:{self.msg}"
```
The envelope then needs to be registered with `EnvelopeMapper.register(TestEnvelope.Identifier, TestEnvelope)`   
This allows the subscriber to correctly identify and deserialize the envelope  
The `correlation_id` is passed through automatically with each message, but it is up to you to use correctly - 
ie.  Assign the same id to a response message, etc. 

The `pubsub` class is the most complex, it has an internal thread that polls for messages and invokes the correct 
callback based on the envelope type.  
The bindings are done as follows: `pubsub.bind(TestEnvelope, callback)`  
Where the callback is a static function that looks like this: `callback(pubsub, obj, context)`
`pubsub` is the instance that raised the callback, `obj` is the deserialized envelope, and `context` is whatever you passed
into the `context` parameter in `__init__()`  
This allows you to create specific bindings for each envelope type.  
There is a catch-all binding `pubsub.bind_everything(callback)` that maps to everything that doesn't have a specific binding.  

**Note** that all messages need to follow the envelope pattern, or they won't work at all.

The message pattern is so that it can be used language-agnostically - the same library exists in C#, and coexists with the python libraries.


## Building
`python -m build `

## Deploying
`python -m twine upload --repository testpypi dist/*`