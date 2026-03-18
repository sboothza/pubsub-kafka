import inspect


def envelope(klass=None, identifier=None):
    def wrap(klass):

        def build_new_methods():
            if "__envelope_built__" not in dir(klass):

                envelope_field_names = []
                for kls in klass.__mro__:
                    for name, attr in inspect.getmembers(kls):
                        if name not in envelope_field_names:
                            envelope_field_names.append(name)

                envelope_field_names.append("correlation_id")

                txt = f"setattr(klass, 'Identifier', '{identifier}')\r\n"
                exec(txt)

                field_list = [f"{f}=None" for f in envelope_field_names]
                txt = f"def __init__(self, {', '.join(field_list)}):\r\n"
                txt += f"\tsuper().__init__(correlation_id)\r\n"
                for field in envelope_field_names:
                    txt += f"\tself.{field} = {field}\r\n"

                txt += "setattr(klass, '__init__', __init__)"
                exec(txt)

                txt = f"def __str__(self):\r\n"
                field_list = [f"{{self.{f}}}" for f in envelope_field_names]
                txt += f"\treturn f'{' '.join(field_list)}'\r\n"
                txt += "setattr(klass, '__str__', __str__)"
                exec(txt)

                txt = f"EnvelopeMapper.register({klass.__name__}.Identifier, {klass.__name__})"
                exec(txt)

                setattr(klass, "__envelope_built__", True)

        build_new_methods()
        return klass

    if klass is None:
        return wrap
    return wrap(klass)
