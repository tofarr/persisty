import base64

from marshy.marshaller.marshaller_abc import MarshallerABC, T


class BytesMarshaller(MarshallerABC[bytes]):
    """
    JSON doesn't really have a concept of binary data, so we encode it as a base64 string.
    """

    def __init__(self):
        super().__init__(bytes)

    def load(self, item: str) -> bytes:
        base64_bytes = item.encode("utf-8")
        raw_bytes = base64.b64decode(base64_bytes)
        return raw_bytes

    def dump(self, item: bytes) -> str:
        base64_bytes = base64.b64encode(item)
        base64_str = base64_bytes.decode("utf-8")
        return base64_str
