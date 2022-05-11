from hashlib import sha1
from typing import Union

INTERVAL_SIZE = 160


class SHA1Hasher(object):
    def __init__(self, size=INTERVAL_SIZE):
        self.ring_size = size

    @property
    def max_value(self):
        return 2**self.ring_size

    def hash(self, identifier: Union[str, bytes]) -> int:
        if isinstance(identifier, str):
            identifier = identifier.encode("utf-8")
        return int(sha1(identifier).hexdigest(), 16) % self.max_value

    def _convert_to_int(self, *args):
        return tuple(self.hash(a) if isinstance(a, str) else a % self.max_value for a in args)

    def in_interval_inc(self, identifier: Union[str, int], a: Union[str, int], b: Union[str, int]) -> bool:
        a, b, identifier = self._convert_to_int(a, b, identifier)
        if a < b:
            return a < identifier <= b
        else:
            return a < identifier or identifier <= b

    def in_interval_exc(self, identifier: str, a: Union[str, int], b: Union[str, int]) -> bool:
        a, b, identifier = self._convert_to_int(a, b, identifier)
        if a < b:
            return a < identifier < b
        else:
            return a < identifier or identifier < b
