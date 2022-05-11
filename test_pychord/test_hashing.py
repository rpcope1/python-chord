import logging
import random
import string


def test_hasher(hasher):
    addr = "localhost:8081"

    assert hasher.in_interval_inc("foo-key", addr, addr)
    assert hasher.in_interval_exc("foo-key", addr, addr)
