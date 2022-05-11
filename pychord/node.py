import threading
import logging
from contextlib import contextmanager
from typing import Union, List, Optional

from pychord.hashing import SHA1Hasher
from pychord import db
from pychord.rpc_client import remote_rpc


node_logger = logging.getLogger(__name__)


class Node(object):
    def __init__(self, address, port, db_path, hasher: SHA1Hasher, remote_addr: Optional[str] = None):
        self.local_addr = "{0}:{1}".format(address, port)
        self.db_path = db_path
        self.hasher = hasher
        self.lock = threading.RLock()
        self.remote_addr = remote_addr
        self.predecessor = None
        self.successor = None
        self.fingers: List[Optional[str]] = [None for _ in range(hasher.ring_size)]
        if remote_addr:
            self.fingers[0] = remote_addr
        self._current_check_finger_index = 1

    @property
    def next_finger_index(self) -> int:
        with self.lock:
            val = self._current_check_finger_index
            self._current_check_finger_index += 1
            self._current_check_finger_index %= self.hasher.ring_size
        return val

    def initialize(self):
        with self.get_conn() as conn:
            db.write_schema(conn)
        if self.remote_addr is not None:
            self.join(self.remote_addr)
        else:
            self.create()

    def find_successor(self, identifier: Union[str, int]) -> str:
        if self.hasher.in_interval_inc(identifier, self.local_addr, self.successor):
            return self.successor
        else:
            other = self.closest_preceding_node(identifier)
            return remote_rpc(other).find_successor(identifier)

    def closest_preceding_node(self, identifier: Union[str, int]) -> str:
        for i in range(self.hasher.ring_size - 1, 0, -1):
            if self.fingers[i] is not None and \
                    self.hasher.in_interval_exc(self.fingers[i], self.local_addr, identifier):
                return self.fingers[i]
        return self.local_addr

    def create(self):
        self.predecessor = None
        self.successor = self.local_addr

    def join(self, other_addr: str):
        self.predecessor = None
        self.successor = remote_rpc(other_addr).find_successor(self.local_addr)

    def stabilize(self):
        if self.successor is not None:
            if self.successor != self.local_addr:
                remote_predecessor = remote_rpc(self.successor).current_predecessor()
            else:
                remote_predecessor = self.predecessor
            if remote_predecessor and self.hasher.in_interval_exc(
                    remote_predecessor, self.local_addr, self.successor
            ) and remote_predecessor != self.successor:
                node_logger.info("Successor changed to: {0}".format(remote_predecessor))
                self.successor = remote_predecessor
            if self.successor != self.local_addr:
                remote_rpc(self.successor).notify(self.local_addr)
            else:
                self.notify(self.local_addr)

    def notify(self, other_addr: str):
        if self.predecessor is None or self.hasher.in_interval_exc(
            other_addr, self.predecessor, self.local_addr
        ):
            node_logger.info("Predecessor changed to: {0}".format(other_addr))
            self.predecessor = other_addr

    def fix_fingers(self):
        index = self.next_finger_index
        node_id = self.hasher.hash(self.local_addr)
        self.fingers[index] = self.find_successor(
            node_id + 2**index
        )

    def check_predecessor(self):
        if self.predecessor:
            try:
                remote_rpc(self.predecessor).ping()
            except:
                node_logger.warning("Predecessor unreachable.", exc_info=True)
                self.predecessor = None

    def get_predecessor(self):
        return self.predecessor

    @contextmanager
    def get_conn(self):
        conn = db.open_conn(self.db_path)
        yield conn
        conn.close()

    def has_local_key(self, key):
        with self.get_conn() as conn:
            return db.does_key_exist(conn, key)

    def get_local_key(self, key, default=None):
        with self.get_conn() as conn:
            return db.get_value_by_key(conn, key, default=default)

    def get(self, key):
        appropriate_node = self.find_successor(key)
        return remote_rpc(appropriate_node).get_local(key)

    def set_local(self, key, value):
        with self.get_conn() as conn:
            with db.transaction_wrapper(conn) as t:
                return db.set_key_value_pair(t, key, value)

    def set(self, key, value):
        appropriate_node = self.find_successor(key)
        return remote_rpc(appropriate_node).set_local(key, value)

    def remove_local(self, key):
        with self.get_conn() as conn:
            with db.transaction_wrapper(conn) as t:
                return db.remove_key(t, key)

    def remove(self, key):
        appropriate_node = self.find_successor(key)
        return remote_rpc(appropriate_node).remove_local(key)

    def dump_state(self):
        return {
            "successor": self.successor,
            "predecessor": self.predecessor,
            "finger_table": self.fingers
        }

    def dump_db(self):
        with self.get_conn() as conn:
            return db.get_all_kv_pairs(conn)
