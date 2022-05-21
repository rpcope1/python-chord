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

    @property
    def hashed_local_id(self):
        return self.hasher.hash(self.local_addr)

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
            try:
                other = self.closest_preceding_node(identifier)
                if other != self.local_addr:
                    return remote_rpc(other).find_successor(identifier)
                else:
                    return self.successor
            except BaseException:
                node_logger.exception("Failed finding successor!")
                raise

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
        try:
            self.successor = remote_rpc(other_addr).find_successor(self.local_addr)
        except BaseException:
            node_logger.exception("Unable to connect to remote node and join! Aborting...")
            raise

    def leave(self):
        with self.get_conn() as conn:
            if self.successor is not None and self.successor != self.local_addr:
                bulk = db.get_all_kv_pairs(conn)
                remote_rpc(self.successor).set_local_bulk(bulk)

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
        node_id = self.hashed_local_id
        try:
            self.fingers[index] = self.find_successor(
                node_id + 2**index
            )
        except BaseException:
            node_logger.warning("Call to find successor failed, ejecting finger {0}".format(index), exc_info=True)
            self.fingers[index] = None

    @property
    def fingers_and_ids(self):
        node_id = self.hashed_local_id
        return [
            (finger, node_id + 2**i) for i, finger in enumerate(self.fingers)
        ]

    def check_predecessor(self):
        if self.predecessor and self.predecessor != self.local_addr:
            try:
                remote_rpc(self.predecessor).ping()
            except BaseException:
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

    def get_all_local(self):
        with self.get_conn() as conn:
            return db.get_all_kv_pairs(conn)

    def get(self, key):
        try:
            appropriate_node = self.find_successor(key)
            return remote_rpc(appropriate_node).get_local(key)
        except BaseException:
            node_logger.exception("Get for key failed!")
            raise

    def set_local(self, key, value):
        with self.get_conn() as conn:
            with db.transaction_wrapper(conn) as t:
                return db.set_key_value_pair(t, key, value)

    def set(self, key, value):
        try:
            appropriate_node = self.find_successor(key)
            return remote_rpc(appropriate_node).set_local(key, value)
        except BaseException:
            node_logger.exception("Failed to set key!")
            raise

    def set_local_bulk(self, bulk_dict):
        with self.get_conn() as conn:
            with db.transaction_wrapper(conn) as t:
                for k, v in bulk_dict.items():
                    db.set_key_value_pair(t, k, v)

    def remove_local(self, key):
        with self.get_conn() as conn:
            with db.transaction_wrapper(conn) as t:
                return db.remove_key(t, key)

    def remove(self, key):
        try:
            appropriate_node = self.find_successor(key)
            return remote_rpc(appropriate_node).remove_local(key)
        except BaseException:
            node_logger.exception("Failed to remove key!")
            raise

    def dump_state(self):
        return {
            "successor": self.successor,
            "predecessor": self.predecessor,
            "finger_table": self.fingers
        }

    def dump_db(self):
        with self.get_conn() as conn:
            return db.get_all_kv_pairs(conn)

    def get_local_pair_count(self):
        with self.get_conn() as conn:
            return db.get_kv_pair_count(conn)
