from bottle_tinyrpc import TinyRPCPlugin
from bottle import Bottle
import logging

from pychord.node import Node


rpc_server_logger = logging.getLogger(__name__)


def attach_rpc(app: Bottle, node: Node):
    rpc_plugin = TinyRPCPlugin("/")
    app.install(rpc_plugin)

    @rpc_plugin.public
    def ping():
        return "pong"

    @rpc_plugin.public
    def find_successor(identifier):
        rpc_server_logger.info("Finding successor for {0}".format(identifier))
        wat = node.find_successor(identifier)
        rpc_server_logger.info("Successor: {0}".format(wat))
        return wat

    @rpc_plugin.public
    def current_predecessor():
        return node.get_predecessor()

    @rpc_plugin.public
    def notify(other_addr):
        return node.notify(other_addr)

    @rpc_plugin.public
    def closest_preceding_node(identifier):
        rpc_server_logger.info("Finding closest preceding node for {0}".format(identifier))
        preceding_node = node.closest_preceding_node(identifier)
        rpc_server_logger.info("Preceding node: {0}".format(preceding_node))
        return preceding_node

    @rpc_plugin.public
    def has_local_key(key):
        rpc_server_logger.info("Searching for local key: {0}".format(key))
        return node.has_local_key(key)

    @rpc_plugin.public
    def get_local(key):
        rpc_server_logger.info("Retrieving local key: {0}".format(key))
        val = node.get_local_key(key)
        rpc_server_logger.info("Value: {0}".format(val))
        return val

    @rpc_plugin.public
    def get(key):
        rpc_server_logger.info("Retrieving key: {0}".format(key))
        return node.get(key)

    @rpc_plugin.public
    def set_local(key, value):
        rpc_server_logger.info("Setting local key/value pair: {0}/{1}".format(key, value))
        return node.set_local(key, value)

    @rpc_plugin.public
    def set(key, value):
        rpc_server_logger.info("Setting key/value pair: {0}/{1}".format(key, value))
        return node.set(key, value)

    @rpc_plugin.public
    def remove_local(key):
        rpc_server_logger.info("Removing local key: {0}".format(key))
        return node.remove_local(key)

    @rpc_plugin.public
    def remove(key):
        rpc_server_logger.info("Removing key: {0}".format(key))
        return node.remove(key)

    @rpc_plugin.public
    def dump_state():
        return node.dump_state()

    @rpc_plugin.public
    def dump_db():
        return node.dump_db()
