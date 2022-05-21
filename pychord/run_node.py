from pychord.node import Node
from pychord.hashing import SHA1Hasher
from pychord.rpc_server import attach_rpc
from pychord.views import attach_views

from bottle import Bottle
import threading
import socket
import time
import logging
from argparse import ArgumentParser


run_node_logger = logging.getLogger(__name__)


def build_app(address, port, db_path, remote_node=None):
    app = Bottle()
    node = Node(address, port, db_path, SHA1Hasher(), remote_addr=remote_node)
    node.initialize()
    attach_rpc(app, node)
    attach_views(app, node)
    return app, node


def background_worker(node: Node, shutdown_event: threading.Event):
    while not shutdown_event.is_set():
        run_node_logger.debug("Running background tasks...")
        node.stabilize()
        time.sleep(1)
        for _ in range(4):
            node.fix_fingers()
        time.sleep(1)
        node.check_predecessor()
        time.sleep(1)


def run_node(node_address, bind_address, port, db_path, remote_node=None):
    app, node = build_app(node_address, port, db_path, remote_node=remote_node)
    shutdown_event = threading.Event()
    t = threading.Thread(target=background_worker, args=(node, shutdown_event))
    t.start()
    try:
        run_node_logger.info("Started...")
        app.run(server="paste", host=bind_address, port=port)
    finally:
        run_node_logger.info("Shutting down..")
        shutdown_event.set()
        node.leave()
        t.join(30)


def attach_run_node(subparser: ArgumentParser):
    def func(args):
        return run_node(
            args.node_address,
            args.bind_address,
            args.port,
            args.db_path,
            remote_node=args.remote_node
        )

    subparser.set_defaults(func=func)
    subparser.add_argument("db_path", type=str)
    subparser.add_argument("-n", "--node-address", default=socket.gethostname())
    subparser.add_argument("-b", "--bind-address", default="localhost")
    subparser.add_argument("-p", "--port", type=int, default=8080)
    subparser.add_argument("--remote-node", type=str, default=None)
