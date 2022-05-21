from bottle import Bottle, static_file, SimpleTemplate, json_dumps, response
import logging
import datetime

from pychord.node import Node
from pychord import STATIC_FILES_DIR


views_logger = logging.getLogger(__name__)

start_time = datetime.datetime.now()

index_template = SimpleTemplate(
    """
<html>
    <head>
        <title>Pychord Node</title>
        <!-- Stolen from: http://bettermotherfuckingwebsite.com/ -->
        <style>
body{margin:40px
auto;max-width:650px;line-height:1.6;font-size:18px;color:#444;padding:0
10px}h1,h2,h3{line-height:1.2}
        </style>
    </head>
    <body>
        <h1>Node {{ node.local_addr }}</h1>
        <h2>Stats:</h2>
        <ul>
            <li>Start Time: {{ start_time }}</li>
            <li>Uptime: {{ uptime }}</li>
            <li>Predecessor: {{ node.predecessor }}</li>
            <li>Successor: {{ node.successor }}</li>
            <li>Local K/V count: {{ node.get_local_pair_count() }}</li>
            <li>Hashed ID: {{ node.hashed_local_id }}</li>
        </ul>
        <h2>Fingers:</h2>
        <ul>
        % for finger, ident in node.fingers_and_ids:
            <li>{{ finger }} ({{ident}})</li>
        % end
        </ul>
    </body>
</html>
    """
)


def attach_views(app: Bottle, node: Node):
    @app.route("/")
    def index_view():
        return index_template.render(
            node=node,
            start_time=start_time,
            uptime=(datetime.datetime.now() - start_time).total_seconds(),
        )

    @app.route("/db-dump")
    def db_dump():
        response.content_type = "application/json"
        return json_dumps(node.get_all_local())

    @app.route("/static/<fname:path>")
    def static_file_handler(fname):
        return static_file(fname, STATIC_FILES_DIR)
