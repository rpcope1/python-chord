from tinyrpc.client import RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

from pychord.constants import JSON_RPC_SUBURL


def build_http_rpc_client(addr):
    client = RPCClient(
        JSONRPCProtocol(),
        HttpPostClientTransport("http://{0}{1}".format(addr, JSON_RPC_SUBURL))
    )
    proxy = client.get_proxy()
    return client, proxy


def remote_rpc(addr):
    _, p = build_http_rpc_client(addr)
    return p
