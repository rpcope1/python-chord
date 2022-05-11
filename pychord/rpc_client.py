from tinyrpc.client import RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport


def build_http_rpc_client(addr):
    client = RPCClient(
        JSONRPCProtocol(),
        HttpPostClientTransport("http://{0}".format(addr))
    )
    proxy = client.get_proxy()
    return client, proxy


def remote_rpc(addr):
    _, p = build_http_rpc_client(addr)
    return p
