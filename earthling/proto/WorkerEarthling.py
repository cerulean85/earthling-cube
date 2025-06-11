import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import grpc, json
import proto.EarthlingProtocol_pb2 as EarthlingProtocol_pb2
import proto.EarthlingProtocol_pb2_grpc as EarthlingProtocol_pb2_grpc
from proto.Earthling import Earthling
from concurrent import futures
from service.Logging import log


class WorkerEarthling(Earthling):

    def __init__(self, worker):
        self.worker = worker


    def Echo(self, request, context):

        message = {
            "request": request.message,
            "response": self.worker.worker_port,
            "working": self.worker.is_working
        }

        return EarthlingProtocol_pb2.EchoResponse(message=json.dumps(message))

class WorkerEarthlingDecorator:

    def __init__(self):
        self.earthling = WorkerEarthling(self)
        self.server = None
    
    def echo(self, targetIP, targetPort, message):
        with grpc.insecure_channel(f"{targetIP}:{targetPort}") as channel:
            stub = EarthlingProtocol_pb2_grpc.EarthlingStub(channel)
            response = stub.Echo(EarthlingProtocol_pb2.EchoRequest(message=message))
        return response.message

    def serve(self, serverPort):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        EarthlingProtocol_pb2_grpc.add_EarthlingServicer_to_server(self.earthling, self.server)
        self.server.add_insecure_port(f'[::]:{serverPort}')
        self.server.start()
        self.server.wait_for_termination()
