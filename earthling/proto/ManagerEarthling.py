import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import grpc
import earthling.proto.EarthlingProtocol_pb2 as EarthlingProtocol_pb2
import earthling.proto.EarthlingProtocol_pb2_grpc as EarthlingProtocol_pb2_grpc
from earthling.proto.Earthling import Earthling
from concurrent import futures
from earthling.service.Logging import log

class ManagerEarthling(Earthling):
    def ReportIdleWorker(self, request, context):
        pass
    
    def NotifyTask(self, request, context):
        pass

class ManagerEarthlingDecorator:

    def __init__(self):
        self.earthling = ManagerEarthling()
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

    def getIdleWorkerCount(self, targetIP, targetPort):
        with grpc.insecure_channel(f"{targetIP}:{targetPort}") as channel:
            stub = EarthlingProtocol_pb2_grpc.EarthlingStub(channel)
            response = stub.ReportIdleWorker(EarthlingProtocol_pb2.ReportRequest())
        return response.idleCount
    
    def notifyTaskToAss(self, targetIP, targetPort, task_no, message, queue_name=''):
        with grpc.insecure_channel(f"{targetIP}:{targetPort}") as channel:
            stub = EarthlingProtocol_pb2_grpc.EarthlingStub(channel)
            response = stub.NotifyTask(EarthlingProtocol_pb2.TaskRequest(taskNo = task_no, message = message, queueName = queue_name))
        return response