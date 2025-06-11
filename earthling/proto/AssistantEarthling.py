import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import grpc, threading, time, json
import earthling.proto.EarthlingProtocol_pb2 as EarthlingProtocol_pb2
import earthling.proto.EarthlingProtocol_pb2_grpc as EarthlingProtocol_pb2_grpc
from earthling.proto.Earthling import Earthling
from concurrent import futures
from earthling.service.Logging import log
from multiprocessing import Process
from earthling.service.ComWorker import WorkerPool
# from handler.earthling_dao import *

class AssistantEarthling(Earthling):

    def __init__(self, idle_count, worker_pool):
        self.idle_count = idle_count
        self.message = ''
        self.worker_pool = worker_pool
        
        self.server = None
    
    def set_idle_count(self, idle_count):
        self.idle_count.value = idle_count

    def get_idle_count(self):
        return self.idle_count.value

    def ReportIdleWorker(self, request, context):
        return EarthlingProtocol_pb2.ReportResponse(idleCount=self.get_idle_count())
    
    def NotifyTask(self, request, context):
        self.message = {
            "is_success": True,
            "err_message": ''
        }

        is_success = True
        task = { 
            "task_no": request.taskNo,
            "message": request.message
        }
        
        
        if self.idle_count.value > 0:
            message = json.loads(request.message)
            channel = message['channel'] 
            # update_state_to_finish(request.taskNo)
            self.idle_count.value = self.idle_count.value - 1
            self.worker_pool.push_task(task)
        else:
            self.message["is_success"]  = False
            self.message["err_message"] = "Can't proc.."

        return EarthlingProtocol_pb2.TaskResponse(
            idleCount = self.get_idle_count(),
            message = json.dumps(self.message))


class AssistantEarthlingDecorator:

    def __init__(self, earthling):
        self.earthling = earthling
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

    def set_idle_count(self, idle_count):
        self.earthling.set_idle_count(idle_count)

