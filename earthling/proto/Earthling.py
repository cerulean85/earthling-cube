import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))


import grpc
import proto.EarthlingProtocol_pb2 as EarthlingProtocol_pb2
import proto.EarthlingProtocol_pb2_grpc as EarthlingProtocol_pb2_grpc
from concurrent import futures

class Earthling(EarthlingProtocol_pb2_grpc.EarthlingServicer):
    def Echo(self, request, context):
        return EarthlingProtocol_pb2.EchoResponse(message=request.message)