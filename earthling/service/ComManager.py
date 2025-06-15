"""
pip3 install grpcio
pip3 install grpcio-tools
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./proto/EarthlingProtocol.proto
# After generating ProtoBuf, adjust the package path if needed => modify EarthlingProtocol_pb2_grpc.py to use from proto..
"""

import random
import os, sys

from earthling.query import QueryPipeTask, get_query_pipe_task_instance

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import time, json
from earthling.proto.ManagerEarthling import ManagerEarthlingDecorator

from multiprocessing import Process
from earthling.service.Com import Com
from earthling.service.Logging import log


class ComManager(Com):

    def __init__(self):
        super().__init__()
        self.decorator = ManagerEarthlingDecorator()

    def serve(self):
        compose = self.monitor.get_compose()["manager"]
        port = compose["port"]
        self.decorator.serve(port)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        EarthlingProtocol_pb2_grpc.add_EarthlingServicer_to_server(Earthling(), server)
        server.add_insecure_port(f"[::]:{port}")
        server.start()
        server.wait_for_termination()

    def loop(self):
        print("ComManager loop Started.")
        base_q_inst = QueryPipeTask()
        while True:
            time.sleep(5)
            result = base_q_inst.search_pending_task()
            for message in result:
                task_no = message["id"]
                assistant = self.monitor.get_compose()["assistant"] or []
                random.shuffle(assistant)
                for ass in assistant:
                    ass_addr, ass_port = ass["address"], ass["port"]

                    try:
                        idle_count = self.decorator.getIdleWorkerCount(
                            ass_addr, ass_port
                        )
                        print(f"idel_count: {idle_count}")
                        time.sleep(1)  # Delay to limit traffic
                    except Exception as err:
                        print(err)
                        print(
                            f"Cannot connect to Assistant server [{ass_addr}:{ass_port}]. (1)"
                        )
                        idle_count = 0
                        time.sleep(5)

                    if idle_count > 0:
                        task_type = message["task_type"]
                        print(f"Requesting task-{task_type}-{task_no} to Assistant Domain: {ass_addr}:{ass_port}")
                        try:
                            if message["current_state"] == "pending":                                
                                q_task_inst = get_query_pipe_task_instance(task_type)
                                q_task_inst.update_state_to_start(task_no, ass_addr)

                                result = self.decorator.notifyTaskToAss(ass_addr, ass_port, task_no, json.dumps(message, default=str))
                                print(">> notified task to assistan")

                                result_message = json.loads(result.message)
                                is_success = result_message["is_success"]
                                err_message = result_message["err_message"]

                                if not is_success:
                                    q_task_inst.update_state_to_wait(task_no)
                                break
                        except Exception as err:
                            print(err)
                            print(
                                f"Cannot connect to Assistant server [{ass_addr}:{ass_port}]. (2)"
                            )
                            time.sleep(5)
                        break
                time.sleep(5)  # This delay is required


def run():
    mng = ComManager()
    p = Process(target=mng.loop, args=())
    p.start()

    compose = mng.monitor.get_compose()["manager"]
    addr, port = compose["address"], compose["port"]
    print(f"Started Manager Server From {addr}:{port}")
    mng.serve()


if __name__ == "__main__":
    run()
