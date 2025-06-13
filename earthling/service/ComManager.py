
'''
pip3 install grpcio
pip3 install grpcio-tools
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./proto/EarthlingProtocol.proto
# ProtoBuf 생성 후 패키지 경로 조정이 필요함 => EarthlingProtocol_pb2_grpc.py from proto.. 로 수정하기
'''

import random
import os, sys

from earthling.query import select_wait_task, update_state_to_start, update_state_to_wait

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
    compose = self.monitor.get_compose()['manager']
    port = compose['port']
    self.decorator.serve(port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    EarthlingProtocol_pb2_grpc.add_EarthlingServicer_to_server(Earthling(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()

  def loop(self):
    print("ComManager loop Started.")
    while True:
      result = select_wait_task()
      # print(result)
      for message in result:
        # print(message)
        site = message['site']
        task_no = message['id']
        assistant = self.monitor.get_compose()['assistant']
        assistant = assistant if assistant is not None else []
        random.shuffle(assistant)
        for ass in assistant:
          ass_addr, ass_port = ass['address'], ass['port']

          try:
            idle_count = self.decorator.getIdleWorkerCount(ass_addr, ass_port)
            time.sleep(1)  # 트래픽 제한을 위한 딜레이
          except Exception as err: 
            # print(err)
            print(f"Assistant 서버[{ass_addr}:{ass_port}]를 연결할 수 없습니다. (1)")
            idle_count = 0
            time.sleep(5)

          if idle_count > 0:
            print(f"Request task-{task_no} to Assistant Domain: {ass_addr}:{ass_port}")
            try:
              print(message)
              channel = message['channel']
              data_exec = message['state']
              if data_exec == 'pending':
                  data_desc = { "site": site, "channel": channel, "state": data_exec }
                  update_state_to_start(task_no, ass_addr)          
                  result = self.decorator.notifyTaskToAss(ass_addr, ass_port, task_no, json.dumps(data_desc))
                  result_message = json.loads(result.message)
                  is_success = result_message["is_success"]
                  err_message = result_message["err_message"]

                  if not is_success:
                      update_state_to_wait(task_no)
                  break
            except Exception as err: 
                print(err)
                print(f"Assistant 서버[{ass_addr}:{ass_port}]를 연결할 수 없습니다. (2)")
                time.sleep(5)
            
            break
          
        time.sleep(5)  # 이 딜레이는 반드시 필요한 딜레이



def run():
    mng = ComManager()
    p = Process(target=mng.loop, args=())
    p.start()

    compose = mng.monitor.get_compose()['manager']
    addr, port = compose['address'], compose['port']
    print(f"Started Manager Server From {addr}:{port}")
    mng.serve()


if __name__ == '__main__':
    run()