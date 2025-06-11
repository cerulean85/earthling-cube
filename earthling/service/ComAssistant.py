import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import time, proto
from multiprocessing import Process, Value, Queue
from .Com import Com
from .ComWorker import WorkerPool
from .Logging import log
from .Monitor import Monitor

class ComAssistant(Com):
    def __init__(self, decorator, task_queue=None):
        super().__init__()
        self.decorator = decorator
        self.procs = []
        self.task_queue = task_queue

    def serve(self):
        compose = self.monitor.get_compose()
        host_addr = compose['ass-host']['address']
        assistant = compose['assistant']
        for ass in assistant:
            if ass['address'] == host_addr:
                port = str(ass['port'])
                self.decorator.serve(port)
                break

    def loop(self, worker_pool):
        # 프로세스 시작 시 task_queue 설정
        if self.task_queue is not None:
            print(f"루프 프로세스에서 task_queue 재설정: {self.task_queue}")
            worker_pool.set_task_queue(self.task_queue)
        
        while True:
            try:
                idle_count = 0
                workers = worker_pool.workers
                
                # print(f"현재 워커 수: {len(workers)}")
                
                for worker in workers:
                    # print(f"Worker-{worker.worker_no.value} 상태: {worker.is_working.value}")
                    is_working = True if worker.is_working.value > 0 else False

                    # task가 있다면 유휴한 Worker를 실행
                    if not is_working: 
                        is_working = worker_pool.pop_work()
                        
                    # 처리할 task가 없다면 idle_count + 1
                    if not is_working: 
                        idle_count = idle_count + 1

                # print(f"현재 idle 워커 수: {idle_count}")

                # idle count를 독립적으로 집계하여 업데이트함
                self.decorator.set_idle_count(idle_count)                    

            except Exception as err:
                print(f"루프 오류: {err}")
                print("Can't connect remote...")
                time.sleep(1)
                pass    

            time.sleep(3)


def action(task):
    print("Undefined Action")

def run(action):
    compose = Monitor().get_compose()

    mng_addr, mng_port   =  compose['manager']['address'], compose['manager']['port']
    host_addr, host_port =  compose['ass-host']['address'], compose['ass-host']['port']
    
    task_queue = Queue() # Shared Queue
    print("메인 프로세스에서 task_queue 생성 및 설정")
    worker_pool = WorkerPool.getInstance(action)
    worker_pool.set_task_queue(task_queue)  # Shared Queue 설정

    shared_idle_count = Value('i', 0) # Shared Variable
    earthling = proto.AssistantEarthling(shared_idle_count, worker_pool) # Shared Variable 설정
    decorator = proto.AssistantEarthlingDecorator(earthling) # Decorate Target 객체 설정
    ass = ComAssistant(decorator, task_queue) # task_queue를 ComAssistant에 전달

    # Manager Server 연결 확인
    message = str(host_port) if 'int' in str(type(host_port)) else host_port
    try:
        echoed = ass.decorator.echo(mng_addr, mng_port, message)
        print(f"Manager로부터 받은 echo 메시지: {echoed}")
    except Exception as err:
        print(f"Manager 서버 연결 실패: {err}")
        print("Manager 서버에 연결할 수 없습니다.")

    # 프로세스 시작 - task_queue는 ComAssistant 내부에서 처리됨
    p = Process(target=ass.loop, args=(worker_pool,))
    p.start()

    print(f"Started Assistant Server From {host_addr}:{host_port}")
    ass.serve()


# if __name__ == '__main__':
#     run(action)