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
        # Set task_queue when process starts
        if self.task_queue is not None:
            worker_pool.set_task_queue(self.task_queue)
        
        while True:
            try:
                idle_count = 0
                workers = worker_pool.workers
                
                for worker in workers:
                    # print(f"Worker-{worker.worker_no.value} state: {worker.is_working.value}")
                    is_working = True if worker.is_working.value > 0 else False

                    # If there is a task, run an idle Worker
                    if not is_working: 
                        is_working = worker_pool.pop_work()
                        
                    # If there are no tasks to process, increment idle_count
                    if not is_working: 
                        idle_count += 1

                # Update idle count independently
                self.decorator.set_idle_count(idle_count)                    

            except Exception as err:
                print(f"Loop error: {err}")
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
    print("Create and set task_queue in main process")
    worker_pool = WorkerPool.getInstance(action)
    worker_pool.set_task_queue(task_queue)  # Set Shared Queue

    shared_idle_count = Value('i', 0) # Shared Variable
    earthling = proto.AssistantEarthling(shared_idle_count, worker_pool) # Set Shared Variable
    decorator = proto.AssistantEarthlingDecorator(earthling) # Set Decorate Target object
    ass = ComAssistant(decorator, task_queue) # Pass task_queue to ComAssistant

    # Check Manager Server connection
    message = str(host_port) if 'int' in str(type(host_port)) else host_port
    try:
        echoed = ass.decorator.echo(mng_addr, mng_port, message)
        print(f"Echo message received from Manager: {echoed}")
    except Exception as err:
        print(f"Failed to connect to Manager server: {err}")
        print("Cannot connect to Manager server.")

    # Start process - task_queue is handled inside ComAssistant
    p = Process(target=ass.loop, args=(worker_pool,))
    p.start()

    print(f"Started Assistant Server From {host_addr}:{host_port}")
    ass.serve()


# if __name__ == '__main__':
#     run(action)