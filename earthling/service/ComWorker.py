import time, threading, yaml, json
from .Com import Com
from .Logging import log
from multiprocessing import Process, Value

class ComWorker(Com):

    def __init__(self, worker_no, is_working, action):
        super().__init__()
        self.worker_no = worker_no
        self.is_working = is_working
        self.action = action

    def lock(self):
        self.is_working.value = 1

    def unlock(self):
        self.is_working.value = 0

    def work(self, task):

        worker_meta = {
            "no": self.worker_no.value,
            "thread_no": threading.get_native_id(),
            "state": "working",
            "task": task,
        }
        date_desc = json.loads(task["message"])

        if "channel" in date_desc:
            channel = date_desc["channel"]
        else:
            # print(f"No known key. Available keys: {list(date_desc.keys())}")
            channel = date_desc["task_type"]

        self.monitor.write_worker(worker_meta)
        print(f"Worker-[{self.worker_no.value}] is starting task-[{task['task_no']}] for '{channel}'. (Thread: {threading.get_native_id()})")

        self.action(task)
        self.is_working.value = 0
        print(f"Worker-[{self.worker_no.value}] has completed task-[{task['task_no']}] for '{channel}'.")

        worker_meta["state"] = "idle"
        self.monitor.write_worker(worker_meta)
        self.unlock()
        print(f"Worker-[{self.worker_no.value}] process terminated")


class WorkerPool:
    _instance = None
    _lock = threading.Lock()

    def __init__(self, action=None):
        self.workers = []
        # self.proc_map = {}
        self.task_queue = None
        self.work_procs = []
        self.action = action
        self._initialize_workers()

    def _initialize_workers(self):

        try:
            with open(f"earth-compose.yaml") as f:
                compose = yaml.load(f, Loader=yaml.FullLoader)
                compose = compose["rpc"]

            # Fix: use host instead of ass-host
            host_addr = compose.get("ass-host", compose.get("host", {})).get(
                "address", "localhost"
            )

            assistant = compose.get("assistant", [])
            for target_ass in assistant:
                if target_ass["address"] == host_addr:
                    target_workers = target_ass.get("workers", [])
                    for worker_no in target_workers:
                        # print("worker_no > ", worker_no)
                        worker = ComWorker(
                            Value("i", worker_no), Value("i", 0), self.action
                        )
                        self.workers.append(worker)
                    break

            print(f"WorkerPool initialization complete: {len(self.workers)} workers created")
        except Exception as e:
            print(f"WorkerPool initialization error: {e}")

    @classmethod
    def getInstance(cls, action=None):
        if cls._instance is None:
            with cls._lock:
                # Double-checked locking
                if cls._instance is None:
                    cls._instance = WorkerPool(action)
                    print("New WorkerPool instance created")
                else:
                    print("Returning existing WorkerPool instance")
        else:
            print("Returning existing WorkerPool instance")
        return cls._instance

    def set_task_queue(self, queue):
        print(f"WorkerPool({id(self)}) - task_queue set: {queue}")
        self.task_queue = queue

    def push_task(self, task):
        if self.task_queue is None:
            print(f"WorkerPool({id(self)}) - Task queue is not set.")
            return
        print(
            f"WorkerPool({id(self)}) - New task added: {task.get('task_no', 'unknown')}"
        )
        self.task_queue.put(task)

    def pop_work(self):
        # Return False if task_queue is not set
        if self.task_queue is None:
            print(f"WorkerPool({id(self)}) - task_queue is None.")
            return False

        task_count = self.task_queue.qsize()
        if task_count > 0:
            task = self.task_queue.get()
            print(
                f"WorkerPool({id(self)}) - Starting task: {task.get('task_no', 'unknown')}"
            )
            self.work(task)
            return True
        return False

    def work(self, task):
        for worker in self.workers:
            if worker.is_working.value == 0:
                worker.lock()                
                p = Process(target=worker.work, args=(task,))
                p.start()
                break