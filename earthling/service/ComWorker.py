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
            "task": task
        }
        date_desc = json.loads(task["message"])
        print(f"Task message 내용: {date_desc}")
        
        # 키 이름을 안전하게 확인하여 가져오기
        if "channel" in date_desc:
            channel = date_desc["channel"]
        else:
            # 기본값 설정 또는 다른 키 찾기
            print(f"알려진 키가 없음. 사용 가능한 키들: {list(date_desc.keys())}")
            channel = "unknown"

        self.monitor.write_worker(worker_meta)
        print(f"Worker-[{self.worker_no.value}]가 task-[{task['task_no']}]의 '{channel}'(을)를 시작합니다. (Thread: {threading.get_native_id()})")
        print(f"Worker-[{self.worker_no.value}]가 task-[{task['task_no']}]의 '{channel}'(을)를 처리중입니다.")

        self.action(task)
        self.is_working.value = 0
        print(f"Worker-[{self.worker_no.value}]가 task-[{task['task_no']}]의 '{channel}'(을)를 완료하였습니다.")

        worker_meta["state"] = "idle"
        self.monitor.write_worker(worker_meta)
        self.unlock()

        # 멀티프로세싱 환경에서는 terminate 호출하지 않음
        # WorkerPool.getInstance().terminate(self.worker_no.value)
        print(f"Worker-[{self.worker_no.value}] 프로세스 종료")

class WorkerPool:    
    _instance = None
    _lock = threading.Lock()

    # def default_action(self, task): 
    #     print("Default Action으로 처리합니다.")
    #     time.sleep(10)

    def __init__(self, action=None):
        self.workers = []
        self.proc_map = {}
        self.task_queue = None
        self.work_procs = []
        self.action = action
        
        # WorkerPool 초기화 - 설정 파일에서 워커 정보 로드
        self._initialize_workers()

    def _initialize_workers(self):
        """워커들을 초기화합니다."""
        try:
            with open(f'earth-compose.yaml') as f:
                compose = yaml.load(f, Loader=yaml.FullLoader)
                compose = compose['rpc']
            
            # 수정: ass-host 대신 host 사용
            host_addr = compose.get('ass-host', compose.get('host', {})).get('address', 'localhost')
            
            assistant = compose.get('assistant', [])
            for target_ass in assistant:
                if target_ass['address'] == host_addr:
                    target_workers = target_ass.get('workers', [])
                    for worker_no in target_workers:
                        worker = ComWorker(Value('i', worker_no), Value('i', 0), self.action)
                        self.workers.append(worker)
                    break
                    
            print(f"WorkerPool 초기화 완료: {len(self.workers)}개 워커 생성")
        except Exception as e:
            print(f"WorkerPool 초기화 오류: {e}")

    @classmethod
    def getInstance(cls, action=None):
        """Thread-safe 싱글톤 구현"""
        if cls._instance is None:
            with cls._lock:
                # Double-checked locking
                if cls._instance is None:
                    cls._instance = WorkerPool(action)
                    print("새로운 WorkerPool 인스턴스 생성")
                else:
                    print("기존 WorkerPool 인스턴스 반환")
        else:
            print("기존 WorkerPool 인스턴스 반환")
        return cls._instance

    def set_task_queue(self, queue):
        print(f"WorkerPool({id(self)}) - task_queue 설정: {queue}")
        self.task_queue = queue

    def push_task(self, task):
        if self.task_queue is None:
            print(f"WorkerPool({id(self)}) - Task queue가 설정되지 않았습니다.")
            return
        print(f"WorkerPool({id(self)}) - 새 task 추가: {task.get('task_no', 'unknown')}")
        self.task_queue.put(task)
    
    def pop_work(self):
        # task_queue가 설정되지 않았다면 False 반환
        if self.task_queue is None:
          print(f"WorkerPool({id(self)}) - task_queue가 None입니다.")
          return False
            
        task_count = self.task_queue.qsize()
        # print(f"WorkerPool({id(self)}) - 대기 중인 task 수: {task_count}")
        
        if task_count > 0:
          task = self.task_queue.get()
          print(f"WorkerPool({id(self)}) - task 처리 시작: {task.get('task_no', 'unknown')}")
          self.work(task)
          return True
        
        # 테스트가 필요한 코드: Process Terminate
        for p in self.work_procs:
          if p.is_alive():
              p.terminate()
        self.work_procs = []

        return False

    def work(self, task):
      for worker in self.workers:
        if worker.is_working.value == 0:
          worker.lock()
          p = Process(target=worker.work, args=(task, ))
          self.work_procs.append(p) # 테스트가 필요한 코드: Process Save
          p.start()

          self.proc_map[worker.worker_no] = p
          break

    def terminate(self, worker_no):
      if self.proc_map.get(worker_no) is not None:
        p = self.proc_map[worker_no]
        p.terminate()