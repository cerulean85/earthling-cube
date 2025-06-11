import time, yaml, os, json
from datetime import datetime
from .Logging import log

class Monitor:
    def get_compose(self):
        compose = ''
        with open(f'earth-compose.yml') as f:
            compose = yaml.load(f, Loader=yaml.FullLoader)
            compose = compose['rpc']
        return compose

    def write_worker(self, worker):
        worker_no = worker["no"]
        
        # temp 디렉토리가 없으면 생성
        temp_dir = "./temp"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            print(f"temp 디렉토리 생성: {temp_dir}")
        
        with open(f"./temp/worker_{worker_no}.json", "w") as fw:
            json.dump(worker, fw, indent="\t")

    def read_worker(self, worker_no):
        with open(f"./temp/worker_{worker_no}.json", "r") as fr:
            data = json.load(fr)
        return data


if __name__ == "__main__":

    monitor = Monitor()
    compose = monitor.get_compose()
    assistant = compose['assistant']
    host_addr = compose['host']['address']
    while True:
        try:
            now = datetime.now()
            idle_count = 0
            for target_ass in assistant:
                if target_ass['address'] == host_addr:
                    target_workers = target_ass['workers']
                    for worker_no in target_workers:
                        if os.path.isfile(f"./temp/worker_{worker_no}.json"):
                            data = monitor.read_worker(worker_no)
                            if data["state"] == 'idle':
                                idle_count = idle_count + 1
                            else:
                                user_id = data['task']['user_id']
                                task_no = data['task']['task_no']
                                print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Worker-[{worker_no}]: USER-[{user_id}] => Task-[{task_no}] 처리중...")

                    break
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 작업이 가능한 Worker가 [{idle_count}]개 존재합니다.")
            time.sleep(5)
        except:
            time.sleep(5)
            continue
