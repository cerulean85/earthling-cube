# =============================================================================
# ComAssistant.py
# -----------------------------------------------------------------------------
# 분산 태스크 처리 시스템의 Assistant 노드 핵심 모듈
#
# [아키텍처 개요]
#   Manager (태스크 발견/할당)
#       ↓ gRPC NotifyTask
#   ComAssistant (태스크 수신 & 워커 분배)  ← 이 파일
#       ↓ multiprocessing.Queue
#   WorkerPool → 개별 Worker 프로세스들
#
# [주요 역할]
#   1. gRPC 서버를 기동하여 Manager로부터 태스크를 수신
#   2. 멀티프로세싱 기반 WorkerPool을 관리하며 태스크를 분배
#   3. 프로세스 간 공유 변수(Value, Queue)를 통한 상태 동기화
# =============================================================================

import os, sys
# 상위 디렉토리를 모듈 탐색 경로에 추가 (proto 패키지 등 참조를 위함)
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import time, proto
# Process: 별도 프로세스 생성 (loop를 독립 프로세스로 실행)
# Value:   프로세스 간 공유 변수 (idle_count 동기화에 사용)
# Queue:   프로세스 간 공유 큐 (태스크 전달 파이프라인)
from multiprocessing import Process, Value, Queue
from .Com import Com              # 공통 통신 베이스 클래스
from .ComWorker import WorkerPool  # 워커 프로세스 풀 관리자
from .Logging import log           # 로깅 유틸리티
from .Monitor import Monitor       # 설정(compose) 정보 조회 모듈

class ComAssistant(Com):
    """
    Assistant 노드의 핵심 클래스.
    
    Com(공통 통신 클래스)을 상속받아 gRPC 서버 기동과
    워커 풀 관리 루프를 담당한다.
    
    [디자인 패턴]
    - Decorator Pattern: AssistantEarthlingDecorator를 통해
      gRPC 통신 로직을 분리하여 관심사를 격리
    - Singleton Pattern: WorkerPool.getInstance()로 워커 풀 단일 인스턴스 보장
    """

    def __init__(self, decorator, task_queue=None):
        """
        Args:
            decorator: AssistantEarthlingDecorator 인스턴스
                       - gRPC 서버 기동(serve), 원격 호출(echo) 등을 담당
                       - idle_count 공유 변수를 내부적으로 관리
            task_queue: multiprocessing.Queue 인스턴스 (프로세스 간 공유 큐)
                        - Manager로부터 수신한 태스크가 이 큐에 적재됨
                        - loop()에서 WorkerPool에 전달하여 워커가 소비
        """
        super().__init__()          # Com 초기화 (Monitor 등 공통 설정)
        self.decorator = decorator  # gRPC 통신 데코레이터
        self.procs = []             # 관리 중인 프로세스 목록 (확장용)
        self.task_queue = task_queue # 프로세스 간 공유 태스크 큐

    def serve(self):
        """
        gRPC 서버를 기동하여 Manager로부터의 요청을 수신 대기한다.
        
        [동작 흐름]
        1. compose 설정에서 현재 호스트(ass-host) 주소를 조회
        2. assistant 목록에서 자신의 주소와 일치하는 항목을 찾아 포트 결정
        3. 해당 포트로 gRPC 서버 기동 (blocking - 메인 스레드 점유)
        
        Note: 이 메서드는 blocking 호출이므로, 반드시 loop()를
              별도 프로세스로 먼저 시작한 뒤 호출해야 한다.
        """
        compose = self.monitor.get_compose()           # earth-compose.yaml 설정 로드
        host_addr = compose['ass-host']['address']     # 현재 호스트의 IP/도메인
        assistant = compose['assistant']               # 전체 Assistant 노드 목록
        for ass in assistant:
            if ass['address'] == host_addr:            # 자기 자신에 해당하는 설정 탐색
                port = str(ass['port'])                # 바인딩할 포트 번호
                self.decorator.serve(port)             # gRPC 서버 시작 (blocking)
                break

    def loop(self, worker_pool):
        """
        워커 상태를 주기적으로 폴링하며 태스크를 분배하는 이벤트 루프.
        별도 프로세스(Process)에서 실행된다.
        
        [동작 흐름] (3초 주기로 반복)
        1. 모든 워커의 작업 상태(is_working)를 순회 확인
        2. 유휴(idle) 워커 발견 시 → 큐에서 태스크를 꺼내 할당
        3. 태스크가 없으면 idle_count 증가
        4. 최종 idle_count를 공유 변수에 반영 → Manager가 참조
        
        Args:
            worker_pool: WorkerPool 인스턴스 (워커 프로세스들의 관리자)
        """
        # 자식 프로세스에서 시작될 때 공유 큐를 워커 풀에 설정
        # (fork 시 큐 객체가 올바르게 전달되도록 프로세스 시작 후 재설정)
        if self.task_queue is not None:
            worker_pool.set_task_queue(self.task_queue)
        
        # ── 메인 이벤트 루프 (무한 반복) ──
        while True:
            try:
                idle_count = 0                       # 매 사이클마다 유휴 워커 수 초기화
                workers = worker_pool.workers         # 전체 워커 리스트 참조
                
                # ── 각 워커의 상태를 순회하며 태스크 분배 ──
                for worker in workers:
                    # 워커의 작업 상태 확인 (Value: 프로세스 간 공유 변수)
                    # is_working > 0 이면 현재 작업 중
                    is_working = True if worker.is_working.value > 0 else False

                    # [태스크 할당] 유휴 워커에게 큐에서 태스크를 꺼내 배정
                    if not is_working: 
                        is_working = worker_pool.pop_work()
                        
                    # [유휴 카운트] 태스크도 없고 작업 중도 아니면 idle
                    if not is_working: 
                        idle_count += 1

                # idle_count를 공유 변수에 반영
                # → Manager가 gRPC로 조회하여 부하 분산 판단에 활용
                self.decorator.set_idle_count(idle_count)                    

            except Exception as err:
                # 네트워크 단절, 공유 메모리 접근 오류 등 예외 처리
                print(f"Loop error: {err}")
                print("Can't connect remote...")
                time.sleep(1)
                pass    

            # 3초 간격으로 폴링 (CPU 부하 방지)
            time.sleep(3)


def action(task):
    """기본 액션 함수 (플레이스홀더). 실제 사용 시 각 Application에서 재정의한다."""
    print("Undefined Action")


def run(action):
    """
    Assistant 노드의 진입점(Entry Point).
    전체 시스템을 조립(Composition)하고 서비스를 시작한다.
    
    [실행 순서]
    1. compose 설정 로드 → Manager/Host 주소 확인
    2. 프로세스 간 공유 자원 생성 (Queue, Value)
    3. WorkerPool 초기화 → 워커 프로세스 준비
    4. gRPC 서비스 객체 조립 (Decorator Pattern)
    5. Manager 연결 확인 (Echo 테스트)
    6. loop()를 별도 프로세스로 시작 → 태스크 분배 루프
    7. gRPC 서버 기동 → Manager 요청 수신 대기 (blocking)
    
    Args:
        action: 워커가 실행할 콜백 함수
                태스크를 인자로 받아 실제 비즈니스 로직 수행
                (예: 검색, 클리닝, TF-IDF 분석 등)
    """
    # ── 1단계: 설정 로드 ──
    compose = Monitor().get_compose()  # earth-compose.yaml에서 노드 구성 정보 로드

    # Manager 서버 접속 정보
    mng_addr, mng_port   =  compose['manager']['address'], compose['manager']['port']
    # 현재 Assistant 호스트 정보
    host_addr, host_port =  compose['ass-host']['address'], compose['ass-host']['port']
    
    # ── 2단계: 프로세스 간 공유 자원 생성 ──
    # Queue: Manager → Assistant → Worker 로 태스크가 흐르는 파이프라인
    task_queue = Queue()
    print("Create and set task_queue in main process")

    # ── 3단계: 워커 풀 초기화 ──
    # Singleton 패턴으로 WorkerPool 인스턴스 생성
    # action 콜백을 주입하여 각 워커가 실행할 로직 결정
    worker_pool = WorkerPool.getInstance(action)
    worker_pool.set_task_queue(task_queue)  # 공유 큐를 워커 풀에 연결

    # ── 4단계: gRPC 서비스 객체 조립 (Decorator Pattern) ──
    # shared_idle_count: 유휴 워커 수를 프로세스 간 공유하는 정수형 변수
    # → Manager가 gRPC로 조회하여 부하가 적은 Assistant에 태스크 할당
    shared_idle_count = Value('i', 0)

    # AssistantEarthling: gRPC 서비스 구현체 (공유 변수 & 워커 풀 참조)
    earthling = proto.AssistantEarthling(shared_idle_count, worker_pool)

    # AssistantEarthlingDecorator: gRPC 서버 기동/클라이언트 호출을 래핑
    # → 핵심 로직(earthling)과 통신 인프라를 분리
    decorator = proto.AssistantEarthlingDecorator(earthling)

    # ComAssistant: 위의 모든 컴포넌트를 통합하는 오케스트레이터
    ass = ComAssistant(decorator, task_queue)

    # ── 5단계: Manager 서버 연결 확인 (Health Check) ──
    # Echo 메시지를 Manager에 보내 연결 상태를 사전 검증
    message = str(host_port) if 'int' in str(type(host_port)) else host_port
    try:
        echoed = ass.decorator.echo(mng_addr, mng_port, message)
        print(f"Echo message received from Manager: {echoed}")
    except Exception as err:
        print(f"Failed to connect to Manager server: {err}")
        print("Cannot connect to Manager server.")

    # ── 6단계: 태스크 분배 루프를 별도 프로세스로 시작 ──
    # loop()는 3초 주기로 워커 상태를 확인하고 태스크를 분배
    # 별도 프로세스로 실행하여 gRPC 서버와 병렬 동작
    p = Process(target=ass.loop, args=(worker_pool,))
    p.start()

    # ── 7단계: gRPC 서버 기동 (blocking) ──
    # 이 시점부터 Manager의 gRPC 요청(NotifyTask 등)을 수신 대기
    print(f"Started Assistant Server From {host_addr}:{host_port}")
    ass.serve()  # blocking 호출 - 서버가 종료될 때까지 반환하지 않음


# if __name__ == '__main__':
#     run(action)