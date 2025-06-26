# import os, sys, yaml, random, time
# from earthling.connector.DBPoolConnector import DBPoolConnector, execute
# from earthling.connector.s3_module import upload_file_to_s3 as s3_upload


from operator import or_
import os, sys, yaml, random
from earthling.connector.s3_module import generate_s3_file_key
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from enum import Enum
from contextlib import contextmanager

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))


class PipeTaskState(Enum):
    PREPARING = "preparing"
    PENDING = "pending"
    IN_PROGRESS = "progress"
    COMPLETED = "completed"


class PipeTaskStatus(Enum):
    SEARCH = "search"
    CLEAN = "clean"
    FREQUENCY = "frequency"
    TFIDF = "tfidf"
    CONCOR = "concor"


def get_db_url():
    with open("earth-compose.yaml") as f:
        compose = yaml.safe_load(f)
        for value in compose["db"].values():
            return value
    return None


def get_mng_host_ip():
    with open("earth-compose.yaml") as f:
        compose = yaml.safe_load(f)
        return compose["rpc"]["mng-host"]["address"]


def get_pending_discovery_count():
    with open("earth-compose.yaml") as f:
        compose = yaml.safe_load(f)
        return compose.get("pending_discovery_count", 10)


db_url = get_db_url()
engine = create_engine(db_url)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


class PipeLine(Base):
    __tablename__ = "pipe_line"
    id = Column(Integer, primary_key=True)
    current_status = Column(String)
    create_date = Column(DateTime)
    mem_id = Column(Integer)


class PipeTaskSearch(Base):
    __tablename__ = "pipe_task_search"
    id = Column(Integer, primary_key=True)
    pipe_line_id = Column(Integer)
    site = Column(String)
    channel = Column(String)
    current_state = Column(String)
    search_keyword = Column(String)
    search_start_date = Column(DateTime)
    search_end_date = Column(DateTime)
    worker_ip = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    create_date = Column(DateTime)
    count = Column(Integer)
    s3_url = Column(String)
    file_size = Column(Float)
    mem_id = Column(Integer)

class PipeTaskClean(Base):
    __tablename__ = "pipe_task_clean"
    id = Column(Integer, primary_key=True)
    pipe_line_id = Column(Integer)
    current_state = Column(String)
    worker_ip = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    create_date = Column(DateTime)
    s3_url = Column(String)
    file_size = Column(Float)
    search_task_id = Column(Integer)
    mem_id = Column(Integer)
    extract_noun = Column(Boolean)
    extract_adjective = Column(Boolean)
    extract_verb = Column(Boolean)


class PipeTaskFrequency(Base):
    __tablename__ = "pipe_task_frequency"
    id = Column(Integer, primary_key=True)
    pipe_line_id = Column(Integer)
    current_state = Column(String)
    worker_ip = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    create_date = Column(DateTime)
    s3_url = Column(String)
    file_size = Column(Float)
    search_task_id = Column(Integer)
    clean_task_id = Column(Integer)
    mem_id = Column(Integer)


class PipeTaskTfidf(Base):
    __tablename__ = "pipe_task_tfidf"
    id = Column(Integer, primary_key=True)
    pipe_line_id = Column(Integer)
    current_state = Column(String)
    worker_ip = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    create_date = Column(DateTime)
    s3_url = Column(String)
    file_size = Column(Float)
    search_task_id = Column(Integer)
    clean_task_id = Column(Integer)
    mem_id = Column(Integer)

class PipeTaskConcor(Base):
    __tablename__ = "pipe_task_concor"
    id = Column(Integer, primary_key=True)
    pipe_line_id = Column(Integer)
    current_state = Column(String)
    worker_ip = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    create_date = Column(DateTime)
    s3_url = Column(String)
    file_size = Column(Float)
    search_task_id = Column(Integer)
    clean_task_id = Column(Integer)
    mem_id = Column(Integer)

pipe_task_tables = {
    PipeTaskStatus.SEARCH: PipeTaskSearch,
    PipeTaskStatus.CLEAN: PipeTaskClean,
    PipeTaskStatus.FREQUENCY: PipeTaskFrequency,
    PipeTaskStatus.TFIDF: PipeTaskTfidf,
    PipeTaskStatus.CONCOR: PipeTaskConcor,
}


class QueryPipeTask:
    def __init__(self, pipe_task_type=None):
        self.pipe_task_type = pipe_task_type
        self.target_task_table = pipe_task_tables.get(pipe_task_type)

    def model_to_dict(self, status, obj):
        return {
            column.name: getattr(obj, column.name)
            for column in obj.__table__.columns
        } | {"task_type": status}

    def search_pending_task(self):
        pending_task_list = []
        discovery_count = get_pending_discovery_count()
        with get_session() as session:
            for status, TaskTable in pipe_task_tables.items():
                result = session.query(TaskTable).filter(
                          or_(TaskTable.worker_ip == None, TaskTable.worker_ip == ''),
                          TaskTable.current_state == PipeTaskState.PENDING.value
                      ).order_by(TaskTable.id.asc()).limit(discovery_count).all()                
                random.shuffle(result)
                if len(result) == 0: continue
                print(f"STATUS: {status.value} => ", result)
                searched_tasks = [self.model_to_dict(status.value, row) for row in result]
                pending_task_list.extend(searched_tasks)
        return pending_task_list
    
    def get_pipe_line_id(self, task_id):
        with get_session() as session:
            TaskTable = self.target_task_table
            return session.query(TaskTable.pipe_line_id).filter(TaskTable.id == task_id).scalar()

    def get_task_by_id(self, task_id):
        with get_session() as session:
            TaskTable = self.target_task_table
            return session.query(TaskTable).filter(TaskTable.id == task_id).first()

    def get_tasks_by_line_id(self, line_id):
        with get_session() as session:
            TaskTable = self.target_task_table
            return session.query(TaskTable).filter(TaskTable.pipe_line_id == line_id).all()

    def update_fields(self, task_id, update_dict):
        with get_session() as session:
            TaskTable = self.target_task_table
            session.query(TaskTable).filter(TaskTable.id == task_id).update(update_dict)
            session.commit()

    def update_state_worker(self, task_id, state, ass_addr):
        self.update_fields(task_id, {"current_state": state, "worker_ip": ass_addr})

    def update_state(self, task_id, state):
        self.update_fields(task_id, {"current_state": state})

    def update_pipe_line_status(self, line_id, status):
        with get_session() as session:
            session.query(PipeLine).filter(PipeLine.id == line_id).update({"current_status": status})
            session.commit()

    def update_pipe_line_to_search(self, line_id):
        self.update_pipe_line_status(line_id, PipeTaskStatus.SEARCH.value)

    def update_pipe_line_to_clean(self, line_id):
        self.update_pipe_line_status(line_id, PipeTaskStatus.CLEAN.value)

    def update_state_to_wait(self, task_id):
        pipe_line_id = self.get_pipe_line_id(task_id)
        if pipe_line_id:
            self.update_pipe_line_to_search(pipe_line_id)
            self.update_state(task_id, PipeTaskState.PENDING.value)

    def update_state_to_start(self, task_id, ass_addr):
        pipe_line_id = self.get_pipe_line_id(task_id)
        self.update_pipe_line_to_search(pipe_line_id)
        self.update_state_worker(task_id, PipeTaskState.IN_PROGRESS.value, ass_addr)

    def update_state_to_finish(self, task_id):
        try:
            self.update_state(task_id, PipeTaskState.COMPLETED.value)
            task = self.get_task_by_id(task_id)
            if not task:
                return
            pipe_line_id = task.pipe_line_id
            tasks = self.get_tasks_by_line_id(pipe_line_id)
            if all(t.current_state == PipeTaskState.COMPLETED.value for t in tasks):
                self.update_pipe_line_to_clean(pipe_line_id)
        except Exception as err:
            print(f"[update_state_to_finish] Error: {err}")

    def get_collection_cond(self, task_id):
        return self.get_task_by_id(task_id)

    def update_s3_file_url(self, task_id, s3_file_url, file_size):
        self.update_fields(task_id, {"s3_url": s3_file_url, "file_size": file_size})

    def update_search_status_start_date_to_now(self, task_id):
        self.update_fields(task_id, {"start_date": datetime.now()})

    def update_state_to_completed(self, task_id):
        self.update_fields(task_id, {"current_state": PipeTaskState.COMPLETED.value, "end_date": datetime.now()})

    def update_state_to_pending(self, task_id):
        self.update_fields(task_id, {"current_state": PipeTaskState.PENDING.value, "end_date": datetime.now()})

    def update_state_to_pending_about_clean_task(self, task_type: PipeTaskStatus, search_task_id):
        with get_session() as session:
            TaskTable = pipe_task_tables.get(task_type)
            session.query(TaskTable).filter(TaskTable.search_task_id == search_task_id).update({"current_state": PipeTaskState.PENDING.value})
            session.commit()        
        print(f"prev_task-[{search_task_id}] {task_type.value} to PENDING")

    def update_state_to_pending_about_analysis_task(self, task_type: PipeTaskStatus, clean_task_id):
        with get_session() as session:
            TaskTable = pipe_task_tables.get(task_type)
            session.query(TaskTable).filter(TaskTable.clean_task_id == clean_task_id).update({"current_state": PipeTaskState.PENDING.value})
            session.commit()        
        print(f"prev_task-[{clean_task_id}] {task_type.value} to PENDING")



class QueryPipeTaskSearch(QueryPipeTask):
    def __init__(self):
        super().__init__(PipeTaskStatus.SEARCH)

    def update_search_status_count(self, task_id, count):
        self.update_fields(task_id, {"count": count, "end_date": datetime.now()})

class QueryPipeTaskClean(QueryPipeTask):
    def __init__(self):
        super().__init__(PipeTaskStatus.CLEAN)

class QueryPipeTaskFrequency(QueryPipeTask):
    def __init__(self):
        super().__init__(PipeTaskStatus.FREQUENCY)

class QueryPipeTaskTfidf(QueryPipeTask):
    def __init__(self):
        super().__init__(PipeTaskStatus.TFIDF)

class QueryPipeTaskConcor(QueryPipeTask):
    def __init__(self):
        super().__init__(PipeTaskStatus.CONCOR)

def get_query_pipe_task_instance(task_status) -> QueryPipeTask:
    if task_status == PipeTaskStatus.SEARCH.value:
        return QueryPipeTaskSearch()
    if task_status == PipeTaskStatus.CLEAN.value:
        return QueryPipeTaskClean()    
    if task_status == PipeTaskStatus.FREQUENCY.value:
        return QueryPipeTaskFrequency()        
    if task_status == PipeTaskStatus.TFIDF.value:
        return QueryPipeTaskTfidf()            
    if task_status == PipeTaskStatus.CONCOR.value:
        return QueryPipeTaskConcor()                
    return None


# Example usage
if __name__ == '__main__':
    query_task = QueryPipeTask()
    result = query_task.search_pending_task()
    print(result)