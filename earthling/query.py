import os, sys, yaml, random, time
from earthling.connector.DBPoolConnector import DBPoolConnector, execute
from earthling.connector.s3_module import upload_file_to_s3 as s3_upload

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from enum import Enum

class ScrapState(Enum):
    PENDING = "pending"
    IN_PROGRESS = "progress"
    COMPLETED = "completed"

class PipeTaskStatus(Enum):
    EXTRACT = "extract"
    MERGE = "merge"

class DBPool(DBPoolConnector):
    def __init__(self):        
        db_name = ""
        with open(f"earth-compose.yaml") as f:
            compose = yaml.load(f, Loader=yaml.FullLoader)
            db_list = compose["db"]            
            db_names = list(db_list.keys())
            db_name = db_names[0] if len(db_names) > 0 else ''
        self.getDBOption(db_name)
        if not DBPool._instance:
            self.pool = self.getPool()

    @classmethod
    def getInstance(cls):
        if not cls._instance:
            cls._instance = DBPool()
        return cls._instance


def get_mng_host_ip():
    host = ""
    with open(f"earth-compose.yaml") as f:
        compose = yaml.load(f, Loader=yaml.FullLoader)
        host = compose["rpc"]["mng-host"]
    return host["address"]


def select_wait_task():
    host_ip = get_mng_host_ip()
    query = f"""
      SELECT 
        SS.*
      FROM (
          SELECT *
          FROM scrap_status
          WHERE (worker IS NULL OR worker != '{host_ip}')
      ) SS
      JOIN pipe_task ON pipe_task.id = SS.pipe_task_id
      WHERE pipe_task.status = '{PipeTaskStatus.EXTRACT.value}'
      ORDER BY SS.id ASC
      LIMIT 10    
    """

    result = execute_query(query)
    random.shuffle(result)
    return result


def update_state(scrap_status_id, state):
    query = (
        f"UPDATE scrap_status SET state = '{state}' WHERE id = {int(scrap_status_id)}"
    )
    execute_query(query)


def update_state_worker(scrap_status_id, state, ass_addr):
    query = f"UPDATE scrap_status SET state = '{state}', worker = '{ass_addr}' WHERE id = {int(scrap_status_id)}"
    execute_query(query)


def get_pipe_task_id(scrap_status_id):
    query = f"SELECT pipe_task_id FROM scrap_status WHERE id={int(scrap_status_id)}"
    result = execute_query(query)
    return result


def update_pipe_task_to_extract(pipe_task_id):
    query = f"UPDATE pipe_task SET status = '{PipeTaskStatus.EXTRACT.value}' WHERE id = {pipe_task_id}"
    execute_query(query)


def update_state_to_wait(scrap_status_id):
    result = get_pipe_task_id(scrap_status_id)
    if len(result) > 0:
        pipe_task_id = result[0]["pipe_task_id"]
        update_pipe_task_to_extract(pipe_task_id)
        update_state(scrap_status_id, ScrapState.PENDING.value)


def update_state_to_start(scrap_status_id, ass_addr):
    query = f"SELECT pipe_task_id FROM scrap_status WHERE id={int(scrap_status_id)}"
    result = execute_query(query)
    if len(result) > 0:
        pipe_task_id = result[0]["pipe_task_id"]
        query = f"UPDATE pipe_task SET status = '{PipeTaskStatus.EXTRACT.value}' WHERE id = {pipe_task_id}"
        execute_query(query)
        update_state_worker(scrap_status_id, ScrapState.IN_PROGRESS.value, ass_addr)


def update_state_to_finish(scrap_status_id):
    try:
        update_state(scrap_status_id, ScrapState.COMPLETED.value)
        query = f"SELECT * FROM scrap_status WHERE id={int(scrap_status_id)}"
        result = execute_query(query)
        if len(result) > 0:
            pipe_task_id = result[0]["pipe_task_id"]
            query = f"SELECT * FROM scrap_status WHERE pipe_task_id={pipe_task_id}"
            result = execute_query(query)
            finished = True
            for row in result:
                if row["state"] != ScrapState.COMPLETED.value:
                    finished = False
                    break

            if finished:
                query = (
                    f"UPDATE pipe_task SET status = '{PipeTaskStatus.MERGE.value}' WHERE id = {pipe_task_id}"
                )
                execute_query(query)
    except Exception as err:
        print(err)
        pass


def get_collection_cond(scrap_status_id):
    query = (
        f"SELECT "
        f"  SS.id, PT.scrap_start_date, PT.scrap_end_date, PT.keyword "
        f"FROM ( "
        f"  SELECT id, pipe_task_id FROM scrap_status WHERE id = {int(scrap_status_id)} "
        f") AS SS "
        f"JOIN pipe_task AS PT ON PT.id = SS.pipe_task_id"
    )

    result = execute_query(query)
    return result


def update_state_Pending_to_Progress():
    query = f"UPDATE scrap_status SET state = '{ScrapState.PENDING.value}' WHERE state = '{ScrapState.IN_PROGRESS.value}'"
    execute_query(query)


def update_s3_file_url(no, s3_file_url, file_size):
    query = f"UPDATE scrap_status SET s3_url = '{s3_file_url}', file_size = {file_size} WHERE id = {no}"
    execute_query(query)


def execute_query(query):
    try:

        alter_pool = DBPool
        result = execute(query, alter_pool)

        if isinstance(result, dict) and "rows" in result and "columns" in result:
            rows = result["rows"]
            columns = result["columns"]
            if rows and columns:
                return [dict(zip(columns, row)) for row in rows]
            else:
                return []
        else:
            return result if result else []

    except Exception as err:
        print(
            f"Database query execution error (table not found or connection issue): {err}"
        )
        return []


def select_pipe_task_id(scrap_status_id):
    query = f"SELECT pipe_task_id from scrap_status where id = {int(scrap_status_id)}"
    result = execute_query(query)
    return result


def update_scrap_status(scrap_status_id, **kwargs):
    set_clauses = []
    for k, v in kwargs.items():
        if isinstance(v, str) and v.lower() == "now()":
            set_clauses.append(f"{k} = now()")
        elif isinstance(v, str):
            set_clauses.append(f"{k} = '{v}'")
        else:
            set_clauses.append(f"{k} = {v}")
    set_clause = ", ".join(set_clauses)
    query = f"UPDATE scrap_status SET {set_clause} WHERE id = {int(scrap_status_id)}"
    execute_query(query)


def update_scrap_status_count(scrap_status_id, count):
    update_scrap_status(scrap_status_id, count=count)


def update_scrap_status_start_date_to_now(scrap_status_id):
    update_scrap_status(scrap_status_id, start_date="now()")


def update_state_to_completed(scrap_status_id):
    update_scrap_status(scrap_status_id, state=ScrapState.COMPLETED.value, end_date="now()")


def update_state_to_pending(scrap_status_id):
    update_scrap_status(scrap_status_id, state=ScrapState.PENDING.value, end_date="now()")


def generate_s3_file_key(file_path):
    file_extension = os.path.splitext(file_path)[1]
    now = time.localtime()
    timestamp = f"{now.tm_year}{now.tm_mon:02d}{now.tm_mday:02d}{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}"
    s3_file_key = f"{timestamp}{file_extension}"
    return s3_file_key


def upload_file_to_s3(file_path):
    s3_file_key = generate_s3_file_key(file_path)
    public_url, file_size = s3_upload(file_path, s3_file_key)
    return public_url, file_size
