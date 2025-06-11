import os, sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import yaml
from handler.earthling_db_pool import exec as pool_exec
from service.Logging import log


class BaseDAO:
    SCRAP_STATUS_TABLE = "scrap_status"
    PIPE_TASK_TABLE = "pipe_task"

    def set_country(self):
        cn_channel_list = ["baidu", "googlecn"]
        if self.channel in cn_channel_list:
            self.country = "cn"
        else:
            self.country = "kr"

    def __init__(self, channel):
        self.channel = channel
        self.set_country()

        self.app_settings = None
        with open("earthling/handler/dao/settings.yaml") as f:
            self.app_settings = yaml.load(f, Loader=yaml.FullLoader)
            self.app_settings = self.app_settings[channel]

        self.data_type_names = self.app_settings["data_type_names"]
        self.test_index = self.app_settings["test_index"]

    def get_mng_host_ip(self):
        host = ""
        with open(f"earth-compose.yml") as f:
            compose = yaml.load(f, Loader=yaml.FullLoader)
            host = compose["rpc"]["mng-host"]
        return host["address"]

    def get_test_index_query(self):
        if self.test_index > 0:
            return f"id = {self.test_index} AND "
        return ""

    def select_wait_task(self):
        pass

    def update_state(self, scrap_status_id, state):
        query = f"UPDATE {self.SCRAP_STATUS_TABLE} SET state = '{state}' WHERE id = {scrap_status_id}"
        self.exec(query)

    def update_state_worker(self, scrap_status_id, state, ass_addr):
        query = f"UPDATE {self.SCRAP_STATUS_TABLE} SET state = '{state}', worker = '{ass_addr}' WHERE id = {scrap_status_id}"
        self.exec(query)

    def get_pipe_task_id(self, scrap_status_id):
        query = f"SELECT pipe_task_id FROM {self.SCRAP_STATUS_TABLE} WHERE id={scrap_status_id}"
        result = self.exec(query)
        return result

    def update_pipe_task_to_extract(self, pipe_task_id):
        query = f"UPDATE {self.PIPE_TASK_TABLE} SET status = 'extract' WHERE id = {pipe_task_id}"
        self.exec(query)

    def update_state_to_wait(self, scrap_status_id):
        result = self.get_pipe_task_id(scrap_status_id)
        if len(result) > 0:
            pipe_task_id = result[0]["pipe_task_id"]
            self.update_pipe_task_to_extract(pipe_task_id)
            self.update_state(scrap_status_id, "Y")

    def update_state_to_start(self, scrap_status_id, ass_addr):
        query = f"SELECT pipe_task_id FROM {self.SCRAP_STATUS_TABLE} WHERE id={scrap_status_id}"
        result = self.exec(query)
        if len(result) > 0:
            pipe_task_id = result[0]["pipe_task_id"]
            query = f"UPDATE {self.PIPE_TASK_TABLE} SET status = 'extract' WHERE id = {pipe_task_id}"
            self.exec(query)
            self.update_state_worker(scrap_status_id, "S", ass_addr)

    def update_state_to_finish(self, no):
        pass

    def get_collection_cond(self, scrap_status_id):
        query = (
            f"SELECT "
            f"  SS.id, PT.scrap_start_date, PT.scrap_end_date, PT.keyword "
            f"FROM ( "
            f"  SELECT id, pipe_task_id FROM {self.SCRAP_STATUS_TABLE} WHERE id = {scrap_status_id} "
            f") AS SS "
            f"JOIN {self.PIPE_TASK_TABLE} AS PT ON PT.id = SS.pipe_task_id"
        )

        result = self.exec(query)
        return result

    def update_state_S_to_Y(self):
        query = f"UPDATE {self.SCRAP_STATUS_TABLE} SET state = 'Y' WHERE state = 'S'"
        self.exec(query)

    def update_s3_file_url(self, no, s3_file_url, file_size):
        query = f"UPDATE {self.SCRAP_STATUS_TABLE} SET s3_url = '{s3_file_url}', file_size = {file_size} WHERE id = {no}"
        self.exec(query)

    def exec(self, query):
        """
        데이터베이스 쿼리를 실행하고 딕셔너리 형태의 결과를 반환합니다.

        Returns:
            list: 각 행이 {컬럼명: 값} 형태의 딕셔너리인 리스트
                  예: [{'id': 1, 'name': 'test', 'status': 'Y'}, ...]
        """
        try:
            result = pool_exec(query, country=self.country)

            # pool_exec에서 {'rows': [...], 'columns': [...]} 형태로 반환됨
            if isinstance(result, dict) and "rows" in result and "columns" in result:
                rows = result["rows"]
                columns = result["columns"]

                # SELECT 쿼리인 경우 딕셔너리로 변환
                if rows and columns:
                    dict_result = []
                    for row in rows:
                        row_dict = {}
                        for i, column in enumerate(columns):
                            row_dict[column] = row[i] if i < len(row) else None
                        dict_result.append(row_dict)
                    return dict_result
                else:
                    # UPDATE/INSERT/DELETE 쿼리인 경우 또는 결과가 없는 경우
                    if "rowcount" in result:
                        print(f"쿼리 실행 완료: {result['rowcount']}개 행이 영향받음")
                    return []
            else:
                # 예외 처리나 기존 형태의 결과인 경우
                return result if result else []

        except Exception as err:
            print(f"DB 쿼리 실행 오류 (테이블이 없거나 연결 문제): {err}")
            return []  # 빈 결과 반환
