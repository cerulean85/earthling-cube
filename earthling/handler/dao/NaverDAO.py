import os, sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import yaml
from handler.dao.BaseDAO import BaseDAO

# 해당 모듈을 추가하면 다른 곳에서 순환 종속 에러 발생할 수 잇음
from service.Logging import log


class NaverDAO(BaseDAO):

    def __init__(self):
        super().__init__("naver")

    def select_wait_task(self):
        test_index_query = self.get_test_index_query()
        host_ip = self.get_mng_host_ip()
        query = f"""
      SELECT 
        SS.*
      FROM (
          SELECT *
          FROM scrap_status
          WHERE (worker IS NULL OR worker != '{host_ip}') AND site = 'naver'
      ) SS
      JOIN pipe_task ON pipe_task.id = SS.pipe_task_id
      WHERE pipe_task.status = 'extract'
      ORDER BY SS.id ASC
      LIMIT 10    
    """
        # print(query)


        result = self.exec(query)
        return result

    def update_state_to_finish(self, scrap_status_id):
        try:
            self.update_state(scrap_status_id, "N")
            result = self.exec(f"SELECT * FROM scrap_status WHERE id={scrap_status_id}")
            if len(result) > 0:
                pipe_task_id, state = result[0]["pipe_task_id"], result[0]["state"]
                result = self.exec(f"SELECT * FROM scrap_status WHERE pipe_task_id={pipe_task_id}")
                finished = True
                for row in result:
                    if row["state"] != "N":
                        finished = False
                        break
                      
                if finished:
                    self.exec(
                        f"UPDATE pipe_task SET status = 'merge' WHERE id = {pipe_task_id}"
                    )
        except Exception as err:
            print(err)
            pass
