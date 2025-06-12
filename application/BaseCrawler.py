import time, os, re, sys, pickle, application.common as cmn
import re

from handler.earthling_dao import update_s3_file_url
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# from application.naver.NaverBase   import NaverBase
# from application.google.GoogleBase  import GoogleBase
# from application.baidu.BaiduBase   import BaiduBase
# from earthling.handler.dao.NaverDAO import NaverDAO
# from earthling.handler.dao.GoogleDAO import GoogleDAO
# from earthling.handler.dao.BaiduDAO import BaiduDAO

from application.common import *
from earthling.service.Logging import log
from earthling.handler.earthling_es import insert_list_to_es
from earthling.handler.earthling_s3 import upload_file_to_s3

class BaseCrawler:

    def get_data_poly(self, site, channel):
        app_settings = cmn.get_settings(site=site)
        serialized_file_path = app_settings.get("serialized_file_path")
        poly_path = f"{serialized_file_path}/{channel}.pickle"
        poly = None

        with open(poly_path, 'rb') as file:
            poly = pickle.load(file)
        return poly

    def exec_search(self, poly, data):
        print("Date =>>>>>>> ", data)
        create_file_name, item_count, html_status = poly.search(
                data["keyword"], 
                idx_num = str(data["task_no"]), 
                stop=data["stop"], 
                date_start=data["date_start"].strftime('%Y-%m-%d'), 
                date_end=data["date_end"].strftime('%Y-%m-%d'),
                out_filepath = data["out_filepath"])

        return create_file_name, item_count, html_status

    def __init__(self, site, dao):
        self.site = site
        self.base = self.get_data_poly(site, "base")
        self.dao = dao

    def factory(self, task_no, row, site, channel):
      if row == None:
        print(f"task-[{task_no}]를 수집할 수 없습니다. KEYWORD_LIST 혹은 SCRAW_NAVER_DATAINFO 테이블에서 [{task_no}]를 확인하세요.")
        return

      print("site", site)
      print("channel", channel)

      keyword, date_start, date_end = row["keyword"], row["scrap_start_date"], row["scrap_end_date"]
      print(f"수집정보 => 채널: {site}, 유형: {channel}, 키워드: {keyword}, 기간: {date_start} ~ {date_end}")

      poly = self.get_data_poly(site, channel) 
      if poly is not None:

        self.dao.update_scrap_status_start_date_to_now(task_no)
        search_data = {
            "keyword": keyword, 
            "task_no": str(task_no), 
            "stop": 100, 
            "date_start": date_start, 
            "date_end": date_end,
            "out_filepath": self.get_out_filepath(site, channel)
        }

        create_file_name, item_count, html_status = self.exec_search(poly, search_data)
        if html_status == 200:
          self.dao.update_state_to_N(task_no)
          self.save(task_no, channel, create_file_name, item_count)
          print(f"데이터 수집을 정상적으로 완료하였습니다.")

        else:
          self.dao.update_state_to_Y(task_no)
          print(f"데이터 수집에 실패하였습니다. (HTML STATUS: {html_status})")
          penalty_delay_time = settings['penalty_delay_time']
          time.sleep(penalty_delay_time)
    
    def get_out_filepath(self, site, channel):
        app_settings = cmn.get_settings(site=site)
        scrap_data_save_path = app_settings["scrap_data_save_path"]
        now = time.localtime()
        uniq_file_name = str(now.tm_year) +"_"+ str(now.tm_mon) +"_"+ str(now.tm_mday) +"_"+ str(now.tm_hour) +"_"+ str(now.tm_min) +"_"+ str(now.tm_sec)
        file_path =f"{scrap_data_save_path}/{uniq_file_name}_file_{channel}.txt"
        return file_path

    def get_site_alias(self, site):
        app_settings = cmn.get_settings(site=site)
        alias = app_settings["alias"]
        return alias

    def generate_s3_file_key(self, file_path, task_no, site, channel):
        """파일 경로에서 확장명을 추출하고 timestamp와 결합하여 S3 파일 키 생성"""
        # 파일 확장명 추출
        file_extension = os.path.splitext(file_path)[1]  # .txt, .csv 등
        
        # timestamp 생성
        now = time.localtime()
        timestamp = f"{now.tm_year}{now.tm_mon:02d}{now.tm_mday:02d}_{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}"
        
        # S3 파일 키 생성: channel/data_type/task_번호_timestamp.확장명
        s3_file_key = f"{site}/{channel}/task_{task_no}_{timestamp}{file_extension}"
        
        return s3_file_key

    def save(self, task_no, channel, create_file_name, item_count):
        p = re.compile(f"""\n+""")
        result = self.dao.select_pipe_task_id(task_no)
        row = result[0] if len(result) > 0 else None
        if row is not None:
          site_alias = self.get_site_alias(self.site)
          pipe_task_id = row["pipe_task_id"]
          
          s3_file_key = self.generate_s3_file_key(create_file_name, task_no, self.site, channel)
          print(f"생성된 S3 파일 키: {s3_file_key}")
          
          out_file = open(create_file_name, 'r', encoding='utf-8')
          data_list = []
          for lines in out_file:
            line = lines.split("\t")
              
            print(line)
              
            try :
                title = str(line[0]).strip()
            except Exception as err:
                print("0 >> ", err)
                title = ""
            try:
                url = str(line[1]).strip()
            except Exception as err:
                print("1 >> ", err)
                url = ""
            try :
                text = str(line[2]).strip()
                re_text = p.sub(" ", text)
            except Exception as err:
                print("2 >> ", err)
                text = ""
                re_text = ""
            
            target = {
                'pipe_task_id' : str(pipe_task_id).strip(),
                'site'         : site_alias,
                'channel'      : str(channel).strip(),
                'title'        : str(title).strip(),
                'url'          : str(url).strip(),
                'text'         : str(re_text).strip()
            }
            data_list.append(target)
            # print("data_list", data_list)
            try:  
              if len(data_list) > 100:
                s3_file_url, file_size = upload_file_to_s3(create_file_name)
                update_s3_file_url(task_no, self.site, s3_file_url, file_size)
                data_list = []
                print(f"task-[{task_no}]의 ({len(data_list)})개의 데이터가 S3에 저장되었습니다.")
            except Exception as err:
                print("3 >> ", err)
                pass
          
          try :                
            if len(data_list) > 0:
              # insert_list_to_es(data_list, es_index_name, "channel")
              s3_file_url, file_size = upload_file_to_s3(create_file_name)
              update_s3_file_url(task_no, self.site, s3_file_url, file_size)
              print(f"task-[{task_no}]의 ({len(data_list)})개의 데이터가 S3에 저장되었습니다.")
          except Exception as err:
            print("4 >> ", err)
            pass

          out_file.close()
          
          self.dao.update_scrap_status_count(task_no, item_count)

          try:
            os.remove(create_file_name)
          except Exception as err:
            print("5 >> ", err)
            pass