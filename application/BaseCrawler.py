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

    def get_data_poly(self, channel, data_type):
        app_settings = cmn.get_settings(channel=channel)
        serialized_file_path = app_settings.get("serialized_file_path")
        poly_path = f"{serialized_file_path}/{data_type}.pickle"
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

    def __init__(self, channel, dao):
        self.channel = channel
        self.base = self.get_data_poly(channel, "base")
        self.dao = dao

    def factory(self, task_no, row, channel, data_type):
        if row == None:
            print(f"task-[{task_no}]를 수집할 수 없습니다. KEYWORD_LIST 혹은 SCRAW_NAVER_DATAINFO 테이블에서 [{task_no}]를 확인하세요.")
            return

        print("channel", channel)
        print("data_type", data_type)
        app_settings = cmn.get_settings(channel=channel)
        table_name = app_settings["scrap_table_name"]

        keyword, date_start, date_end = row["keyword"], row["scrap_start_date"], row["scrap_end_date"]
        print(f"수집정보 => 채널: {channel}, 유형: {data_type}, 키워드: {keyword}, 기간: {date_start} ~ {date_end}")

        poly = self.get_data_poly(channel, data_type) 
        if poly is not None:
            self.dao.exec(f"UPDATE {table_name} SET start_date = now() WHERE id={task_no}")

            search_data = {
                "keyword": keyword, 
                "task_no": str(task_no), 
                "stop": 100, 
                "date_start": date_start, 
                "date_end": date_end,
                "out_filepath": self.get_out_filepath(channel, data_type)
            }

            create_file_name, item_count, html_status = self.exec_search(poly, search_data)

            print("완료됨!!!", item_count)
            print("html_status", html_status)
            print("create_file_name", create_file_name)

            if html_status == 200:
                self.dao.exec(f"UPDATE {table_name} SET state = 'N', end_date = now() WHERE id={task_no}")
                self.save(task_no, data_type, create_file_name, item_count)
                print(f"데이터 수집을 정상적으로 완료하였습니다.")

            else:
                self.dao.exec(f"UPDATE {table_name} SET state = 'Y', end_date = now() WHERE id={task_no}")
                print(f"데이터 수집에 실패하였습니다. (HTML STATUS: {html_status})")
                app_settings = cmn.get_settings(channel=channel)
                penalty_delay_time = settings['penalty_delay_time']
                time.sleep(penalty_delay_time)
    
    def get_out_filepath(self, channel, data_type):
        app_settings = cmn.get_settings(channel=channel)
        scrap_data_save_path = app_settings["scrap_data_save_path"]
        now = time.localtime()
        uniq_file_name = str(now.tm_year) +"_"+ str(now.tm_mon) +"_"+ str(now.tm_mday) +"_"+ str(now.tm_hour) +"_"+ str(now.tm_min) +"_"+ str(now.tm_sec)
        file_path =f"{scrap_data_save_path}/{uniq_file_name}_file_{data_type}.txt"
        return file_path

    # def get_es_index_name(self, channel):
    #     app_settings = cmn.get_settings(channel=channel)
    #     es_index_name = app_settings["es_index_name"]
    #     return es_index_name

    def get_channel_alias(self, channel):
        app_settings = cmn.get_settings(channel=channel)
        alias = app_settings["alias"]
        return alias

    def generate_s3_file_key(self, file_path, task_no, channel, data_type):
        """파일 경로에서 확장명을 추출하고 timestamp와 결합하여 S3 파일 키 생성"""
        # 파일 확장명 추출
        file_extension = os.path.splitext(file_path)[1]  # .txt, .csv 등
        
        # timestamp 생성
        now = time.localtime()
        timestamp = f"{now.tm_year}{now.tm_mon:02d}{now.tm_mday:02d}_{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}"
        
        # S3 파일 키 생성: channel/data_type/task_번호_timestamp.확장명
        s3_file_key = f"{channel}/{data_type}/task_{task_no}_{timestamp}{file_extension}"
        
        return s3_file_key

    def save(self, task_no, data_type, create_file_name, item_count):
        p = re.compile(f"""\n+""")
        app_settings = cmn.get_settings(self.channel)
        table_name = app_settings["scrap_table_name"]
        query = f"SELECT pipe_task_id from {table_name} where id = {task_no}"
        print(query)
        result = self.dao.exec(query)
        print(result)
        row = result[0] if len(result) > 0 else None
        if row is not None:
            # es_index_name = self.get_es_index_name(self.channel)
            channel_alias = self.get_channel_alias(self.channel)
            k_idx = row["pipe_task_id"]
            
            # S3 파일 키 생성
            s3_file_key = self.generate_s3_file_key(create_file_name, task_no, self.channel, data_type)
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
                    'idx'          : str(k_idx).strip(),
                    'kind'         : channel_alias,
                    'subKind'      : str(data_type).strip(),
                    'title'        : str(title).strip(),
                    'url'          : str(url).strip(),
                    'text'         : str(re_text).strip()
                }
                data_list.append(target)
                # print("data_list", data_list)
                try:  
                    if len(data_list) > 100:
                        # insert_list_to_es(data_list, es_index_name, "channel")
                        
                        s3_file_url, file_size = upload_file_to_s3(create_file_name)
                        update_s3_file_url(task_no, self.channel, s3_file_url, file_size)
                        data_list = []
                        print(f"task-[{task_no}]의 ({len(data_list)})개의 데이터가 S3에 저장되었습니다.")
                except Exception as err:
                    print("3 >> ", err)
                    pass
             
            try :
                
                if len(data_list) > 0:
                    # insert_list_to_es(data_list, es_index_name, "channel")
                    s3_file_url, file_size = upload_file_to_s3(create_file_name)
                    update_s3_file_url(task_no, self.channel, s3_file_url, file_size)
                    print(f"task-[{task_no}]의 ({len(data_list)})개의 데이터가 S3에 저장되었습니다.")
            except Exception as err:
                print("4 >> ", err)
                pass

            out_file.close()
            self.dao.exec(f"UPDATE {table_name} SET count={item_count} WHERE id = {task_no}")

            # try :
            #     nsize = os.path.getsize(create_file_name)
            #     result = self.dao.exec(f"SELECT mem_id, scrap_start_date, scrap_end_date, keyword from pipe_task where id ={k_idx}")
            #     row_user = result[0] if len(result) > 0 else None # cursor.fetchone()
            #     if row_user is not None:
            #         user_id, date_start, date_end, keyword = row_user["mem_id"], row_user["scrap_start_date"], row_user["scrap_end_date"], row_user["keyword"].replace("'", "''")
            #         file_size = float(nsize/1024)
            #         query = "INSERT INTO scrw_use_size (" \
            #                 "  keyword_idx, keyword, user_id, channel, sub_channel, scrw_size, start_date, end_date, site_kind" \
            #                 ") VALUES (" \
            #                 f"{k_idx}, '{re.escape(keyword)}', '{user_id}', '{channel_alias}', '{data_type}', {file_size}, '{date_start}', '{date_end}', 'channel'" \
            #                 ")"
            #         # print(query)
            #         self.dao.exec(query)

            # except Exception as err:
            #     print(err)
            #     pass

            try:
                os.remove(create_file_name)
            except Exception as err:
                print("5 >> ", err)
                pass