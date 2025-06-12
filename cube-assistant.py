import os, sys, yaml
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from earthling.service.ComAssistant import *
from earthling.service.Logging import log

import time, json, application.settings as settings
from handler.earthling_dao import get_dao, update_state_to_finish, get_collection_cond

def set_directory(site):
    log_file = settings.LOG_DATA_SAVE_PATH
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
        app_settings = app_settings[site]
        
    file_name = app_settings["scrap_data_save_path"]
    
    if not os.path.exists(log_file): os.makedirs(log_file)
    os.chmod(log_file, 0o777)
    
    if not os.path.exists(file_name): os.makedirs(file_name)
    os.chmod(file_name, 0o77)

def get_crawler(site):
    crawler = None
    dao = get_dao(site)
    from application.BaseCrawler import BaseCrawler
    crawler = BaseCrawler(site, dao)
    return crawler

def action(message):

    task_no = message["task_no"]
    data_type = json.loads(message["message"])
    site = data_type["site"]
    channel = data_type["channel"]
    set_directory(site)

    result = get_collection_cond(task_no, site)
    row = result[0] if len(result) else None
    if row is not None:
        crawler = get_crawler(site)
        crawler.factory(task_no, row, site, channel)
        update_state_to_finish(task_no, site)
    else:
        # print(f"다음의 쿼리에서 검색된 레코드가 없습니다 => {query}")
        print(f"데이터베이스 접속 정보를 확인하세요. 개발? 운영?")
        print(f"From cube-assistant.py")

if __name__ == "__main__":
    run(action)
