import os, sys, yaml

from earthling.query import get_collection_cond, update_state_to_finish

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from earthling.service.ComAssistant import *
import json, application.settings as settings

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
    from application.BaseCrawler import BaseCrawler
    crawler = BaseCrawler(site)
    return crawler

def action(message):

    task_no = message["task_no"]
    data_type = json.loads(message["message"])
    site = data_type["site"]
    channel = data_type["channel"]
    set_directory(site)

    result = get_collection_cond(task_no)
    row = result[0] if len(result) else None
    if row is not None:
        crawler = get_crawler(site)
        crawler.factory(task_no, row, site, channel)        
        update_state_to_finish(task_no)
    else:      
        print(f"Confirm DB connection info.")

if __name__ == "__main__":
    run(action)
