import os, sys, yaml


from earthling.query import (
    PipeTaskStatus,
    QueryPipeTask,
    QueryPipeTaskSearch
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from earthling.service.ComAssistant import *
import json, application.settings as settings
from application.search.SearchApplication import SearchApplication
from application.clean.CleanApplication import CleanApplication
from application.frequency.FrequencyApplication import FrequencyApplication
from application.tfidf.TfidfApplication import TfidfApplication
from application.concor.ConcorApplication import ConcorApplication

def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    os.chmod(dir_path, 0o77)  

def set_log_dir():
    log_file = settings.LOG_DATA_SAVE_PATH
    if not os.path.exists(log_file):
        os.makedirs(log_file)
    os.chmod(log_file, 0o777)

def get_app_settings():
    app_settings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    with open(app_settings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
    return app_settings

def search(task, task_no, site, channel):
    q_inst = QueryPipeTaskSearch()
    set_log_dir()
    
    if task is not None:
        q_inst.update_state_to_pending(task_no)
        app = SearchApplication(site)
        app.execute(task_no, task, site, channel)
        q_inst.update_state_to_finish(task_no)

    else:
        print(f"Confirm DB connection info.")
    

def action(message):

    task_no = message["task_no"]
    task = json.loads(message["message"])
    task_type = task["task_type"]

    if task_type == PipeTaskStatus.SEARCH.value:        
        site, channel = task["site"], task["channel"]
        search(task, task_no, site, channel)

    if task_type == PipeTaskStatus.CLEAN.value:        
        app = CleanApplication()
        app.execute(task)

    if task_type == PipeTaskStatus.FREQUENCY.value:        
        app = FrequencyApplication()
        app.execute(task)

    if task_type == PipeTaskStatus.TFIDF.value:        
        app = TfidfApplication()
        app.execute(task)

    if task_type == PipeTaskStatus.CONCOR.value:        
        app = ConcorApplication()
        app.execute(task)

if __name__ == "__main__":
    run(action)
