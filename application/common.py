from enum import Enum
import os, yaml, application.settings as settings
import time
from earthling.connector.s3_module import upload_file_to_s3, upload_from_buffer_to_s3
from earthling.service.Logging import log

class AppType(Enum):
    SEARCH = "search"
    CLEAN = "clean"
    FREQUENCY = "frequency"
    TFIDF = "tfidf"
    CONCOR = "concor"

def get_settings():
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
    return app_settings

def get_site_settings(site='', channel=''):
    if site != '': 
      set_site_dir(site)
            
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
        app_settings = app_settings[site]
        if channel != '':
            app_settings = app_settings.get(channel)
    return app_settings

def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    os.chmod(dir_path, 0o777)  

def set_dir(app_type: AppType):
    app_settings = get_settings()
    dir_path = app_settings[f"{app_type.value}_data_save_path"]
    create_dir(dir_path)    

def set_site_dir(site):
    app_settings = get_settings()
    app_settings = app_settings[site]
    dir_path = app_settings["search_data_save_path"]
    create_dir(dir_path)

def get_out_filepath(app_type: AppType):
    set_dir(app_type)
    app_settings = get_settings()    
    save_path = app_settings[f"{app_type.value}_data_save_path"]
    now = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    return f"{save_path}/{app_type.value}_{now}.json"

def get_save_filename(app_type: AppType):
    now = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    return f"{app_type.value}_{now}.json"

def save_to_s3_and_update(query, task_no, file_name):
    try:
        s3_file_url, file_size = upload_file_to_s3(file_name)
        query.update_s3_file_url(task_no, s3_file_url, file_size)
        print(f">> File: [{file_name}] Saved to S3.")
    except Exception as err:
        print("Save: S3 File >> ", err)

def save_to_s3_and_update_with_buffer(query, task_no, file_name, buffer):
    try:
        s3_file_url, file_size = upload_from_buffer_to_s3(buffer, file_name)
        query.update_s3_file_url(task_no, s3_file_url, file_size)
        print(f">> File: [{file_name}] Saved to S3.")
    except Exception as err:
        print("Save: S3 Buffer >> ", err)



def convert_morph_to_json(morph_data):
    result = []
    for idx, sentence in enumerate(morph_data):
        tokens = [{"word": token[0], "pos": token[1]} for token in sentence]
        result.append({
            "sentence_index": idx,
            "tokens": tokens
        })
    return result

def convert_json_to_morph(json_data):
    original_format = []
    for sentence in json_data:
        tokens = sentence["tokens"]
        token_list = [[token["word"], token["pos"]] for token in tokens]
        original_format.append(token_list)
    return original_format

