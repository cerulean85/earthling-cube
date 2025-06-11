import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from earthling.service.Logging import log
from earthling.handler.dao.BaseDAO import BaseDAO
from earthling.handler.dao.NaverDAO import NaverDAO

dao_class = {
    "naver": NaverDAO(),
}

from application.naver.NaverBase import NaverBase
from application.naver.NaverWeb import NaverWeb
from application.naver.NaverBlog import NaverBlog
from application.naver.NaverNews import NaverNews

data_class = {
  "naver": {
      "base": NaverBase(),  
      "web": NaverWeb(), 
      "blog": NaverBlog(), 
      "news": NaverNews()
  },
}


import pickle, yaml
if __name__ == "__main__":

    dao_settings = None
    with open("earthling/handler/dao/settings.yaml") as f:
        dao_settings = yaml.load(f, Loader=yaml.FullLoader)

        for channel in list(dao_settings.keys()):
            dao = None
            channel_setting = dao_settings.get(channel)
            dao_file_path = channel_setting.get("dao_file_path")
            dao = dao_class.get(channel)
            if dao is None:
                continue
            
            with open(dao_file_path, 'wb') as file:
                print(dao_file_path)
                pickle.dump(dao, file)

    app_settings = None
    with open("application/settings.yaml") as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)

        for channel in list(app_settings.keys()):
            dao = None
            channel_setting = app_settings.get(channel)            
            if "dict" not in str(type(channel_setting)):
                continue
            
            serialized_file_path = channel_setting.get("serialized_file_path")
            data_types = channel_setting.get("data_types")

            save_file_path = f"{serialized_file_path}/base.pickle"
            with open(save_file_path, 'wb') as ff:
                print(save_file_path)
                target_data_class = data_class.get(channel).get("base")
                pickle.dump(target_data_class, ff)

            for data_type in data_types:
                target_data_class = data_class.get(channel).get(data_type)

                if target_data_class is None:
                    continue
                
                save_file_path = f"{serialized_file_path}/{data_type}.pickle"
                with open(save_file_path, 'wb') as ff:
                    print(save_file_path)
                    pickle.dump(target_data_class, ff)