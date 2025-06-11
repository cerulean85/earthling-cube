import os, sys, time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import yaml, random, pickle
from service.Logging import log

def get_dao(channel=""):
  dao_list = []
  with open("earthling/handler/dao/settings.yaml") as f:
    dao_settings = yaml.load(f, Loader=yaml.FullLoader)

    if channel != "":
      channel_setting = dao_settings.get(channel)
      dao_file_path = channel_setting.get("dao_file_path")
      with open(dao_file_path, "rb") as file:
          dao = pickle.load(file)
      return dao

    for channel in list(dao_settings.keys()):
      dao = None
      channel_setting = dao_settings.get(channel)
      dao_file_path = channel_setting.get("dao_file_path")
      with open(dao_file_path, "rb") as file:
        dao = pickle.load(file)
        dao_list.append(dao)

  return dao_list



def select_wait_task():
  daos = get_dao()
  random.shuffle(daos)
  # print(daos)
  result = []
  for dao in daos:
      q_result = dao.select_wait_task()
      if "list" in str(type(q_result)):
          result = result + q_result
      time.sleep(1)
  return result


def update_state_to_wait(no, channel):
  dao = get_dao(channel)
  dao.update_state_to_wait(no)


def update_state_to_start(no, channel, ass_addr):
  dao = get_dao(channel)
  dao.update_state_to_start(no, ass_addr)


def update_state_to_finish(no, channel):
  dao = get_dao(channel)
  dao.update_state_to_finish(no)


def get_collection_cond(no, channel):
  dao = get_dao(channel)
  return dao.get_collection_cond(no)


def update_state_S_to_Y(channel):
  dao = get_dao(channel)
  return dao.update_state_S_to_Y()


def update_s3_file_url(no, channel, s3_file_url, file_size):
  dao = get_dao(channel)
  return dao.update_s3_file_url(no, s3_file_url, file_size)