import os, sys, time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import random
from service.Logging import log
from handler.dao.NaverDAO import NaverDAO

dao_dict = {
  "naver": NaverDAO(),
}

def get_dao(site=None):
  if site is None: 
    return None
  return dao_dict[site]
      
def select_wait_task():
  daos = list(dao_dict.values())
  random.shuffle(daos)

  result = []
  for dao in daos:
      q_result = dao.select_wait_task()
      if "list" in str(type(q_result)):
          result = result + q_result
      time.sleep(1)
  return result

def update_state_to_wait(no, site):
  dao = get_dao(site)
  dao.update_state_to_wait(no)

def update_state_to_start(no, site, ass_addr):
  dao = get_dao(site)
  dao.update_state_to_start(no, ass_addr)

def update_state_to_finish(no, site):
  dao = get_dao(site)
  dao.update_state_to_finish(no)

def get_collection_cond(no, site):
  dao = get_dao(site)
  return dao.get_collection_cond(no)

def update_state_S_to_Y(site):
  dao = get_dao(site)
  return dao.update_state_S_to_Y()

def update_s3_file_url(no, site, s3_file_url, file_size):
  dao = get_dao(site)
  return dao.update_s3_file_url(no, s3_file_url, file_size)

def select_pipe_task_id(site, scrap_status_id):
  dao = get_dao(site)
  return dao.select_pipe_task_id(scrap_status_id)

def update_scrap_status_count(site, scrap_status_id, count):
  dao = get_dao(site)
  return dao.update_scrap_status_count(scrap_status_id, count)

def update_scrap_status_start_date_to_now(site, scrap_status_id):
  dao = get_dao(site)
  return dao.update_scrap_status_start_date_to_now(scrap_status_id)

def update_state_to_N(site, scrap_status_id):
  dao = get_dao(site)
  return dao.update_state_to_N(scrap_status_id)
    
def update_state_to_Y(site, scrap_status_id):
  dao = get_dao(site)
  return dao.update_state_to_Y(scrap_status_id)