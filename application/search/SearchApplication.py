from enum import Enum
import time, os, re, sys, application.common as cmn
from dataclasses import dataclass
from earthling.query import PipeTaskStatus, QueryPipeTaskSearch

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from application.common import *
from application.search.naver.NaverBase import NaverBase
from application.search.naver.NaverWeb import NaverWeb
from application.search.naver.NaverBlog import NaverBlog
from application.search.naver.NaverNews import NaverNews

# Data classes for structured data
@dataclass
class SearchData:
    keyword: str
    task_no: str
    stop: int
    date_start: str
    date_end: str
    out_filepath: str

@dataclass
class ParsedLine:
    pipe_line_id: str
    site: str
    channel: str
    title: str
    url: str
    text: str

class SearchSiteType(Enum):
    NAVER = "naver"

class SearchChannelType(Enum):
    WEB = "web"
    BLOG = "blog"
    NEWS = "news"


search_class = {
  SearchSiteType.NAVER.value: {
      SearchChannelType.WEB.value: NaverWeb, 
      SearchChannelType.BLOG.value: NaverBlog, 
      SearchChannelType.NEWS.value: NaverNews
  },
}

class SearchApplication:
    def __init__(self, site):
        self.site = site
        self.query = QueryPipeTaskSearch()

    def get_search_object(self, site: str, channel: str):
        return search_class[site][channel]()

    def exec_search(self, poly, data: SearchData):
        return poly.search(
            data.keyword,
            idx_num=str(data.task_no),
            stop=data.stop,
            date_start=data.date_start.split(' ')[0],
            date_end=data.date_end.split(' ')[0],
            out_filepath=data.out_filepath
        )

    def execute(self, task_no, row, site, channel):
        if row is None:
            print(f"Can't collect task-[{task_no}] Data. Confirm KEYWORD_LIST or SCRAW_NAVER_DATAINFO [{task_no}]from table.")
            return

        keyword = row["search_keyword"]
        date_start = row["search_start_date"]
        date_end = row["search_end_date"]
        print(f"Collecting info => Site: {site}, Channel: {channel}, Keyword: {keyword}, Date: {date_start} ~ {date_end}")

        search_object = self.get_search_object(site, channel)
        if search_object is not None:
            self.query.update_search_status_start_date_to_now(task_no)
            search_data = SearchData(
                keyword=keyword,
                task_no=str(task_no),
                stop=100,
                date_start=date_start,
                date_end=date_end,
                out_filepath=self.get_out_filepath(site, channel)
            )
            create_file_name, item_count, html_status = self.exec_search(search_object, search_data)
            if html_status == 200:
                self.query.update_state_to_completed(task_no)
                self.save(task_no, channel, create_file_name, item_count)
                print(f"Finished to collect [task-{site}-{channel}-{task_no}] data")
                self.query.update_state_to_pending_about_other_task(PipeTaskStatus.CLEAN, task_no)
            else:
                self.query.update_state_to_pending(task_no)
                print(f"Failed to collect data (HTML STATUS: {html_status})")
                penalty_delay_time = settings['penalty_delay_time']
                time.sleep(penalty_delay_time)

    def get_out_filepath(self, site, channel):
        app_settings = cmn.get_site_settings(site=site)
        search_data_save_path = app_settings["search_data_save_path"]
        now = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
        return f"{search_data_save_path}/{now}_file_{channel}.txt"

    def get_site_alias(self, site):
        return cmn.get_site_settings(site=site)["alias"]

    def parse_line(self, line, regex, pipe_line_id, site_alias, channel):
        try:
            title = str(line[0]).strip()
        except Exception as err:
            print("Save: 0 >> ", err)
            title = ""
        try:
            url = str(line[1]).strip()
        except Exception as err:
            print("Save: 1 >> ", err)
            url = ""
        try:
            text = str(line[2]).strip()
            re_text = regex.sub(" ", text)
        except Exception as err:
            print("Save: 2 >> ", err)
            text = ""
            re_text = ""
        return ParsedLine(
            pipe_line_id=str(pipe_line_id).strip(),
            site=site_alias,
            channel=str(channel).strip(),
            title=title,
            url=url,
            text=re_text
        )

    def save(self, task_no, channel, create_file_name, item_count):
        regex = re.compile(r"\n+")
        row = self.query.get_task_by_id(task_no)
        if not row:
            return

        site_alias = self.get_site_alias(self.site)
        pipe_line_id = row.pipe_line_id

        data_list = []
        try:
            with open(create_file_name, 'r', encoding='utf-8') as out_file:
                for lines in out_file:
                    line = lines.split("\t")
                    parsed = self.parse_line(line, regex, pipe_line_id, site_alias, channel)
                    data_list.append(parsed)
                    if len(data_list) > 100:
                        cmn.save_to_s3_and_update(self.query, task_no, create_file_name)
                        data_list = []
                if data_list:
                    cmn.save_to_s3_and_update(self.query, task_no, create_file_name)
        except Exception as err:
            print("Save: file read >> ", err)

        self.query.update_search_status_count(task_no, item_count)
        try:
            os.remove(create_file_name)
        except Exception as err:
            print("Save: file remove >> ", err)