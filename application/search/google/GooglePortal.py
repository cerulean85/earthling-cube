import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service  # Service 추가
from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from .GoogleBase import GoogleBase
from datetime import datetime, timedelta
from earthling.service.Logging import log
import undetected_chromedriver as uc

class GooglePortal(GoogleBase):

    def get_url(self, keyword, date_start="", date_end="", start=0):
        url = f"https://www.google.com/search?q={keyword}&num=50&start={start}&tbs=cdr:1,cd_min:{date_start},cd_max:{date_end}"

        print(url)
        return url

    def search(
        self,
        keyword,
        idx_num,
        stop=0,
        date_start="",
        date_end="",
        num=1,
        out_filepath="",
    ):

        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        browser = uc.Chrome(options=options, version_main=None) 

        d_start = datetime.strptime(date_start, "%Y-%m-%d")
        d_end = datetime.strptime(date_end, "%Y-%m-%d")
        dates = [(d_start + timedelta(days=i)).strftime("%m/%d/%Y") for i in range((d_end-d_start).days+1)]
        dates = sorted(dates, reverse=True)

        out_file = open(out_filepath, "a", encoding="utf-8")
        creat_file_name = out_file.name

        fin, current_count = False, 0
        for i in range(1, len(dates)):        
            start_date, end_date = dates[i], dates[i-1]
            print(start_date, end_date)
            
            url = self.get_url(keyword, start_date, end_date, 0)
            html_status = self.get_page(url, browser)
            if html_status != 200:
                return creat_file_name, current_count, html_status

            
            html_element = browser.page_source
            soup = BeautifulSoup(
                str(html_element), "html.parser", from_encoding="utf-8"
            )

            settings = self.get_settings("portal")
            web_list = soup.findAll("div", {"class": "MjjYud"})

            print(len(web_list))
            for i in web_list:
                try:  
                    title = i.find("h3").text.strip()
                    print(title)
                except:
                    title = ""

                try:  
                    link = i.find("a").get('href')
                    print(link)
                except:
                    link = ""

                try:
                    # for span in i.find_all("span", {"class": "YrbPuc"}):
                    #     span.decompose()  # span 요소를 완전히 제거
                    contents = i.find("div", {"class": "kb0PBd"}).text.strip()
                    print(contents)
                except:
                    contents = ""

                try:
                  out_file.write(search_text + "\n")                
                  search_text = title + "\t" + link + "\t" + contents
                except Exception as err:
                    log.debug(err)
                    continue
            
            time.sleep(settings["delay_time"])        
            current_count += len(web_list)
            if current_count >= settings["max_count"]:
                break          

        browser.quit()
        out_file.close()

        return creat_file_name, current_count, html_status


if __name__ == "__main__":
    google = GooglePortal()
    google.search(
        "코로나",
        1,
        stop=0,
        date_start="2023-01-01",
        date_end="2023-01-31",
        dir_id="0",
        num=1,
        start=1,
        pause=2.0,
        out_filepath="test.txt",
    )
