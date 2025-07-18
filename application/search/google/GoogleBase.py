import time
from application import common as cmn
from application.search import util
import urllib.request as urllib2
import requests

class GoogleBase:

    def get_page(self, url, driver, delay_time=3):
        resp = requests.get(url)
        html_status = resp.status_code
        if html_status != 200:
            util.proc_html_status(html_status)
            return html_status

        driver.implicitly_wait(delay_time)
        driver.get(url)
        time.sleep(5)  # 너무 짧으면 429 발생 가능
        return html_status

    def get_page_with_session(self, url):
        html = ''
        html_status = 200
        try:
            request = urllib2.Request(url)
            #request.add_header('User-Agent','Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0)')
            request.add_header('User-Agent','*/*')
            request.add_header('Accept', '*/*')
            request.add_header('Accept-Language', 'ko-kr,ko;q=0.8,en-us;q=0.5,en;q=0.3')
            request.add_header('Accept-Charset', 'ISO-8859-1,utf-8;q=0.7,*;q=0.7')
            request.add_header('Connection', 'keep-alive')
            request.add_header('Referer','http://www.naver.com')

            self.cookie_jar.add_cookie_header(request)

            response = urllib2.urlopen(request)
            html_status = response.status
            if html_status == 200:
                self.cookie_jar.extract_cookies(response, request)
                html = response.read()
            
            else:
                cmn.proc_html_status(html_status)

            response.close()
            self.cookie_jar.save()

        except Exception as err:
            # print(err)
            print(err)

        return html, html_status

    def search(self,
        keyword, 
        idx_num, 
        stop = 0, 
        date_start = '', 
        date_end = '', 
        num = 1, 
        out_filepath=''):
        pass
    
    def get_url(self, keyword, date_start='', date_end=''):
        pass

    def get_chrome_driver_path(self):
        return util.get_chrome_driver_path()

    def get_settings(self, channel=''):
        return cmn.get_site_settings("google", channel)

    def set_cookie_jar(self, channel):
        self.cookie_jar = util.get_cookie_jar("google", channel)
        try:
            self.cookie_jar.load()
        except Exception:
            pass