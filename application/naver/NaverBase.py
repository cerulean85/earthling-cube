import yaml, common as cmn
import urllib.request as urllib2
import requests
# from earthling.service.Logging import log
class NaverBase:

    def get_page(self, url, driver, delay_time=3):
        resp = requests.get(url)
        html_status = resp.status_code
        if html_status != 200:
            cmn.proc_html_status(html_status)
            return html_status

        driver.implicitly_wait(delay_time)
        driver.get(url)
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
        dir_id = '0', 
        num = 1, 
        start = 0, 
        pause = 2.0, 
        out_filepath=''):
        pass
    
    def get_url(self, keyword, date_start='', date_end='', start=0):
        pass

    def get_chrome_driver_path(self):
        return cmn.get_chrome_driver_path()
        # log_file = settings.LOG_DATA_SAVE_PATH
        # app_sttings_path = settings.APP_SETTINGS_PATH
        # app_settings = None
        # chrome_driver_path = ''
        # with open(app_sttings_path) as f:
        #     app_settings = yaml.load(f, Loader=yaml.FullLoader)
        #     chrome_driver_path = app_settings["chrome_driver_path"]
        # return chrome_driver_path

    def get_settings(self, data_type=''):
        return cmn.get_settings("naver", data_type)
        # log_file = settings.LOG_DATA_SAVE_PATH
        # app_sttings_path = settings.APP_SETTINGS_PATH
        # app_settings = None
        # with open(app_sttings_path) as f:
        #     app_settings = yaml.load(f, Loader=yaml.FullLoader)
        #     self.chrome_driver_path = app_settings["chrome_driver_path"]
        #     app_settings = app_settings['naver']
        #     if data_type != '':
        #         app_settings = app_settings.get(data_type)

        # return app_settings

    def set_cookie_jar(self, data_type):
        self.cookie_jar = cmn.get_cookie_jar("naver", data_type)
        try:
            self.cookie_jar.load()
        except Exception:
            pass