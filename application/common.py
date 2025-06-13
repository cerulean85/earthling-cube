import os, yaml, application.settings as settings
import http.cookiejar as cookielib
from earthling.service.Logging import log

def get_chrome_driver_path():
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    chrome_driver_path = ''
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
        chrome_driver_path = app_settings["chrome_driver_path"]
    return chrome_driver_path

def get_settings(site='', channel=''):
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
        app_settings = app_settings[site]
        if channel != '':
            app_settings = app_settings.get(channel)
    return app_settings


def get_cookie_jar(site, data_name):
    cookie_name = f".{site}_{data_name}-cookie"
    home_folder = os.getenv('HOME')
    if not home_folder:
        home_folder = os.getenv('USERHOME')
        if not home_folder:
            home_folder = '.'
    cookie_jar = cookielib.LWPCookieJar(os.path.join(home_folder, cookie_name))
    return cookie_jar

def proc_html_status(html_status):
    print(f"Exited abnormally to collect. (HTTP STATUS: {html_status})")

def get_site_channel_object(site, channel):
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

    return data_class[site][channel]