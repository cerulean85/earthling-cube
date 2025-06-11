import os, yaml, application.settings as settings
import http.cookiejar as cookielib
from earthling.service.Logging import log

def get_chrome_driver_path():
    log_file = settings.LOG_DATA_SAVE_PATH
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    chrome_driver_path = ''
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
        chrome_driver_path = app_settings["chrome_driver_path"]
    return chrome_driver_path

def get_settings(channel='', data_type=''):
    log_file = settings.LOG_DATA_SAVE_PATH
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
        chrome_driver_path = app_settings["chrome_driver_path"]
        app_settings = app_settings[channel]
        if data_type != '':
            app_settings = app_settings.get(data_type)

    return app_settings


def get_cookie_jar(channel, data_name): # '.daum_blog-cookie'
    cookie_name = f".{channel}_{data_name}-cookie"
    home_folder = os.getenv('HOME')
    if not home_folder:
        home_folder = os.getenv('USERHOME')
        if not home_folder:
            home_folder = '.'   # Use the current folder on error.
    cookie_jar = cookielib.LWPCookieJar(os.path.join(home_folder, cookie_name))
    return cookie_jar

def proc_html_status(html_status):
    print(f"수집 작업이 비정상적으로 종료되었습니다 (HTTP STATUS: {html_status})")


# get_settings("naver", "blog")