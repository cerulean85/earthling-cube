import settings, yaml, os
import http.cookiejar as cookielib

def get_chrome_driver_path():
    app_sttings_path = settings.APP_SETTINGS_PATH
    app_settings = None
    chrome_driver_path = ''
    with open(app_sttings_path) as f:
        app_settings = yaml.load(f, Loader=yaml.FullLoader)
        chrome_driver_path = app_settings["chrome_driver_path"]
    return chrome_driver_path

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