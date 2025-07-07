import yaml, os
import http.cookiejar as cookielib

def get_chrome_driver_path():
    """Chrome driver 경로를 가져옵니다."""
    # 프로젝트 루트에서 app_settings.yaml 찾기
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    app_settings_path = os.path.join(project_root, 'app_settings.yaml')
    
    chrome_driver_path = ''
    
    if not os.path.exists(app_settings_path):
        print(f"Warning: {app_settings_path} not found")
        return ''
        
    try:
        with open(app_settings_path, 'r', encoding='utf-8') as f:
            app_settings = yaml.load(f, Loader=yaml.FullLoader)
            chrome_driver_path = app_settings.get("chrome_driver_path", "")
    except Exception as e:
        print(f"Error reading settings: {e}")
        return ''
    
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