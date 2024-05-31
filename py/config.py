import os


cfg = \
{
    "result_save_path": "result_save/",
    "log_path": "log/",
    "resource_path": "resource/",
    "stock_pool_path": "resource/stock_pool.txt",
    # "stock_pool_path": "resource/stock_pool50.txt",
    # "driver_path": "resource\chromedriver\win64/125.0.6422.60\chromedriver.exe"
    "driver_path": "resource\chromedriver\win64/125.0.6422.60\chromedriver"
}
    
SAVE_PATH = cfg["result_save_path"]
LOG_PATH = cfg["log_path"]
RESOURE_PATH = cfg["resource_path"]
STOCK_POOL_PATH = cfg['stock_pool_path']
DRIVER_PATH = cfg['driver_path']

if not os.path.exists(SAVE_PATH):
    os.mkdir(SAVE_PATH)

if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)
