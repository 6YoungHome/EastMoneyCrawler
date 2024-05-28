import os


cfg = \
{
    "result_save_path": "result_save/",
    "log_path": "log/",
    "resource_path": "resource/",
    "stock_pool_path": "resource/stock_pool50.txt"
}
    
SAVE_PATH = cfg["result_save_path"]
LOG_PATH = cfg["log_path"]
RESOURE_PATH = cfg["resource_path"]
STOCK_POOL_PATH = cfg['stock_pool_path']

if not os.path.exists(SAVE_PATH):
    os.mkdir(SAVE_PATH)

if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)
