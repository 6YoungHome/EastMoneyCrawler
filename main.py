from crawler import PostCrawler
from crawler import CommentCrawler
import threading
import multiprocessing
from multiprocessing import Queue, Lock
import time
import datetime


def task_split_generate(stock_pool_path):
    q = Queue()
    with open(stock_pool_path) as f:
        stock_pool = f.readlines()
        
    for stock_code in stock_pool:
        stock_code = stock_code.strip().split(".")[0]
        q.put(stock_code)
    
    return q


def post_thread_update(thread_num, stock_code_quene, process_name, log_path, lock):
    def post_thread_yesterday(stock_symbol):
        post_crawler = PostCrawler(stock_symbol)
        post_crawler.crawl_oneday_post(date_diff=1)
    
    count = 1
    while True:
        stock_count = 0
        thread_pool = []
        start = time.time()
        for i in range(thread_num):
            if not stock_code_quene.empty():
                stock_code = stock_code_quene.get()
                stock_count += 1
            elif thread_pool == []:
                return 
            else:
                break 
            thread_pool.append(threading.Thread(target=post_thread_yesterday, args=(stock_code,)))

        for t in thread_pool:
            t.start()
            
        for t in thread_pool:
            t.join()
        lock.acquire()
        with open(f"{log_path}update_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
            f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t Tasks {count} of {process_name} success!!!, 耗时{round(time.time()-start, 2)}s, 获取 {stock_count} 只股票\n"])
        count += 1
        lock.release()


def post_thread_download(thread_num, stock_code_quene, process_name, log_path, lock):
    def post_thread_all(stock_symbol):
        post_crawler = PostCrawler(stock_symbol)
        post_crawler.crawl_by_page(1, 0)
    
    count = 1
    while True:
        stock_count = 0
        thread_pool = []
        start = time.time()
        for i in range(thread_num):
            if not stock_code_quene.empty():
                stock_code = stock_code_quene.get()
                stock_count += 1
            elif thread_pool == []:
                return 
            else:
                break 
            thread_pool.append(threading.Thread(target=post_thread_all, args=(stock_code,)))

        for t in thread_pool:
            t.start()
            
        for t in thread_pool:
            t.join()
        
        lock.acquire()
        with open(f"{log_path}download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
            f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t Tasks {count} of {process_name} success!!!, 耗时{round(time.time()-start, 2)}s, 获取 {stock_count} 只股票\n"])
        count += 1
        lock.release()
        
        
if __name__ == "__main__":
    # 全量股票更新程序
    process_num = 3
    thread_num = 10
    stock_pool_path = 'stock_pool.txt'
    log_path = "./log/"
    
    stock_code_quene = task_split_generate(stock_pool_path)
    lock = Lock()
    
    process_pool = []
    start = time.time()
    for i in range(process_num):
        process_pool.append(
            multiprocessing.Process(
                target=post_thread_download, 
                args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock)
            )
        )

    for p in process_pool:
        p.start()
        
    for p in process_pool:
        p.join()
        
    # # “昨日”更新程序
    # process_num = 2
    # thread_num = 30
    # stock_pool_path = 'stock_pool.txt'
    # log_path = "./log/"
    
    # stock_code_quene = task_split_generate(stock_pool_path)
    # lock = Lock()
    
    # process_pool = []
    # start = time.time()
    # for i in range(process_num):
    #     process_pool.append(
    #         multiprocessing.Process(
    #             target=post_thread_update, 
    #             args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock)
    #         )
    #     )

    # for p in process_pool:
    #     p.start()
        
    # for p in process_pool:
    #     p.join()
    
    
