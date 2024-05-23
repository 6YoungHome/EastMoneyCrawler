from crawler import *
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


def post_update_thread(thread_num, stock_code_quene, process_name, log_path, lock):
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
        with open(f"{log_path}post_update_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
            f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t Tasks {count} of {process_name} success!!!, 耗时{round(time.time()-start, 2)}s, 获取 {stock_count} 只股票\n"])
        count += 1
        lock.release()


def post_download_thread(thread_num, stock_code_quene, process_name, log_path, lock):
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
        with open(f"{log_path}post_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
            f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t Tasks {count} of {process_name} success!!!, 耗时{round(time.time()-start, 2)}s, 获取 {stock_count} 只股票\n"])
        count += 1
        lock.release()
        

def comment_download_thread(thread_num, stock_code_quene, process_name, log_path, lock, start_date=None, end_date=None):
    def comment_thread_all(stock_symbol, start_date, end_date):
        comment_crawler = CommentCrawler(stock_symbol)
        comment_crawler.find_by_date(start_date=start_date, end_date=end_date)
        comment_crawler.crawl_comment_info()
    
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
            thread_pool.append(threading.Thread(target=comment_thread_all, args=(stock_code, start_date, end_date)))

        for t in thread_pool:
            t.start()
            
        for t in thread_pool:
            t.join()
        
        lock.acquire()
        with open(f"{log_path}comment_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            du = round(time.time()-start, 2)
            f.writelines([f"{now} \t Tasks {count} of {process_name} success!!!, 耗时{du}s, 获取 {stock_count} 只股票 {start_date} 到 {end_date} 的评论\n"])
        count += 1
        lock.release()
        

def post_text_download_thread(thread_num, stock_code_quene, process_name, log_path, lock, start_date=None, end_date=None):
    def post_text_thread_all(stock_symbol, start_date, end_date):
        ptc = PostTextCrawler(stock_symbol)
        ptc.find_by_date(start_date=start_date, end_date=end_date)
        ptc.crawl_post_text_info()
    
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
            thread_pool.append(threading.Thread(target=post_text_thread_all, args=(stock_code, start_date, end_date)))

        for t in thread_pool:
            t.start()
            
        for t in thread_pool:
            t.join()
        
        lock.acquire()
        with open(f"{log_path}post_text_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            du = round(time.time()-start, 2)
            f.writelines([f"{now} \t Tasks {count} of {process_name} success!!!, 耗时{du}s, 获取 {stock_count} 只股票 {start_date} 到 {end_date} 的内容\n"])
        count += 1
        lock.release()