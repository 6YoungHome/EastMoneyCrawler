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


def post_update_thread(thread_num, stock_code_quene, process_name, lock):
    def post_thread_yesterday(stock_code_quene, semaphore):
        nonlocal stock_count
        with semaphore:
            if not stock_code_quene.empty():
                stock_code = stock_code_quene.get()
                stock_count += 1
            else:
                return 
            post_crawler = PostCrawler(stock_code)
            post_crawler.crawl_oneday_post(date_diff=1)
        
    semaphore = threading.BoundedSemaphore(thread_num)
    stock_count = 0
    start = time.time()
    
    while True:
        t = threading.Thread(target=post_thread_yesterday, args=(stock_code_quene, semaphore))
        t.start()
        t.join()
        if stock_count % thread_num == 0:
            lock.acquire()
            with open(LOG_PATH+f"post_update_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
            start = time.time()
            lock.release()
        if stock_code_quene.empty():
            lock.acquire()
            with open(LOG_PATH+f"post_update_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} finished!!!\n"])
            start = time.time()
            lock.release()
            break


def post_download_thread(thread_num, stock_code_quene, process_name, lock):
    def post_thread_all(stock_code_quene, semaphore):
        nonlocal stock_count
        with semaphore:
            if not stock_code_quene.empty():
                stock_code = stock_code_quene.get()
                stock_count += 1
            else:
                return 
            post_crawler = PostCrawler(stock_code)
            post_crawler.crawl_by_page(1, 0)
    
    semaphore = threading.BoundedSemaphore(thread_num)
    stock_count = 0
    start = time.time()
    
    while True:
        t = threading.Thread(target=post_thread_all, args=(stock_code_quene, semaphore))
        t.start()
        t.join()
        if stock_count % thread_num == 0:
            lock.acquire()
            with open(LOG_PATH+f"post_download_all_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
            start = time.time()
            lock.release()
        if stock_code_quene.empty():
            lock.acquire()
            with open(LOG_PATH+f"post_download_all_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} finished!!!\n"])
            start = time.time()
            lock.release()
            break


def post_get_history_thread(thread_num, stock_code_quene, process_name, start_date, lock):
    def post_thread_history(stock_code_quene, semaphore, start_date):
        nonlocal stock_count
        with semaphore:
            if not stock_code_quene.empty():
                stock_code = stock_code_quene.get()
                stock_count += 1
            else:
                return 
            post_crawler = PostCrawler(stock_code)
            post_crawler.crawl_history_post(start_date)
        
    semaphore = threading.BoundedSemaphore(thread_num)
    stock_count = 0
    start = time.time()
    
    while True:
        t = threading.Thread(target=post_thread_history, args=(stock_code_quene, semaphore, start_date))
        t.start()
        t.join()
        if stock_count % thread_num == 0:
            lock.acquire()
            with open(LOG_PATH+f"post_download_history_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
            start = time.time()
            lock.release()
        if stock_code_quene.empty():
            lock.acquire()
            with open(LOG_PATH+f"post_download_history_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} finished!!!\n"])
            start = time.time()
            lock.release()
            break
        
        
def comment_download_thread(thread_num, stock_code_quene, process_name, start_date, end_date, lock):
    def comment_thread_all(stock_code_quene, start_date, end_date, semaphore):
        nonlocal stock_count
        with semaphore:
            if not stock_code_quene.empty():
                stock_code = stock_code_quene.get()
                stock_count += 1
            else:
                return 
            comment_crawler = CommentCrawler(stock_code)
            comment_crawler.find_by_date(start_date=start_date, end_date=end_date)
            comment_crawler.crawl_comment_info()
        
    semaphore = threading.BoundedSemaphore(thread_num)
    stock_count = 0
    start = time.time()
    
    while True:
        t = threading.Thread(target=comment_thread_all, args=(stock_code_quene, start_date, end_date, semaphore))
        t.start()
        t.join()
        if stock_count % thread_num == 0:
            lock.acquire()
            with open(LOG_PATH+f"comment_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
            start = time.time()
            lock.release()
        if stock_code_quene.empty():
            lock.acquire()
            with open(LOG_PATH+f"comment_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} finished!!!\n"])
            start = time.time()
            lock.release()
            break

def post_text_download_thread(thread_num, stock_code_quene, process_name, start_date, end_date, lock):
    def post_text_thread_all(stock_code_quene, start_date, end_date, semaphore):
        nonlocal stock_count
        with semaphore:
            if not stock_code_quene.empty():
                stock_code = stock_code_quene.get()
                stock_count += 1
            else:
                return 
            ptc = PostTextCrawler(stock_code)
            ptc.find_by_date(start_date=start_date, end_date=end_date)
            ptc.crawl_post_text_info()
    
    semaphore = threading.BoundedSemaphore(thread_num)
    stock_count = 0
    start = time.time()
    
    while True:
        t = threading.Thread(target=post_text_thread_all, args=(stock_code_quene, start_date, end_date, semaphore))
        t.start()
        t.join()
        if stock_count % thread_num == 0:
            lock.acquire()
            with open(LOG_PATH+f"post_text_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
            start = time.time()
            lock.release()
        if stock_code_quene.empty():
            lock.acquire()
            with open(LOG_PATH+f"post_text_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} success!!!, 本次耗时{round(time.time()-start, 2)}s, 本进程累计获取 {stock_count} 只股票\n"])
                f.writelines([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \t {process_name} finished!!!\n"])
            start = time.time()
            lock.release()
            break