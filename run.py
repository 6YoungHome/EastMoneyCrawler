# -*- coding:utf-8 -*-
import click
import threading
import multiprocessing
from multiprocessing import Lock
import datetime

from thread_task import *
from exceptions import ParameterMissingError, InvalidParameterError

@click.group()
def cli():
    pass

@cli.command()
@click.option('--method', type=str, default='', help='crawl method')
@click.option('--process_num', type=int, default=4, help='process')
@click.option('--thread_num', type=int, default=30, help='thread')
@click.option('--stock_pool_path', type=str, default='', help='stock pool')
@click.option('--log_path', type=str, default='./log/', help='log path')
@click.option('--start_date', type=str, default='', help='start date')
@click.option('--end_date', type=str, default='', help='end date')


# python run.py crawl --method=post_get_history --process_num=50 --thread_num=50 --stock_pool_path="stock_pool.txt" --start_date="2024-04-20"
# python run.py crawl --method=post_get_history --process_num=2 --thread_num=10 --stock_pool_path="stock_pool50.txt" --start_date="2024-05-24"

def crawl(method, process_num, thread_num, log_path, stock_pool_path, start_date, end_date):
    stock_code_quene = task_split_generate(stock_pool_path)
    lock = Lock()
    process_pool = []
    
    
    if method == "post_download_all":
        log_file_name = f"{log_path}post_download_all_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
        with open(log_file_name, "a") as f:
            f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
        for i in range(process_num):
            process_pool.append(
                multiprocessing.Process(
                    target=post_download_thread, 
                    args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock)
                )
            )
    elif method == "post_get_history":
        if start_date == "":
            raise ParameterMissingError("start_date", "历史数据的开始日期")
        log_file_name = f"{log_path}post_download_history_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
        with open(log_file_name, "a") as f:
            f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
        process_pool = []
        for i in range(process_num):
            process_pool.append(
                multiprocessing.Process(
                    target=post_get_history_thread, 
                    args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, start_date, lock)
                )
            )
    elif method == "post_update_yesterday":
        log_file_name = f"{log_path}post_update_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
        with open(log_file_name, "a") as f:
            f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
        process_pool = []
        for i in range(process_num):
            process_pool.append(
                multiprocessing.Process(
                    target=post_update_thread, 
                    args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock)
                )
            )
    elif method == "comment_download":
        if start_date == "":
            start_date = None
        if end_date == "":
            end_date = None
        log_file_name = f"{log_path}comment_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
        with open(log_file_name, "a") as f:
            f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
        for i in range(process_num):
            process_pool.append(
                multiprocessing.Process(
                    target=comment_download_thread, 
                    args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, start_date, end_date, lock)
                )
            )
    elif method == "post_text_download":
        if start_date == "":
            start_date = None
        if end_date == "":
            end_date = None
        log_file_name = f"{log_path}post_text_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
        with open(log_file_name, "a") as f:
            f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
        for i in range(process_num):
            process_pool.append(
                multiprocessing.Process(
                    target=post_text_download_thread, 
                    args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, start_date, end_date, lock)
                )
            )
    else:
        raise InvalidParameterError(
            "method", method, ["post_download_all", "post_get_history", 
            "post_update_yesterday", "comment_download", "post_text_download"]
        )
    for p in process_pool:
        p.start()
    for p in process_pool:
        p.join()
        

    with open(log_file_name, "a") as f:
        f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} task finish!】\n"])
    print("All task finish!")

if __name__ == '__main__':
    cli()
