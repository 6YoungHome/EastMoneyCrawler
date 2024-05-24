from thread_task import *
        
        
if __name__ == "__main__":
    # # # 全量股票帖子更新程序
    # process_num = 3
    # thread_num = 10
    # stock_pool_path = 'stock_pool.txt'
    # log_path = "./log/"
    
    # stock_code_quene = task_split_generate(stock_pool_path)
    # lock = Lock()
    # with open(f"{log_path}post_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
    #     f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
    
    # process_pool = []
    # start = time.time()
    # for i in range(process_num):
    #     process_pool.append(
    #         multiprocessing.Process(
    #             target=post_download_thread, 
    #             args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock)
    #         )
    #     )

    # for p in process_pool:
    #     p.start()
        
    # for p in process_pool:
    #     p.join()
        
        
    # “昨日”帖子更新程序
    process_num = 3
    thread_num = 10
    stock_pool_path = 'stock_pool50.txt'
    log_path = "./log/"
    
    stock_code_quene = task_split_generate(stock_pool_path)
    lock = Lock()
    with open(f"{log_path}post_update_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
        f.writelines([f"\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
    
    process_pool = []
    start = time.time()
    for i in range(process_num):
        process_pool.append(
            multiprocessing.Process(
                target=post_update_thread, 
                args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock)
            )
        )

    for p in process_pool:
        p.start()
        
    for p in process_pool:
        p.join()
        
    
    # # start-end的评论更新程序, 必须先爬帖子信息
    # process_num = 2
    # thread_num = 20
    # start_date = "2024-05-20"
    # end_date = "2024-05-20"
    # stock_pool_path = 'stock_pool.txt'
    # log_path = "./log/"
    
    # stock_code_quene = task_split_generate(stock_pool_path)
    # lock = Lock()
    # with open(f"{log_path}comment_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
    #     f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
    
    # process_pool = []
    # start = time.time()
    # for i in range(process_num):
    #     process_pool.append(
    #         multiprocessing.Process(
    #             target=comment_download_thread, 
    #             args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock, start_date, end_date)
    #         )
    #     )

    # for p in process_pool:
    #     p.start()
        
    # for p in process_pool:
    #     p.join()
    
    
    # # start-end的帖子内容更新程序, 必须先爬帖子信息
    # process_num = 2
    # thread_num = 20
    # start_date = "2024-05-20"
    # end_date = "2024-05-20"
    # stock_pool_path = 'stock_pool.txt'
    # log_path = "./log/"
    
    # stock_code_quene = task_split_generate(stock_pool_path)
    # lock = Lock()
    # with open(f"{log_path}post_text_download_log_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as f:
    #     f.writelines([f"\n\n【{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} start new project!!!】\n"])
    
    # process_pool = []
    # start = time.time()
    # for i in range(process_num):
    #     process_pool.append(
    #         multiprocessing.Process(
    #             target=post_text_download_thread, 
    #             args=(thread_num, stock_code_quene, f"Process-{i+1}", log_path, lock, start_date, end_date)
    #         )
    #     )

    # for p in process_pool:
    #     p.start()
        
    # for p in process_pool:
    #     p.join()
