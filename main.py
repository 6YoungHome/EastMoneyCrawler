from crawler import PostCrawler
from crawler import CommentCrawler
import threading
import time


def post_thread(stock_symbol, start_page, end_page):  # stock_symbol为股票的代码，page为想要爬取的页面范围
    post_crawler = PostCrawler(stock_symbol)
    post_crawler.crawl_post_info(start_page, end_page)


def comment_thread_date(stock_symbol, start_date, end_date):  # stock_symbol为股票的代码，date为想要爬取的日期范围
    comment_crawler = CommentCrawler(stock_symbol)
    comment_crawler.find_by_date(start_date, end_date)
    comment_crawler.crawl_comment_info()


def comment_thread_id(stock_symbol, start_id, end_id):  # stock_symbol为股票的代码，id是通过post_id来确定爬取，适合断联续爬
    comment_crawler = CommentCrawler(stock_symbol)
    comment_crawler.find_by_id(start_id, end_id)
    comment_crawler.crawl_comment_info()


if __name__ == "__main__":
    # with open('stock_pool.txt') as f:
    #     stock_pool = f.readlines()
    # thread_pool = []
    # for stock_code in stock_pool[0: 5]:
    #     stock_code = stock_code.strip().split(".")[0]
    #     thread_pool.append(threading.Thread(target=post_thread, args=(stock_code, 1, 0)))

    # for t in thread_pool:
    #     t.start()
        
    # for t in thread_pool:
    #     t.join()
    
    # print("All tasks success!!!")
    # 600392: 已经成功爬取第 1238 页帖子基本信息，进度 48.49%
    # start = time.time()
    # post_crawler = PostCrawler("000001")
    # post_crawler.crawl_post_info(1, 5)
    
    post_crawler = PostCrawler("600519")
    post_crawler.crawl_post_info(1, 5)
    
    # post_crawler.create_webdriver()
    # print(time.time()-start)
    # comment_crawler = CommentCrawler('600519')
    # comment_crawler.find_by_date()
    # comment_crawler.crawl_comment_info()
    
    # comment_crawler = CommentCrawler('000001')
    # comment_crawler.find_by_date()
    # comment_crawler.crawl_comment_info()
    
    # # 爬取发帖信息
    
    # thread1 = threading.Thread(target=post_thread, args=('000333', 1, 500))  # 设置想要爬取的股票代码，开始与终止页数
    # thread2 = threading.Thread(target=post_thread, args=('000729', 1, 500))  # 可同时进行多个线程

    # # 爬取评论信息，注意需先爬取发帖信息储存到数据库里后才可以爬取评论信息（因为需要用到第一步中的url）
    # # thread1 = threading.Thread(target=comment_thread_date, args=('000333', '2020-01-01', '2023-12-31'))
    # # thread2 = threading.Thread(target=comment_thread_date, args=('000729', '2020-01-01', '2023-12-31'))

    # # 中断之后重新通过_id接着爬取
    # # thread1 = threading.Thread(target=comment_thread_id, args=('000651', 384942, 411959))
    # # thread2 = threading.Thread(target=comment_thread_id, args=('000651', 62929, 321047))

    # thread1.start()
    # thread2.start()

    # thread1.join()
    # thread2.join()

    # print(f"you have fetched data successfully, congratulations!")
