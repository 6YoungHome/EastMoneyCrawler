import time
import random
import pandas as pd
import os
import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from browser_parser import PostParser, CommentParser
from exceptions import StockSymbolError
from config import *

class Crawler:
    def __init__(self, stock_symbol: str):
        self.browser = None
        self.symbol = stock_symbol
        self.format_symbol = self.format_stock_symbol(stock_symbol)
        self.start = time.time()  # calculate the time cost
        
    def create_webdriver(self):
        options = webdriver.ChromeOptions()  # configure the webdriver
        options.add_argument('no-sandbox')
        options.add_argument('disable-dev-shm-usage')
        options.add_argument('lang=zh_CN.UTF-8')
        options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                             'like Gecko) Chrome/111.0.0.0 Safari/537.36"')
        options.add_argument('headless')
        self.browser = webdriver.Chrome(options=options)

        js_file_path = os.path.join(RESOURE_PATH+'stealth.min.js')
        with open(js_file_path) as f:
            js = f.read()
        self.browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": js
        })
        
    @staticmethod
    def format_stock_symbol(stock_code):
        if "." in stock_code:
            return stock_code.split(".")[0]
        elif stock_code[0] == "6":
            return stock_code+".SH"
        elif stock_code[0] == "0":
            return stock_code+".SZ"
        elif stock_code[0] == "3":
            return stock_code+".SZ"
        elif stock_code[0] == "8":
            return stock_code+".BJ"
        else:
            raise StockSymbolError(stock_code)


class PostCrawler(Crawler):
    def __init__(self, stock_symbol: str):
        super().__init__(stock_symbol)

    def get_page_num(self):
        self.browser.get(f'http://guba.eastmoney.com/list,{self.symbol},f_1.html')
        page_element = self.browser.find_element(By.CSS_SELECTOR, 'ul.paging > li:nth-child(7) > a > span')
        return int(page_element.text)

    def crawl_by_page(self, page1: int, page2: int=0):
        # start = time.time()
        self.create_webdriver()
        # print(f"create driver:{time.time() - start:.2f}s ")
        
        max_page = self.get_page_num()  # confirm the maximum page number to crawl
        current_page = page1  # start page
        if page2 != 0:
            stop_page = min(page2, max_page)  # avoid out of the index
        else:
            stop_page = max_page

        parser = PostParser()

        dic_list = []
        while current_page <= stop_page:  # use 'while' instead of 'for' is crucial for exception handling
            time.sleep(abs(random.normalvariate(0, 0.01)))  # random sleep time
            # start = time.time()
            url = f'http://guba.eastmoney.com/list,{self.symbol},f_{current_page}.html'
            try:
                self.browser.get(url)
                parser_dict_list, bar_code = parser.parse_post(browser=self.browser)
                if bar_code != self.symbol:
                    with open("./error.txt") as f:
                        f.write(f"爬取 {self.symbol} 时, 进入了错误的地点 {bar_code}, 疑似IP被封禁!!!\n")
                        f.writelines([str(i)+"\n" for i in parser_dict_list])
                    os._exit(1)
                dic_list.extend(parser_dict_list) 
                print(f'{self.symbol}: 已经成功爬取第 {current_page} 页帖子基本信息，'
                      f'进度 {(current_page - page1 + 1)*100/(stop_page - page1 + 1):.2f}%')
                current_page += 1
                
            except Exception as e:
                print(f'{self.symbol}: 第 {current_page} 页出现了错误 {e}')
                time.sleep(0.01)
                self.browser.refresh()
                self.browser.delete_all_cookies()
                self.browser.quit()  # if we don't restart the webdriver, our crawler will be restricted access speed
                self.create_webdriver()  # restart it again!
        end = time.time()
        time_cost = end - self.start  # calculate the time cost
        # start_date = postdb.find_last()['publish_time']
        # end_date = postdb.find_first()['publish_time']  # get the post time range
        # row_count = postdb.count_documents()
        start_date = dic_list[0]['publish_date']
        end_date = dic_list[-1]['publish_date']
        row_count = len(dic_list)
        self.browser.quit()
        df = pd.DataFrame(dic_list)
        df['symbol'] = self.format_symbol
        # df = df.drop_duplicates(subset=['post_id', ''])
        df.to_parquet(SAVE_PATH+"post_eastmoney_guba", index=False, partition_cols=["symbol", "publish_date"])
        print(f'成功爬取 {self.symbol}股吧共 {stop_page - page1 + 1} 页帖子，总计 {row_count} 条，花费 {time_cost/60:.2f} 分钟')
        print(f'帖子的时间范围从 {start_date} 到 {end_date}')
    
    def crawl_oneday_post(self, aim_date=None, date_diff=None):
        """获取某一日的post数据

        Args:
            day_diff (_type_): _description_
        """
        if aim_date:
            if (datetime.datetime.today() - datetime.datetime.strptime(aim_date, "%Y-%m-%d")).days > 5:
                print(f"目标日期{aim_date}太久远！")
                return 
            elif (datetime.datetime.today() - datetime.datetime.strptime(aim_date, "%Y-%m-%d")).days < 0:
                print(f"目标日期{aim_date}是未来日期")
                return
        elif date_diff:
            if date_diff > 5:
                print(f"目标日期太久远！")
                return 
            elif date_diff < 0:
                print(f"目标日期是未来日期")
                return 
            else:
                aim_date = (datetime.datetime.today() - datetime.timedelta(days=date_diff)).strftime("%Y-%m-%d")
        else:
            aim_date = datetime.datetime.today().strftime("%Y-%m-%d")

        self.create_webdriver()
        max_page = self.get_page_num() 
        current_page = 1
        stop_page = max_page

        parser = PostParser()

        dic_list = []
        aim_date_small_count = 0
        while current_page <= stop_page:  # use 'while' instead of 'for' is crucial for exception handling
            time.sleep(abs(random.normalvariate(0, 0.01)))  # random sleep time
            # start = time.time()
            url = f'http://guba.eastmoney.com/list,{self.symbol},f_{current_page}.html'
            try:
                self.browser.get(url)
                dic_list_, bar_code = parser.parse_post(browser=self.browser)
                if bar_code != self.symbol:
                    with open("./error.txt") as f:
                        f.write(f"爬取 {self.symbol} 时, 进入了错误的地点 {bar_code}, 疑似IP被封禁!!!\n")
                        f.writelines([str(i)+"\n" for i in dic_list_])
                    os._exit(1)
                for dic in dic_list_:
                    if dic['publish_date'] == aim_date:
                        dic_list.append(dic)
                        aim_date_small_count = 0
                    elif dic['publish_date'] < aim_date:
                        aim_date_small_count += 1
                    if aim_date_small_count == 10:
                        current_page = stop_page + 100
                        break
                current_page += 1
                
            except Exception as e:
                print(f'{self.symbol}: 第 {current_page} 页出现了错误 {e}')
                time.sleep(0.01)
                self.browser.refresh()
                self.browser.delete_all_cookies()
                self.browser.quit()  # if we don't restart the webdriver, our crawler will be restricted access speed
                self.create_webdriver()  # restart it again!
        end = time.time()
        time_cost = end - self.start
        row_count = len(dic_list)
        self.browser.quit()
        df = pd.DataFrame(dic_list)
        if not df.empty:
            df['symbol'] = self.format_symbol
            df.to_parquet(SAVE_PATH+"post_eastmoney_guba", index=False, partition_cols=["symbol", "publish_date"])
            print(f'成功爬取 {self.symbol}股吧{aim_date}帖子，共 {row_count} 条数据，花费 {time_cost/60:.2f} 分钟')
        else:
            print(f'成功爬取 {self.symbol}股吧{aim_date}帖子，共 0 条数据，花费 {time_cost/60:.2f} 分钟')

    def crawl_history_post(self, aim_date=None, date_diff=None):
        """获取过去某一日到现在的post数据

        Args:
            day_diff (_type_): _description_
        """
        if aim_date:
            if (datetime.datetime.today() - datetime.datetime.strptime(aim_date, "%Y-%m-%d")).days < 0:
                print(f"目标日期{aim_date}是未来日期")
                return 
        elif date_diff:
            if date_diff < 0:
                print(f"目标日期是未来日期")
                return 
            else:
                aim_date = (datetime.datetime.today() - datetime.timedelta(days=date_diff)).strftime("%Y-%m-%d")
        else:
            aim_date = datetime.datetime.today().strftime("%Y-%m-%d")

        self.create_webdriver()
        max_page = self.get_page_num() 
        current_page = 1
        parser = PostParser()

        dic_list = []
        aim_date_small_count = 0
        while current_page <= max_page:  # use 'while' instead of 'for' is crucial for exception handling
            time.sleep(abs(random.normalvariate(0, 0.01)))  # random sleep time
            # start = time.time()
            url = f'http://guba.eastmoney.com/list,{self.symbol},f_{current_page}.html'
            try:
                self.browser.get(url)
                dic_list_, bar_code = parser.parse_post(browser=self.browser)
                if bar_code != self.symbol:
                    with open(LOG_PATH+"error.txt", "a") as f:
                        f.write(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%:%S')} \t 爬取 {self.symbol} 时, 进入了错误的地点 {bar_code}, 疑似IP被封禁!!!\n")
                        f.writelines([str(i)+"\n" for i in dic_list_])
                    os._exit(1)
                for dic in dic_list_:
                    if dic['publish_date'] >= aim_date:
                        dic_list.append(dic)
                        aim_date_small_count = 0
                    elif dic['publish_date'] < aim_date:
                        aim_date_small_count += 1
                    if aim_date_small_count == 10:
                        current_page = max_page + 100
                        break
                current_page += 1
                
            except Exception as e:
                print(f'{self.symbol}: 第 {current_page} 页出现了错误 {e}')
                time.sleep(0.01)
                self.browser.refresh()
                self.browser.delete_all_cookies()
                self.browser.quit()  # if we don't restart the webdriver, our crawler will be restricted access speed
                self.create_webdriver()  # restart it again!
        end = time.time()
        time_cost = end - self.start
        row_count = len(dic_list)
        self.browser.quit()
        df = pd.DataFrame(dic_list)
        if not df.empty:
            df['symbol'] = self.format_symbol
            df.to_parquet(SAVE_PATH+"post_eastmoney_guba", index=False, partition_cols=["symbol", "publish_date"])
            print(f'成功爬取 {self.symbol}股吧{aim_date}到今日的帖子，共 {row_count} 条数据，花费 {time_cost/60:.2f} 分钟')
        else:
            print(f'成功爬取 {self.symbol}股吧{aim_date}到今日的帖子，共 0 条数据，花费 {time_cost/60:.2f} 分钟')

class CommentCrawler(Crawler):
    def __init__(self, stock_symbol: str):
        super().__init__(stock_symbol)
        self.post_df = None  # dataframe about the post_url and post_id
        self.current_num = 0

    def find_by_date(self, start_date=None, end_date=None):
        # get comment urls through date (used for the first crawl)
        """
        :param start_date: '2003-07-21' 字符串格式 ≥
        :param end_date: '2024-07-21' 字符串格式 ≤
        """
        # postdb = MongoAPI('post_eastmoney_guba', f'post_{self.symbol}')
        # time_query = {
        #     'post_date': {'$gte': start_date, '$lte': end_date},
        #     'comment_num': {'$ne': 0}  # avoid fetching urls with no comment
        # }
        # post_info = postdb.find(time_query, {'_id': 1, 'post_url': 1})  # , 'post_date': 1
        # self.post_df = pd.DataFrame(post_info)
        if start_date and end_date:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol), ("publish_date", ">=", start_date), 
                        ("publish_date", "<=", end_date), ("post_comment_count", "!=", 0)]
            )
        elif start_date:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol), ("publish_date", ">=", start_date), ("comment_num", "!=", 0)]
            )
        elif end_date:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol), ("publish_date", "<=", end_date), ("post_comment_count", "!=", 0)]
            )
        else:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol), ("post_comment_count", "!=", 0)]
            )

    def crawl_comment_info(self):
        url_df = self.post_df['post_url']
        id_df = self.post_df['post_id']
        total_num = self.post_df.shape[0]

        self.create_webdriver()
        parser = CommentParser()

        for idx in range(total_num):
            url = url_df.iloc[idx]
            pid = id_df.iloc[idx]
            print(url, pid)
            cfh = 'cfhpl' in url
            try:
                time.sleep(abs(random.normalvariate(0.03, 0.01)))  # random sleep time
                try:  # sometimes the website needs to be refreshed (situation comment is loaded unsuccessfully)
                    self.browser.get(url)  # this function may also get timeout exception
                    WebDriverWait(self.browser, 0.2, poll_frequency=0.1).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.reply_item.cl')))
                except TimeoutException:  # timeout situation
                    self.browser.refresh()
                finally:
                    max_reply_page = self.browser.find_elements(By.XPATH, '//*[@class="pager"]/ul/li')
                    if max_reply_page == []:
                        max_reply_page = 1
                    else:
                        max_reply_page = int(max_reply_page[-2].text)
                    for reply_page in range(1, max_reply_page+1):
                        url_page = f"{url[:-5]}_{reply_page}.html"
                        print(url_page)
                        time.sleep(abs(random.normalvariate(0, 0.01)))
                        try:  # sometimes the website needs to be refreshed (situation comment is loaded unsuccessfully)
                            self.browser.get(url_page)  # this function may also get timeout exception
                            for item in self.browser.find_elements(By.XPATH, '//*[@class="moreL2reply"]/span[2]'):
                                item.click()
                            WebDriverWait(self.browser, 0.2, poll_frequency=0.1).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.reply_item.cl')))
                        except TimeoutException:  # timeout situation
                            self.browser.refresh()
                            print('------------ refresh ------------')
                        finally:
                            parser.parse_comment_info(self.browser, pid, cfh)
                self.current_num += 1
                print(f'{self.symbol}: 已成功爬取 {self.current_num} 页评论信息，进度 {self.current_num*100/total_num:.3f}%')

            except TypeError as e:  # some comment is not allowed to display, just skip it
                self.current_num += 1
                print(f'{self.symbol}: 第 {self.current_num} 页出现了错误 {e} （{url}）')  # maybe the invisible comments
                # print(f'应爬取的id范围是 {id_df.iloc[0]} 到 {id_df.iloc[-1]}, id {id_df.iloc[self.current_num - 1]} 出现了错误')
                self.browser.delete_all_cookies()
                self.browser.quit()  # restart webdriver if crawler is restricted
                self.create_webdriver()

        end = time.time()
        time_cost = end - self.start
        df = pd.DataFrame(parser.comment_dict_list)
        df['symbol'] = self.format_symbol
        df.to_parquet(SAVE_PATH+"comment_eastmoney_guba", index=False, partition_cols=["symbol", "reply_date"])
        self.browser.quit()
        print(f'成功爬取 {self.symbol}股吧 {self.current_num} 页评论，花费 {time_cost/60:.2f}分钟')


class PostTextCrawler(Crawler):
    def __init__(self, stock_symbol: str):
        super().__init__(stock_symbol)
        self.post_df = None  # dataframe about the post_url and post_id

    def find_by_date(self, start_date=None, end_date=None):
        # get comment urls through date (used for the first crawl)
        """
        :param start_date: '2003-07-21' 字符串格式 ≥
        :param end_date: '2024-07-21' 字符串格式 ≤
        """
        # postdb = MongoAPI('post_eastmoney_guba', f'post_{self.symbol}')
        # time_query = {
        #     'post_date': {'$gte': start_date, '$lte': end_date},
        #     'comment_num': {'$ne': 0}  # avoid fetching urls with no comment
        # }
        # post_info = postdb.find(time_query, {'_id': 1, 'post_url': 1})  # , 'post_date': 1
        # self.post_df = pd.DataFrame(post_info)
        if start_date and end_date:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol), ("publish_date", ">=", start_date), 
                        ("publish_date", "<=", end_date)]
            )
        elif start_date:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol), ("publish_date", ">=", start_date)]
            )
        elif end_date:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol), ("publish_date", "<=", end_date)]
            )
        else:
            self.post_df = pd.read_parquet(
                f"post_eastmoney_guba", 
                filters=[("symbol", "==", self.format_symbol)]
            )

    def crawl_post_text_info(self):
        url_df = self.post_df['post_url']
        id_df = self.post_df['post_id']
        pub_date_df = self.post_df['publish_date']
        total_num = self.post_df.shape[0]

        self.create_webdriver()
        parser = PostParser()

        idx = 0
        dict_list = []
        while idx < total_num:
            url = url_df.iloc[idx]
            pid = id_df.iloc[idx]
            pdt = pub_date_df.iloc[idx]
            
            try:
                print(url)
                time.sleep(abs(random.normalvariate(0.03, 0.01)))  # random sleep time
                try:  # sometimes the website needs to be refreshed (situation comment is loaded unsuccessfully)
                    self.browser.get(url)  # this function may also get timeout exception
                    WebDriverWait(self.browser, 0.2, poll_frequency=0.1).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.reply_item.cl')))
                except TimeoutException:  # timeout situation
                    self.browser.refresh()
                finally:
                    text = parser.get_post_text(self.browser)
                    dict_list.append({
                        "post_text": text, "post_id": pid, 'publish_date': pdt
                    })
                    
                idx += 1
                print(f'{self.symbol}: 已成功爬取 {idx} 个帖子内容，进度 {idx*100/total_num:.3f}%')

            except TypeError as e:  # some comment is not allowed to display, just skip it

                print(f'{self.symbol}: 第 {idx} 页出现了错误 {e} （{url}）')  # maybe the invisible comments
                # print(f'应爬取的id范围是 {id_df.iloc[0]} 到 {id_df.iloc[-1]}, id {id_df.iloc[self.current_num - 1]} 出现了错误')
                self.browser.delete_all_cookies()
                self.browser.quit()  # restart webdriver if crawler is restricted
                self.create_webdriver()

        end = time.time()
        time_cost = end - self.start
        df = pd.DataFrame(dict_list)
        df['symbol'] = self.format_symbol
        df.to_parquet(SAVE_PATH+"post_text_eastmoney_guba", index=False, partition_cols=["symbol", "publish_date"])
        self.browser.quit()
        print(f'成功爬取 {self.symbol}股吧 {idx} 页内容，花费 {time_cost/60:.2f}分钟')
        
if __name__ == "__main__":
    print(RESOURE_PATH+'stealth.min.js')
    # with open(RESOURE_PATH+'stealth.min.js') as f:
    #     print(f.readlines())