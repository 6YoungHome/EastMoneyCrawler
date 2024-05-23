from selenium.webdriver.common.by import By 
from selenium import webdriver
from selenium.common import StaleElementReferenceException
from datetime import datetime
import json
import re

class PostParser(object):

    def __init__(self):
        self.year = None
        self.month = 13
        self.id = 0
    
    @staticmethod
    def dict_keys_index(data_dict, key_list):
        tmp = {}
        for key in key_list:
            tmp[key] = data_dict[key]
        return tmp
    
    def parse_post(self, browser):
        fields = [
            'post_id', 'post_title', 'stockbar_code', 'user_nickname', 
            'user_id', 'post_click_count', 'post_forward_count', 'post_comment_count', 
            'post_publish_time', 'post_last_time', 'post_type', 'post_source_id', 
        ]
        tmp_ = json.loads(browser.execute_script("return JSON.stringify(article_list)"))
        dic_list_ = tmp_['re']
        bar_code = tmp_['bar_code']
        crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dic_list = []
        for data_dict in dic_list_:
            if "post_is_like" in data_dict:
                continue
            data_dict = self.dict_keys_index(data_dict, fields)
            data_dict['publish_date'] = data_dict['post_publish_time'].split(" ")[0]
            data_dict['publish_time'] = data_dict['post_publish_time'].split(" ")[1]
            del data_dict['post_publish_time']
            
            data_dict['update_date'] = data_dict['post_last_time'].split(" ")[0]
            data_dict['update_time'] = data_dict['post_last_time'].split(" ")[1]
            del data_dict['post_last_time']
            
            data_dict['crawl_date'] = crawl_time.split(" ")[0]
            data_dict['crawl_time'] = crawl_time.split(" ")[1]
            if data_dict['post_source_id'] == "":
                data_dict['post_url'] = f"https://guba.eastmoney.com/news,{data_dict['stockbar_code']},{data_dict['post_id']}.html"
            else:
                data_dict['post_url'] = f"https://guba.eastmoney.com/news,cfhpl,{data_dict['post_id']}.html"
            dic_list.append(data_dict)
        return dic_list, bar_code
        
    def __parse_post(self, html):
        ## 不用了
        dic_list = []
        
        read = html.find_elements(By.XPATH, '//*[@class="listbody"]/tr/td[1]/div')
        read = [i.text for i in read]

        reply = html.find_elements(By.XPATH, '//*[@class="listbody"]/tr/td[2]/div')
        reply = [i.text for i in reply]
        
        title = html.find_elements(By.XPATH, '//*[@class="listbody"]/tr/td[3]/div/a')
        post_url = [i.get_attribute('href') for i in title]
        title = [i.text for i in title]
        
        author = html.find_elements(By.XPATH, '//*[@class="listbody"]/tr/td[4]/div/a[1]') 
        author = [i.text for i in author]
        
        publish = html.find_elements(By.XPATH, '//*[@class="listbody"]/tr/td[5]/div')
        publish = [i.text for i in publish]
        crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for i in range(len(read)):
            if self.year is None:
                self.get_post_year(post_url[i])
                
            if self.month < int(publish[i][0:2]) == 12:
                self.year -= 1
            self.month = int(publish[i][0:2])
            publish_time = f'{self.year}-{publish[i]}'
            dic_list.append({
                'post_id': self.id,
                'post_title': title[i],
                'post_author': author[i],
                'post_view': read[i],
                'comment_num': reply[i],
                'post_url': post_url[i],
                'publish_date': publish_time.split(" ")[0],
                'publish_time': publish_time.split(" ")[1],
                'crawl_date': crawl_time.split(" ")[0],
                'crawl_time': crawl_time.split(" ")[1]
            })
            self.id += 1
        return dic_list
    
    def get_post_year(self, post_url):
        driver = webdriver.Chrome()

        if 'guba.eastmoney.com' in post_url:  # 这是绝大部分的普通帖子
            driver.get(post_url)
            date_str = driver.find_element(By.CSS_SELECTOR, 'div.newsauthor > div.author-info.cl > div.time').text
            self.year = int(date_str[:4])
            driver.quit()
        elif 'caifuhao.eastmoney.com' in post_url:  # 有些热榜帖子会占据第一位，对于这种情况要特殊处理
            driver.get(post_url)
            date_str = driver.find_element(By.CSS_SELECTOR, 'div.article.page-article > div.article-head > '
                                                            'div.article-meta > span.txt').text
            self.year = int(date_str[:4])
            driver.quit()
        else:
            self.year = datetime.now().year
            
    def get_post_text(self, browser):
        try:
            for item in browser.find_elements(By.XPATH, '//*[@class="readMoreBtn"]'):
                item.click()
        except StaleElementReferenceException:
            for item in browser.find_elements(By.XPATH, '//*[@class="readMoreBtn"]'):
                item.click()
        text = browser.find_elements(By.XPATH, '//*[@class="newstext "]')
        text = text[0].text.replace("\n", "") if text != [] else ""
        return text


class CommentParser(object):
    def __init__(self):
        self.comment_dict_list = []
        
    def parse_comment_info(self, html, pid, cfh=False):  # sub_pool is used to distinguish sub-comments
        try:
            for item in html.find_elements(By.XPATH, '//*[@class="moreL2reply"]/span[2]'):
                item.click()
        except StaleElementReferenceException:
            for item in html.find_elements(By.XPATH, '//*[@class="moreL2reply"]/span[2]'):
                item.click()

        crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if cfh:
            reply_items = html.find_elements(By.XPATH, '//div[@class="allReplyList"]/div[@class="replylist_content"]/div')
        else:
            reply_items = html.find_elements(By.XPATH, '//*[@class="reply_item cl  "]')
        user_name = [i.find_element(By.XPATH, 'div/div[2]/a').text for i in reply_items]
        user_id = [i.find_element(By.XPATH, 'div/div[2]/a').get_attribute("href").split("/")[-1] for i in reply_items]
        reply_id = [i.get_attribute("data-reply_id") for i in reply_items]
        reply_date = [i.find_element(By.XPATH, 'div/div[3]/span').text.split(" ")[0] for i in reply_items]
        reply_time = [i.find_element(By.XPATH, 'div/div[3]/span').text.split(" ")[1] for i in reply_items]
        replay_place = [i.find_element(By.XPATH, 'div/div[3]/span[2]').text.split(" ")[1] for i in reply_items]
        reply_content = [i.find_element(By.XPATH, 'div/div[4]').text for i in reply_items]
        reply_num = [i.find_element(By.XPATH, 'div/div[5]/ul/li/span[@class="replybtn"]').text for i in reply_items]
        reply_num = [0 if i == '评论' else int(i) for i in reply_num]
        like_num = [i.find_element(By.XPATH, 'div/div[5]/ul/li/span[@class="likemodule"]').text for i in reply_items]
        like_num = [0 if i == '点赞' else int(i) for i in like_num]
        
        if cfh:
            sub_reply = html.find_elements(By.XPATH, '//div[@class="allReplyList"]/div/div/div/ul/li[@class="reply_item_l2  "]')
        else:
            sub_reply = html.find_elements(By.XPATH, '//li[@class="reply_item_l2  "]')
        
        user_name_s = [i.find_element(By.XPATH, 'div[@class="reuser_l2 cl"]/div/div[2]/a').text for i in sub_reply]
        user_id_s = [i.find_element(By.XPATH, 'div[@class="reuser_l2 cl"]/div/div[2]/a').get_attribute("href").split("/")[-1] for i in sub_reply]
        reply_id_s = [i.get_attribute("data-reply_id") for i in sub_reply]
        reply_date_s = [i.find_element(By.XPATH, 'div[@class="reply_bottom_l2 cl"]/div/span[1]').text.split(" ")[0] for i in sub_reply]
        reply_time_s = [i.find_element(By.XPATH, 'div[@class="reply_bottom_l2 cl"]/div/span[1]').text.split(" ")[1] for i in sub_reply]
        replay_place_s = [i.find_element(By.XPATH, 'div[@class="reply_bottom_l2 cl"]/div/span[2]').text.split(" ")[1] for i in sub_reply]
        reply_content_s = [i.find_element(By.XPATH, './/*[@class="reply_title_span"]').text for i in sub_reply]
        reply_num_s = [i.find_element(By.XPATH, 'div[@class="reply_bottom_l2 cl"]/ul/li[2]').text for i in sub_reply]
        reply_num_s = [0 if i == '评论' else int(i) for i in reply_num_s]
        like_num_s = [i.find_element(By.XPATH, 'div[@class="reply_bottom_l2 cl"]/ul/li[3]').text for i in sub_reply]
        like_num_s = [0 if i == '点赞' else int(i) for i in like_num_s]

        for pos in range(len(reply_id)):
            comment_dict = {
                "post_id": pid,
                "reply_id": reply_id[pos],
                "user_name":  user_name[pos],
                "user_id":  user_id[pos],
                "reply_date": reply_date[pos],
                "reply_time": reply_time[pos],
                "replay_place": replay_place[pos],
                "reply_content": reply_content[pos],
                "reply_num": reply_num[pos],
                "like_num": like_num[pos],
                'father_reply_id': "",
                'crawl_date': crawl_time.split(" ")[0],
                'crawl_time': crawl_time.split(" ")[1]
            }
            self.comment_dict_list.append(comment_dict)
            
        pos = 0
        for rpos in range(len(reply_num)):
            if reply_num[rpos] == 0:
                continue
            for _ in range(reply_num[rpos]):
                f_rid = reply_id[rpos]
                comment_dict = {
                    "post_id": pid,
                    "reply_id": reply_id_s[pos],
                    "user_name":  user_name_s[pos],
                    "user_id":  user_id_s[pos],
                    "reply_date": reply_date_s[pos],
                    "reply_time": reply_time_s[pos],
                    "replay_place": replay_place_s[pos],
                    "reply_content": reply_content_s[pos],
                    "reply_num": reply_num_s[pos],
                    "like_num": like_num_s[pos],
                    'father_reply_id': f_rid,
                    'crawl_date': crawl_time.split(" ")[0],
                    'crawl_time': crawl_time.split(" ")[1]
                }
                pos += 1
                self.comment_dict_list.append(comment_dict)
 
 # type: ignore