import requests
import json
import numpy as np
import time
import pandas as pd
import random
import os
from multiprocessing import Pool
import traceback

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    # Cookie可能需要每次更新
    # 'Cookie': 'Hm_lvt_a90b3b5dbcc21c25dc23813fcd21968d=1576850560; SESSION=eea7489e-9b0a-4514-be90-17a31301d49e; _ga=GA1.2.1288123266.1576852737; Hm_lpvt_a90b3b5dbcc21c25dc23813fcd21968d=1577186169',
    'Cookie': '_T_WM=22252338461; XSRF-TOKEN=2c2fb1; WEIBOCN_FROM=1110006030; MLOGIN=1; M_WEIBOCN_PARAMS=oid%3D4467107636950632%26luicode%3D20000061%26lfid%3D4467107636950632%26uicode%3D20000061%26fid%3D4467107636950632; SUB=_2A25z4zoRDeRhGeNK6lUW8CjNwzyIHXVRLEZZrDV6PUJbktAfLXH4kW1NSRnmv2j53o3_avnjQL8HTRsXDbkt_q_u; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFTVP1SsjLrZ3Xg_3V3N7ZL5JpX5KzhUgL.Fo-XeKMNehqp1h52dJLoIpjLxK-LB.eLBK5LxK-LBKBLBKMLxK-L1KzL1-2t; SUHB=0mxEKlgBGeSCTm; SSOLoginState=1592216129',
}
error_flag = 'no error'

def timer(func):
    def cal_time(*args, **kw):
        start_time = time.time()
        out = func(*args, **kw)
        end_time = time.time()
        print('函数 ', func.__name__, ' 运行耗时', end_time-start_time, '秒', sep = '')
        return out
    return cal_time

def pause(sec = 1):
    t = np.random.rand() * 2 * sec # 均值为sec秒的暂停
    time.sleep(t)

user_agent_list = [
    # Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'
    # Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:59.0) Gecko/20100101 Firefox/59.0',
    # MyMac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',

    # Others
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36'
]


def headers_generator():
    return headers
    # return {
    #     'User-Agent': random.choice(user_agent_list),
    #     'Cookie': '_T_WM=22252338461; XSRF-TOKEN=2c2fb1; WEIBOCN_FROM=1110006030; MLOGIN=1; M_WEIBOCN_PARAMS=oid%3D4467107636950632%26luicode%3D20000061%26lfid%3D4467107636950632%26uicode%3D20000061%26fid%3D4467107636950632; SUB=_2A25z4zoRDeRhGeNK6lUW8CjNwzyIHXVRLEZZrDV6PUJbktAfLXH4kW1NSRnmv2j53o3_avnjQL8HTRsXDbkt_q_u; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFTVP1SsjLrZ3Xg_3V3N7ZL5JpX5KzhUgL.Fo-XeKMNehqp1h52dJLoIpjLxK-LB.eLBK5LxK-LBKBLBKMLxK-L1KzL1-2t; SUHB=0mxEKlgBGeSCTm; SSOLoginState=1592216129',
    # }

def crawl(url, repeat_times = 10, headers_gene_func = headers_generator):
    flag = False
    i = 0
    try:
        while True:
            response = requests.get(url, headers = headers_gene_func())
            pause(i+1)
            # print(response, 'response')
            # print(response.text, 'response.text')
            if response.status_code == 200:
                out = response
                break
            i += 1
            if i >= repeat_times:
                # out = 'unable to requests.get page under repeat_times'
                out = 2
                print('response.status_code: ', response.status_code)
                raise Exception('unable to requests.get page under repeat_times，url：' + url)
                break
    except:
        # out = 'unknown reason, unable to requests.get page'
        out = 3
        print(traceback.format_exc())
        raise Exception('unknown reason, unable to requests.get page, url: ' + url)
    return out

# @timer
def crawl_comments_reply(para):
    global error_flag
    # 确保para里面有cid和father_path两个键
    cid = para['cid']
    father_path = para['father_path']
    try:
        print("start crawling comments reply, cid:", cid)
        comments_reply = []
        path = father_path + 'comments_reply/'
        init_url = 'https://m.weibo.cn/comments/hotFlowChild?cid=' + str(cid) + '&max_id=0&max_id_type=0'
        res = crawl(init_url)
        if type(res) != type(2):
            dic = json.loads(res.text)
            comments_reply += dic['data']
            max_id = dic['max_id']
            next_url = None
            i = 1
            while True:
                if max_id == 0:
                    break
                print('轮：', i, sep = '')
                next_url = 'https://m.weibo.cn/comments/hotFlowChild?cid=' + str(cid) + '&max_id=' + str(max_id) + '&max_id_type=0'
                res = crawl(next_url)
                if type(res) == type(2):
                    break
                dic = json.loads(res.text)
                comments_reply += dic['data']
                max_id = dic['max_id']
                i += 1

    except: 
        print(traceback.format_exc())
        error_flag = 'error'

    finally:
        print(len(comments_reply), 'len(comments_reply')
        reply_df = pd.DataFrame(comments_reply)
        if not os.path.exists(path):
            os.mkdir(path)
        reply_df.to_csv(path + cid + '.csv', encoding = 'utf_8_sig')
        print("end crawling comments reply, cid:", cid)

@timer
def crawl_all(hotflow_id, continue_flag = '0'):
    global error_flag
    error_flag = 'no error'
    # 先爬评论
    try:
        print("begin crawling comments of hotflow_id:", hotflow_id)

        path = './' + hotflow_id + '/'
        if continue_flag == '0':
            init_url = 'https://m.weibo.cn/comments/hotflow?id=' + hotflow_id + '&mid=' + hotflow_id + '&max_id_type=0'
            next_url = None
            max_id = None
            res = crawl(init_url)
            if type(res) == type(2):
                print("unable to get the init url's response")
                return
            # print(res.text)
            dic = json.loads(res.text)
            comments = dic['data']['data']
            comments_reply = []
            max_id = str(dic['data']['max_id'])
            cid_list = []
            i = 1
        # 添加出错后的继续的功能
        else: 
            comments = []
            comments_reply = []
            continue_crawl_info = None
            with open(path + 'continue_crawl_info.json', 'r') as fr:
                continue_crawl_info = json.loads(fr.read())
            max_id = continue_crawl_info['max_id']
            cid_list = []
            i = continue_crawl_info['i']

        while True:
            print('轮：', i, sep = '')
            next_url = 'https://m.weibo.cn/comments/hotflow?id=' + hotflow_id + '&mid=' + hotflow_id + '&max_id=' + max_id + '&max_id_type=1'
            res = crawl(next_url)
            if type(res) == type(2):
                print("unable to get the later url's response")
                break
            dic = json.loads(res.text)
            comments += dic['data']['data']
            for record in dic['data']['data']:
                if 'more_info_users' in record:
                    # cid是标记回复评论的回复的id
                    cid_list.append({'cid': record['rootid'], 'father_path': path})
            # 添加评论回复
            if dic['data']['max_id_type'] == 0:
                print("dic['data']['max_id_type'] == 0, break")
                break
            else:
                max_id = str(dic['data']['max_id'])
            i += 1

    except:
        print(traceback.format_exc())
        print('max_id when error occur:', max_id)
        continue_crawl_info = {
            'max_id': max_id,
            'i': i,
        }
        if not os.path.exists(path):
            os.mkdir(path)
        with open(path + 'continue_crawl_info.json', 'w') as fw:
            fw.write(json.dumps(continue_crawl_info, indent = 2))
        error_flag = 'error'

    finally:
        # print(comments)
        if not os.path.exists(path):
            os.mkdir(path)
        print(len(comments), 'len(comments)')
        if continue_flag == '0':
            comments_df = pd.DataFrame(comments)
            comments_df.to_csv(path + 'comments' + '.csv', encoding = 'utf_8_sig')
            # 保存之后会用的cidlist
            with open(path + 'cid_list.json', 'w') as fw:
                fw.write(json.dumps(cid_list, indent = 2))
        else:
            if len(comments) != 0:
                comments_df = pd.read_csv(path + 'comments' + '.csv', index_col = 0)
                comments_df = comments_df.append(comments)
                comments_df.to_csv(path + 'comments' + '.csv', encoding = 'utf_8_sig')
                with open(path + 'cid_list.json', 'r') as fr:
                    saved_cid_list = json.loads(fr.read())
                saved_cid_list += cid_list
                with open(path + 'cid_list.json', 'w') as fw:
                    fw.write(json.dumps(saved_cid_list, indent = 2))
        print("end crawling comments of hotflow_id:", hotflow_id)

    # 再爬评论下的回复
    print('-------------')
    print("begin crawling comments reply of hotflow_id:", hotflow_id)
    pool = Pool()
    pool.map(crawl_comments_reply, cid_list)
    pool.close()
    pool.join()
    print("end crawling comments reply of hotflow_id:", hotflow_id)
    # with open('test.json', 'w') as fw:
    #     fw.write(json.dumps(comments, indent = 2))

def crawl_all_wrapper():
    global error_flag
    # 4466768535861595 十几万的那个
    # '4467107636950632' 号称百万评论的那个微博id
    # hotflow_id = input('请输入微博id：')
    # hotflow_id = '4466768535861595'
    hotflow_id = '4467107636950632' 
    continue_flag = input('请输入continue_flag：')
    crawl_all(hotflow_id, continue_flag)
    while True:
        print('error_flag', error_flag)
        if error_flag == 'no error':
            break
        # 等待个60s再爬
        print('sleeping 60s')
        time.sleep(60)
        crawl_all(hotflow_id, '1')

if __name__ == '__main__':

    crawl_all_wrapper()
    # a = [1,2,3,4,5]
    # with open('./4466768535861595/continue_crawl_info.json', 'w') as fw:
    #     fw.write(json.dumps(a, indent = 2))
    # with open('test.json', 'r') as fr:
    #     a = json.loads(fr.read())

    # print(a)
    # print(type(a[0]))

    pass





