class Builder(object):
    from retrying import retry
    def __init__(self, type, l_product, l_high_product, key):
        from elasticsearch import Elasticsearch
        self.type = type
        self.l_product = l_product
        self.l_high_product = l_high_product
        self.key = key
        self.es = Elasticsearch(hosts=[{"host": "192.168.0.67", "port": 9200}])
        self.location_dict = {"11": "北京市", "12": "天津市", "13": "河北省", "14": "山西省", "15": "内蒙古自治区",
                         "21": "辽宁省", "22": "吉林省", "23": "黑龙江省", "31": "上海市", "32": "江苏省",
                         "33": "浙江省", "34": "安徽省", "35": "福建省", "36": "江西省", "37": "山东省",
                         "41": "河南省", "42": "湖北省", "43": "湖南省", "44": "广东省", "45": "广西壮族自治区",
                         "46": "海南省", "50": "重庆市", "51": "四川省", "52": "贵州省", "53": "云南省",
                         "54": "西藏自治区", "61": "陕西省", "62": "甘肃省", "63": "青海省",
                         "64": "宁夏回族自治区", "65": "新疆维吾尔自治区", "71": "台湾省",
                         "81": "香港特别行政区", "82": "澳门特别行政区"}

    @staticmethod
    def is_chinese(uchar):
        if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
            return True
        else:
            return False

    @staticmethod
    def clean(unclean_list):
        unclean_list = unclean_list.str.strip()
        unclean_list = unclean_list.str.replace("\d年", "")
        unclean_list = unclean_list.str.replace("（.+）", "")
        clean_list = unclean_list.str.replace("[^\u4e00-\u9fa5]+", "")
        return clean_list

    @staticmethod
    def timestamp_to_time(data):
        import time
        l = []
        for i in data:
            # 转换成localtime
            time_local = time.localtime(i)
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d", time_local)
            l.append(dt)
        return l

    @retry
    def get_item_data(self,keyword):
        from elasticsearch import helpers
        item = keyword
        final_result = list()
        item_list = []
        if Builder.is_chinese(item):
            if len(item) > 2:
                n = 0
                while n < len(item) - 1:
                    item_list.append(item[n:n + 2])
                    n += 1
            elif len(item) <= 2:
                item_list.append(item)
        else:
            item_list.append(item)
        body_list = []
        for i in item_list:
            body_list.append(
                {
                    "query_string": {
                        "default_field": "content",
                        "query": "\"" + i + "\""
                    }
                },
            )
        body_list.append({
            "term": {
                "type": "1"
            }
        })
        body = {
            "_source": [
                "district_news_id",
                "successful_time",
                "win_bid_figure",
                "subscriber",
                "win_bid_company",
                "district",
                "title"
            ],
            "query": dict(bool=dict(must=body_list, must_not=[], should=[])),
            "from": 0,
            "size": 1000,
            "sort": [],
            "aggs": {}
        }
        ret = helpers.scan(self.es, body, scroll="5m", index="tendernews_info_v1", doc_type="news", request_timeout=60)
        for code in ret:
            code["_source"]["keyword_cp"] = item
            final_result.append(code["_source"])
        return final_result

    @retry
    def get_win_bid_company(self,keyword):
        from elasticsearch import helpers
        com = keyword
        final_result = list()
        body = {
            "_source": [
                "district_news_id",
                "successful_time",
                "win_bid_figure",
                "subscriber",
                "district",
                "title"
            ],
            "query": {
                "bool": {
                    "must": [
                        {"term": {
                            "win_bid_company.keyword": com
                        }},
                        {
                            "term": {
                                "type": "1"
                            }
                        }
                        ,
                    ],
                    "must_not": [],
                    "should": []
                }
            },
            "from": 0,
            "size": 1000,
            "sort": [],
            "aggs": {}
        }
        ret = helpers.scan(self.es, body, scroll="5m", index="tendernews_info_v1", doc_type="news", request_timeout=30)
        for code in ret:
            code["_source"]["company"] = com
            final_result.append(code["_source"])
        return final_result

    def get_company(self, list, company_list, final_company_result):
        for i in list:
            com = company_list[i]
            tmp = self.get_win_bid_company(com)
            final_company_result.append(tmp)

    @retry
    def get_win_bid_subscriber(self, keyword):
        from elasticsearch import helpers
        com = keyword
        final_result = list()
        body = {
            "_source": [
                "district_news_id",
                "successful_time",
                "win_bid_figure",
                "win_bid_company",
                "district",
                "title"
            ],
            "query": {
                "bool": {
                    "must": [
                        {"term": {
                            "subscriber.keyword": com
                        }},
                        {
                            "term": {
                                "type": "1"
                            }
                        }
                        ,
                    ],
                    "must_not": [],
                    "should": []
                }
            },
            "from": 0,
            "size": 1500,
            "sort": [],
            "aggs": {}
        }
        ret = helpers.scan(self.es, body, scroll="5m", index="tendernews_info_v1", doc_type="news", request_timeout=30)
        for code in ret:
            code["_source"]["subscriber"] = com
            final_result.append(code["_source"])
        return final_result

    def get_subscriber(self, list, subscriber_list, final_company_result):
        for i in list:
            com = subscriber_list[i]
            tmp = self.get_win_bid_subscriber(com)
            final_company_result.append(tmp)

    def bernoulli_distribution(self,company_list, data, lenth, key, l1, l2, l3, l4, l5):
        import time as t
        import numpy as np
        import pandas as pd
        for num in lenth:
            com = company_list[num]
            piece = data[data[str(key)] == com]
            piece = piece.append({"successful_time": int(t.time()), str(key): com}, ignore_index=True)
            piece["successful_time"] = self.timestamp_to_time(piece["successful_time"])
            piece["successful_time"] = pd.to_datetime(piece["successful_time"])
            piece.drop_duplicates(subset="successful_time", inplace=True)
            piece.set_index("successful_time", inplace=True)
            piece = piece["district_news_id"].resample("3m", "count")
            piece[piece != 0] = 1
            piece = list(piece)
            if len(piece) == 1:
                l1.append(com)
                l2.append("/")
                l3.append("/")
                l4.append("/")
                l5.append("/")
            else:
                l1.append(com)
                l2.append(np.mean(piece))
                l3.append(np.std(piece))
                l4.append(np.mean(piece) - np.std(piece) / len(piece) * 2)
                l5.append(np.mean(piece) + np.std(piece) / len(piece) * 2)


    def run(self):
        import pandas as pd
        import threading

        print("开始提取原始数据")
        print("中标公司维度：")
        final_result = []
        for item in self.l_product:
            tmp = self.get_item_data(item)
            final_result += tmp
            print(item + ":" + str(len(tmp)))
        data1 = pd.DataFrame(final_result)
        data1["subscriber"] = self.clean(data1["subscriber"])
        data1["win_bid_company"] = self.clean(data1["win_bid_company"])
        data1.drop_duplicates(subset="district_news_id", inplace=True)
        data1 = data1[(data1["win_bid_company"] != "") & (data1["subscriber"] != "")]
        data1.index = range(len(data1))
        data2 = data1.copy()
        data2 = data2["subscriber"].value_counts().reset_index()
        data2.columns = ["subscriber_product", "subscriber_product_sum"]

        if type == "1":
            data_host = data1.copy()
        elif type == "0":
            final_result = []
            print("甲方维度：")
            for item in self.l_high_product:
                tmp = self.get_item_data(item)
                final_result += tmp
                print(item + ":" + str(len(tmp)))
            data_host = pd.DataFrame(final_result)
            data_host["subscriber"] = self.clean(data_host["subscriber"])
            data_host["win_bid_company"] = self.clean(data_host["win_bid_company"])
            data_host.drop_duplicates(subset="district_news_id", inplace=True)
            data_host = data_host[(data_host["win_bid_company"] != "") & (data_host["subscriber"] != "")]
            data_host.index = range(len(data_host))

        g_product = data1.groupby("win_bid_company")
        tmp_product = g_product["keyword_cp"].apply(lambda l: ",".join(l.drop_duplicates())).reset_index()
        tmp_countsum = g_product["district_news_id"].apply(lambda l: l.drop_duplicates().count()).reset_index()
        g_subscriber = data1.groupby(["win_bid_company", "subscriber"])
        tmp_subscriber = g_subscriber["district_news_id"].count().reset_index()
        max_l = list(
            tmp_subscriber["district_news_id"].groupby(tmp_subscriber["win_bid_company"]).apply(lambda l: l.idxmax()))
        tmp_subscriber = tmp_subscriber.iloc[max_l, :]
        df2 = pd.merge(tmp_product, tmp_countsum, on="win_bid_company")
        df2 = pd.merge(df2, tmp_subscriber, on="win_bid_company")
        df2.columns = ["company", "product_key", "product_count", "subscriber_product", "subscriber_product_count"]
        df3 = df2.copy()
        df3 = df3[df3["product_count"] > 1]
        district_time = data1[["win_bid_company", "district", "successful_time", "district_news_id"]]
        district_time.drop_duplicates(subset="district_news_id", inplace=True)
        district_time["district"] = district_time["district"].str[:2]
        district_time["district"] = district_time["district"].map(self.location_dict)
        district_time.sort_values("successful_time", inplace=True, ascending=False)
        district_time["successful_time"] = self.timestamp_to_time(district_time["successful_time"])
        district = district_time["district_news_id"].groupby(
            [district_time["win_bid_company"], district_time["district"]]).count()
        district = district.reset_index()
        sum = district_time["win_bid_company"].value_counts().reset_index()
        sum.columns = ["company", "sum"]
        max_index = district["district_news_id"].groupby(district["win_bid_company"]).apply(lambda l: l.idxmax())
        district = district.iloc[max_index, :]
        district.columns = ["company", "max_province", "district_count"]
        time = district_time["successful_time"].groupby(district_time["win_bid_company"]).first()
        time = time.reset_index()
        time.columns = ["company", "last_success_time"]
        df3 = pd.merge(df3, district, on="company", how="left")
        df3 = pd.merge(df3, time, on="company", how="left")
        df3 = pd.merge(df3, sum, on="company", how="left")
        df3["sum"].fillna(0, inplace=True)
        df3["sum"][df3["sum"] == 0] = df3["subscriber_product_count"]
        df3.index = range(len(df3))
        print(len(df3))
        company_list = list(df3["company"])
        final_company_result = []
        max_ = len(df3)
        if max_ > 2000:
            n1 = int(max_ / 1000)
            n2 = int(max_ % 1000)
            th_list = []
            for i in range(n1):
                th = threading.Thread(target=self.get_company, args=[range(i * 1000, (i + 1) * 1000), company_list, final_company_result],)
                th.setDaemon(True)
                th_list.append(th)
            th = threading.Thread(target=self.get_company, args=[range(n1 * 1000, n1 * 1000 + n2), company_list, final_company_result])
            th.setDaemon(True)
            th_list.append(th)
            for th in th_list:
                th.start()
            for th in th_list:
                th.join()
        elif (max_ >= 1000) & (max_ <= 2000):
            th_list = []
            th1 = threading.Thread(target=self.get_company, args=[range(1000),company_list,final_company_result])
            th2 = threading.Thread(target=self.get_company, args=[range(1000, max_),company_list,final_company_result])
            th1.setDaemon(True)
            th2.setDaemon(True)
            th_list.append(th1)
            th_list.append(th2)
            for th in th_list:
                th.start()
            for th in th_list:
                th.join()
        elif max_ < 1000:
            th1 = threading.Thread(target=self.get_company, args=[range(max_),company_list,final_company_result])
            th1.setDaemon(True)
            th1.start()
            th1.join()
        print("提取中标公司信息，进度：%.2f%%" % ((len(final_company_result) / len(df3)) * 100))
        final_result = []
        for m in final_company_result:
            final_result += m
        company_data = pd.DataFrame(final_result)
        print("提取中标公司信息完成")
        company_data = company_data[company_data["subscriber"] != ""]
        company_data.drop_duplicates("district_news_id", inplace=True)
        company_data["company"] = self.clean(company_data["company"])
        company_data["subscriber"] = self.clean(company_data["subscriber"])
        g_success_win_bid_company = company_data.groupby(["company", "subscriber"])
        tmp_top3 = g_success_win_bid_company["district_news_id"].count().reset_index()
        tmp_top3.index = range(len(tmp_top3))
        top1_index = list(tmp_top3["district_news_id"].groupby(tmp_top3["company"]).apply(lambda l: l.idxmax()))
        top1 = tmp_top3.iloc[top1_index, :]
        tmp_top3 = tmp_top3.iloc[list(set(tmp_top3.index).difference(set(top1_index))), :]
        tmp_top3.index = range(len(tmp_top3))
        top2_index = list(tmp_top3["district_news_id"].groupby(tmp_top3["company"]).apply(lambda l: l.idxmax()))
        top2 = tmp_top3.iloc[top2_index, :]
        tmp_top3 = tmp_top3.iloc[list(set(tmp_top3.index).difference(set(top2_index))), :]
        tmp_top3.index = range(len(tmp_top3))
        top3_index = list(tmp_top3["district_news_id"].groupby(tmp_top3["company"]).apply(lambda l: l.idxmax()))
        top3 = tmp_top3.iloc[top3_index, :]
        top = pd.merge(top1, top2, "left", on="company")
        top = pd.merge(top, top3, "left", on="company")
        top.columns = ["company", "top1", "top1_count", "top2", "top2_count", "top3", "top3_count"]
        top["top1"].fillna("/", inplace=True)
        top["top2"].fillna("/", inplace=True)
        top["top3"].fillna("/", inplace=True)
        top["top1_count"].fillna(0, inplace=True)
        top["top2_count"].fillna(0, inplace=True)
        top["top3_count"].fillna(0, inplace=True)
        top["top1_count"] = top["top1_count"].astype("int")
        top["top2_count"] = top["top2_count"].astype("int")
        top["top3_count"] = top["top3_count"].astype("int")
        df3 = pd.merge(df3, data2, how="left", on="subscriber_product")
        df3["subscriber_product_sum"].fillna(0, inplace=True)
        df3["subscriber_product_sum"][df3["subscriber_product_count"] > df3["subscriber_product_sum"]] = df3[
            "subscriber_product_count"]
        df3["subscriber_product_pct"] = df3["subscriber_product_count"] / df3["subscriber_product_sum"]
        df3 = pd.merge(top, df3, how="left", on="company")
        df3["是否为关系公司1"] = "/"
        df3["是否为关系公司1"][df3["subscriber_product"] == df3["top1"]] = "top1"
        df3["是否为关系公司1"][df3["subscriber_product"] == df3["top2"]] = "top2"
        df3["是否为关系公司1"][df3["subscriber_product"] == df3["top3"]] = "top3"
        # 利用伯努利分布计算中标公司未来三月中标几率的置信水平95.4%的置信区间
        data1.drop_duplicates(subset="district_news_id", inplace=True)
        company = data1[["win_bid_company", "successful_time", "district_news_id"]]
        company = company[company["successful_time"] != 0]
        company_list = list(df3["company"].drop_duplicates())
        tmp_data = pd.DataFrame()
        l1, l2, l3, l4, l5 = [], [], [], [], []
        n1 = round(len(company_list) / 4)
        n2 = len(company_list) % n1
        t_list = []
        for i in range(3):
            th = threading.Thread(target=self.bernoulli_distribution,
                                  args=(company_list, company, range(n1 * i, n1 * (i + 1)), "win_bid_company", l1, l2, l3, l4, l5))
            th.setDaemon(True)
            t_list.append(th)
        th = threading.Thread(target=self.bernoulli_distribution,
                              args=(company_list, company, range(n1 * 3, n1 * 4 + n2), "win_bid_company", l1, l2, l3, l4, l5))
        th.setDaemon(True)
        t_list.append(th)
        for th in t_list:
            th.start()
        for th in t_list:
            th.join()
        tmp_data["company"] = l1
        tmp_data["mean"] = l2
        tmp_data["std"] = l3
        tmp_data["low"] = l4
        tmp_data["high"] = l5
        df3 = pd.merge(df3, tmp_data, how="left", on="company")
        subscriber = data1[["subscriber", "successful_time", "district_news_id"]]
        subscriber = subscriber[subscriber["successful_time"] != 0]
        tmp_subscriber = pd.DataFrame()
        subscribers = df3["subscriber_product"].drop_duplicates()
        subscribers = list(subscribers)
        l1, l2, l3, l4, l5 = [], [], [], [], []
        n1 = round(len(subscribers) / 4)
        n2 = len(subscribers) % n1
        t_list = []
        for i in range(3):
            th = threading.Thread(target=self.bernoulli_distribution,
                                  args=(subscribers, subscriber, range(n1 * i, n1 * (i + 1)), "subscriber", l1, l2, l3, l4, l5))
            th.setDaemon(True)
            t_list.append(th)
        th = threading.Thread(target=self.bernoulli_distribution,
                              args=(subscribers, subscriber, range(n1 * 3, n1 * 4 + n2), "subscriber", l1, l2, l3, l4, l5))
        th.setDaemon(True)
        t_list.append(th)
        for th in t_list:
            th.start()
        for th in t_list:
            th.join()
        tmp_subscriber["subscriber"] = l1
        tmp_subscriber["mean"] = l2
        tmp_subscriber["std"] = l3
        tmp_subscriber["low"] = l4
        tmp_subscriber["high"] = l5
        df3 = pd.merge(df3, tmp_subscriber, how="left", left_on="subscriber_product", right_on="subscriber")
        df3.fillna("/", inplace=True)
        df3 = df3[["company", "last_success_time", "product_key", "product_count", "mean_x", "std_x", "low_x", "high_x",
                   "subscriber_product", "是否为关系公司1", "subscriber_product_count", "subscriber_product_sum",
                   "subscriber_product_pct", "mean_y", "std_y", "low_y", "high_y", "max_province", "district_count",
                   "top1", "top1_count", "top2", "top2_count", "top3", "top3_count"]]

        data3 = data_host.copy()
        data3 = data3[data3["win_bid_company"] != ""]
        data3.drop_duplicates(subset="district_news_id", inplace=True)
        data3 = data3["win_bid_company"].value_counts().reset_index()
        data3.columns = ["company_product", "company_product_sum"]
        g_product = data_host.groupby("subscriber")
        tmp_product = g_product["keyword_cp"].apply(lambda l: ",".join(l.drop_duplicates())).reset_index()
        tmp_countsum = g_product["district_news_id"].apply(lambda l: l.drop_duplicates().count()).reset_index()
        g_subscriber = data_host.groupby(["subscriber", "win_bid_company"])
        tmp_subscriber = g_subscriber["district_news_id"].count().reset_index()
        max_l = list(
            tmp_subscriber["district_news_id"].groupby(tmp_subscriber["subscriber"]).apply(lambda l: l.idxmax()))
        tmp_subscriber = tmp_subscriber.iloc[max_l, :]
        df4 = pd.merge(tmp_product, tmp_countsum, on="subscriber")
        df4 = pd.merge(df4, tmp_subscriber, on="subscriber")
        df4.columns = ["subscriber", "product_key", "product_count", "company_product", "company_product_count"]
        df5 = df4.copy()
        # 剔除招标次数少于1的甲方记录
        df5 = df5[df5["company_product_count"] > 1]
        district_time = data_host[["subscriber", "district", "successful_time", "district_news_id", "win_bid_company"]]
        district_time.drop_duplicates(subset="district_news_id", inplace=True)
        district_time["district"] = district_time["district"].str[:2]
        district_time["district"] = district_time["district"].map(self.location_dict)
        district_time.sort_values("successful_time", inplace=True, ascending=False)
        district_time["successful_time"] = self.timestamp_to_time(district_time["successful_time"])
        district = district_time["district_news_id"].groupby(
            [district_time["subscriber"], district_time["district"]]).count()
        district = district.reset_index()
        sum = district_time["subscriber"].value_counts().reset_index()
        sum.columns = ["subscriber", "sum"]
        max_index = district["district_news_id"].groupby(district["subscriber"]).apply(lambda l: l.idxmax())
        district = district.iloc[max_index, :]
        district.columns = ["subscriber", "max_province", "district_count"]
        district = district[["subscriber", "max_province"]]
        time = district_time[["successful_time", "win_bid_company"]].groupby(district_time["subscriber"]).first()
        time = time.reset_index()
        time.columns = ["subscriber", "last_success_time", "win_bid_company"]
        df5 = pd.merge(df5, district, on="subscriber", how="left")
        df5 = pd.merge(df5, time, on="subscriber", how="left")
        df5 = pd.merge(df5, sum, on="subscriber", how="left")
        df5["sum"].fillna(0, inplace=True)
        df5["sum"][df5["sum"] == 0] = df5["product_count"]
        df5.index = range(len(df5))
        print(len(df5))
        subscriber_list = list(df5["subscriber"])
        final_company_result = []
        max_ = len(df5)
        if max_ > 2000:
            n1 = int(max_ / 1000)
            n2 = int(max_ % 1000)
            th_list = []
            for i in range(n1):
                th = threading.Thread(target=self.get_subscriber, args=[range(i * 1000, (i + 1) * 1000), subscriber_list, final_company_result])
                th.setDaemon(True)
                th_list.append(th)
            th = threading.Thread(target=self.get_subscriber, args=[range(n1 * 1000, n1 * 1000 + n2), subscriber_list, final_company_result])
            th.setDaemon(True)
            th_list.append(th)
            for th in th_list:
                th.start()
            for th in th_list:
                th.join()
        elif (max_ >= 1000) & (max_ <= 2000):
            th_list = []
            th1 = threading.Thread(target=self.get_subscriber, args=[range(1000), subscriber_list, final_company_result])
            th2 = threading.Thread(target=self.get_subscriber, args=[range(1000, max_), subscriber_list, final_company_result])
            th1.setDaemon(True)
            th2.setDaemon(True)
            th_list.append(th1)
            th_list.append(th2)
            for th in th_list:
                th.start()
            for th in th_list:
                th.join()
        elif max_ < 1000:
            th1 = threading.Thread(target=self.get_subscriber, args=[range(max_), subscriber_list, final_company_result])
            th1.setDaemon(True)
            th1.start()
            th1.join()

        print("提取甲方信息，进度：%.2f%%" % ((len(final_company_result) / len(df5)) * 100))
        final_result = []
        for m in final_company_result:
            final_result += m
        company_data = pd.DataFrame(final_result)
        print("提取甲方信息完成")
        company_data = company_data[company_data["win_bid_company"] != ""]
        company_data.drop_duplicates("district_news_id", inplace=True)
        company_data["win_bid_company"] = self.clean(company_data["win_bid_company"])
        company_data["subscriber"] = self.clean(company_data["subscriber"])
        g_success_win_bid_company = company_data.groupby(["subscriber", "win_bid_company"])
        tmp_top3 = g_success_win_bid_company["district_news_id"].count().reset_index()
        tmp_top3.index = range(len(tmp_top3))
        top1_index = list(tmp_top3["district_news_id"].groupby(tmp_top3["subscriber"]).apply(lambda l: l.idxmax()))
        top1 = tmp_top3.iloc[top1_index, :]
        tmp_top3 = tmp_top3.iloc[list(set(tmp_top3.index).difference(set(top1_index))), :]
        tmp_top3.index = range(len(tmp_top3))
        top2_index = list(tmp_top3["district_news_id"].groupby(tmp_top3["subscriber"]).apply(lambda l: l.idxmax()))
        top2 = tmp_top3.iloc[top2_index, :]
        tmp_top3 = tmp_top3.iloc[list(set(tmp_top3.index).difference(set(top2_index))), :]
        tmp_top3.index = range(len(tmp_top3))
        top3_index = list(tmp_top3["district_news_id"].groupby(tmp_top3["subscriber"]).apply(lambda l: l.idxmax()))
        top3 = tmp_top3.iloc[top3_index, :]
        top = pd.merge(top1, top2, "left", on="subscriber")
        top = pd.merge(top, top3, "left", on="subscriber")
        top.columns = ["subscriber", "top1", "top1_count", "top2", "top2_count", "top3", "top3_count"]
        top["top1"].fillna("/", inplace=True)
        top["top2"].fillna("/", inplace=True)
        top["top3"].fillna("/", inplace=True)
        top["top1_count"].fillna(0, inplace=True)
        top["top2_count"].fillna(0, inplace=True)
        top["top3_count"].fillna(0, inplace=True)
        top["top1_count"] = top["top1_count"].astype("int")
        top["top2_count"] = top["top2_count"].astype("int")
        top["top3_count"] = top["top3_count"].astype("int")

        df5 = pd.merge(df5, data3, how="left", on="company_product")
        df5["company_product_sum"].fillna(0, inplace=True)
        df5["company_product_sum"][df5["company_product_count"] < df5["company_product_sum"]] = df5[
            "company_product_sum"]
        df5["company_product_pct"] = df5["company_product_count"] / df5["company_product_sum"]
        df5 = pd.merge(top, df5, how="left", on="subscriber")

        df5["是否为关系公司1"] = "/"
        df5["是否为关系公司1"][df5["company_product"] == df5["top1"]] = "top1"
        df5["是否为关系公司1"][df5["company_product"] == df5["top2"]] = "top2"
        df5["是否为关系公司1"][df5["company_product"] == df5["top3"]] = "top3"
        # 利用伯努利分布计算中标公司未来三月中标几率的置信水平95.4%的置信区间
        data_host.drop_duplicates(subset="district_news_id", inplace=True)
        subscriber = data_host[["subscriber", "successful_time", "district_news_id"]]
        subscribers = list(df5["subscriber"].drop_duplicates())
        tmp_data = pd.DataFrame()
        l1, l2, l3, l4, l5 = [], [], [], [], []
        n1 = round(len(subscribers) / 4)
        n2 = len(subscribers) % n1
        t_list = []
        for i in range(3):
            th = threading.Thread(target=self.bernoulli_distribution,
                                  args=(subscribers, subscriber, range(n1 * i, n1 * (i + 1)), "subscriber", l1, l2, l3, l4, l5))
            th.setDaemon(True)
            t_list.append(th)
        th = threading.Thread(target=self.bernoulli_distribution,
                              args=(subscribers, subscriber, range(n1 * 3, n1 * 4 + n2), "subscriber", l1, l2, l3, l4, l5))
        th.setDaemon(True)
        t_list.append(th)
        for th in t_list:
            th.start()
        for th in t_list:
            th.join()
        tmp_data["subscriber"] = l1
        tmp_data["mean"] = l2
        tmp_data["std"] = l3
        tmp_data["low"] = l4
        tmp_data["high"] = l5
        df5 = pd.merge(df5, tmp_data, how="left", on="subscriber")
        company = data_host[["win_bid_company", "successful_time", "district_news_id"]]
        company_list = list(df5["company_product"].drop_duplicates())
        tmp_company = pd.DataFrame()
        l1, l2, l3, l4, l5 = [], [], [], [], []
        n1 = round(len(company_list) / 4)
        n2 = len(company_list) % n1
        t_list = []
        for i in range(3):
            th = threading.Thread(target=self.bernoulli_distribution,
                                  args=(company_list, company, range(n1 * i, n1 * (i + 1)), "win_bid_company", l1, l2, l3, l4, l5))
            th.setDaemon(True)
            t_list.append(th)
        th = threading.Thread(target=self.bernoulli_distribution,
                              args=(company_list, company, range(n1 * 3, n1 * 4 + n2), "win_bid_company", l1, l2, l3, l4, l5))
        th.setDaemon(True)
        t_list.append(th)
        for th in t_list:
            th.start()
        for th in t_list:
            th.join()
        tmp_company["company_product"] = l1
        tmp_company["mean"] = l2
        tmp_company["std"] = l3
        tmp_company["low"] = l4
        tmp_company["high"] = l5
        df5 = pd.merge(df5, tmp_company, how="left", on="company_product")
        df5.fillna("/", inplace=True)
        df5 = df5[["subscriber", "last_success_time", "win_bid_company", "product_key", "product_count", "max_province",
                   "mean_x", "std_x", "low_x", "high_x", "company_product", "是否为关系公司1", "company_product_count",
                   "company_product_sum", "company_product_pct", "mean_y", "std_y", "low_y", "high_y", "top1",
                   "top1_count", "top2", "top2_count", "top3", "top3_count"]]
        write = pd.ExcelWriter("C:/Users/Administrator/Desktop/" + self.key + ".xlsx")
        df3.to_excel(write, sheet_name="主键：中标公司")
        df5.to_excel(write, sheet_name="主键：甲方")
        write.close()


if __name__ == '__main__':
    type = input("中标公司与甲方是否使用相同关键词？（0：不是，1：是）")
    if type == "0":
        l_product = input("请输入中标公司维度关键词").split(",")
        l_high_product = input("请输入甲方维度关键词").split(",")
    elif type == "1":
        l_product = input("请输入中标公司维度关键词").split(",")
        l_high_product = []
    key = input("公司名")
    g = Builder(type,l_product,l_high_product,key)
    g.run()