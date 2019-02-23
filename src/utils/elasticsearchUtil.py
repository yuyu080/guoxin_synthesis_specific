# -*- encoding = utf-8 -*-
import os
import traceback
import logging

task_logger = logging.getLogger('task_logger')

class ES_Utiles:

    def __init__(self,es, LOCAL_ES_SOURCE, GUOXING_OUT,GUOXING_IN):
        self.es = es
        self.LOCAL_ES_SOURCE = LOCAL_ES_SOURCE
        self.GUOXING_OUT = GUOXING_OUT
        self.GUOXING_IN = GUOXING_IN

    def source(self, res, source):
        # source = ["bbdQyxxId", "regcapAmount", "address", "frname", "companyName"]
        s = ''
        for i in source:
            s += str(res[i]) + '|'
        return s[:-1] + '\n'

    def _first_query_by_fieldCode(self,index_, _source, order_field, search_item, itemCode):
        # index_ = 'cjpt.qyxx_basic'
        # _source = '["bbdQyxxId", "regcapAmount", "address", "frname", "companyName"]'
        try:
            rs = self.es.search(
                index=index_,
                # doc_type='qyxx_basic',
                body={
                    "query": {
                        "term": {search_item: itemCode}
                    },
                    "_source": _source,
                    "size": 1000,
                    "sort": [
                        {
                            order_field: {
                                "order": "desc"
                            }
                        }
                    ]
                })
            return rs
        except Exception as e:
            # raise Exception("{0} search error".format(index_)+"------errorMsg------"+repr(e))
            task_logger.error("{0} search error".format(index_), exc_info=True)
            self.print_error("{0} search error".format(index_) + "------errorMsg------", e)

    def _first_query(self,index_, _source, order_field):
        try:
            rs = self.es.search(
                index=index_,
                # doc_type='qyxx_basic',
                body={
                    "query": {
                        "match_all": {}
                    },
                    "_source": _source,
                    "size": 1000,
                    "sort": [
                        {
                            order_field: {
                                "order": "desc"
                            }
                        }
                    ]
                })
            return rs
        except Exception as e:
            # raise Exception("{0} search error".format(index_)+"------errorMsg------"+repr(e))
            task_logger.error("{0} search error".format(index_), exc_info=True)
            self.print_error("{0} search error".format(index_) + "------errorMsg------", e)

    def _get_first_data(self,first_rs, _source, file_name, order_field):
        i = 0
        if first_rs:
            temp_data = []
            for hit in first_rs['hits']['hits']:
                res = hit['_source']
                id = hit['_source'][order_field]
                # id = hit['_id']
                if res is None:
                    res = ""
                temp_data.append(res)
                open(self.LOCAL_ES_SOURCE + file_name, mode='a'). \
                    write(self.source(res, _source))
                i += 1
                if i == 1000:
                    return id

    def _second_query_by_fieldCode(self,index_, _source, id, order_field, search_item, itemCode):
        try:
            rs = self.es.search(
                index=index_,
                body={
                    "size": 1000,
                    "query": {
                        "bool": {
                            "filter": {
                                "range": {
                                    order_field: {
                                        "lt": id
                                    }
                                }
                            },
                            "must": [
                                {"term": {search_item: itemCode}}
                            ]
                        }
                    },
                    "sort": [
                        {
                            order_field: {
                                "order": "desc"
                            }
                        }
                    ],
                    "_source": _source
                })
            return rs
        except Exception as e:
            # raise Exception("{0} search error".format(index_) + "------errorMsg------" + repr(e))
            task_logger.error("{0} search error".format(index_), exc_info=True)
            self.print_error("{0} search error".format(index_) + "------errorMsg------", e)


    def _second_query(self,index_, _source, id, order_field):
        try:
            rs = self.es.search(
                index=index_,
                body={
                    "size": 1000,
                    "query": {
                        "bool": {
                            "filter": {
                                "range": {
                                    order_field: {
                                        "lt": id
                                    }
                                }
                            }
                        }
                    },
                    "sort": [
                        {
                            order_field: {
                                "order": "desc"
                            }
                        }
                    ],
                    "_source": _source
                })
            return rs
        except Exception as e:
            # raise Exception("{0} search error".format(index_) + "------errorMsg------" + repr(e))
            task_logger.error("{0} search error".format(index_), exc_info=True)
            self.print_error("{0} search error".format(index_) + "------errorMsg------", e)

    # taskId为“null”时获取全部es数据，不为空时获取指定任务的数据
    # 注意：source里面必须包含排序字段

    def get_and_save_es_data(self,index_, _source, file_name, order_field, search_item, itemCode, retry_times=1):
        if retry_times == 1:
            os.system("rm {path}".format(path=self.LOCAL_ES_SOURCE + '{name}'.format(name=file_name)))
            os.system("hdfs dfs -rm {path}".format(path=self.GUOXING_IN + '{name}'.format(name=file_name)))
        print("get es data {name}...".format(name=index_))
        task_logger.info("开始获取样本：index={}, source={}, search_item={}, itemCode={}".format(index_, _source,
                                                                                          search_item, itemCode))
        try:
            if itemCode != "":
                first_rs = self._first_query_by_fieldCode(index_, _source, order_field, search_item, itemCode)
            else:
                first_rs = self._first_query(index_, _source, order_field)
            first_id = self._get_first_data(first_rs, _source, file_name, order_field)
            # print(first_id)
            while first_id:
                if itemCode != "":
                    second_rs = self._second_query_by_fieldCode(index_, _source, first_id, order_field, search_item,
                                                           itemCode)
                else:
                    second_rs = self._second_query(index_, _source, first_id, order_field)
                first_id = self._get_first_data(second_rs, _source, file_name, order_field)
                if first_id is None:
                    break
                # print(first_id)
        except Exception as e:
            print("get es data {name} error!".format(name=index_))
            if (retry_times < 4):
                print('retry {times}...'.format(times=retry_times))
                retry_times = retry_times + 1
                self.get_and_save_es_data(index_, _source, file_name, order_field, search_item, itemCode, retry_times)
            else:
                task_logger.error("{0} search error".format(index_), exc_info=True)
                self.print_error("{0} search error".format(index_), e)
                # raise Exception("{0} search error".format(index_))
        print("get es data {name} done!".format(name=index_))
        task_logger.info("获取样本成功：index={}, source={}, search_item={}, itemCode={}".format(index_, _source,
                                                                                          search_item, itemCode))
        os.system("hdfs dfs -rmr {path}".format(path=self.GUOXING_OUT + '{name}'.format(name=file_name)))
        os.system("hdfs dfs -put {path1} {path2}".format(path1=self.LOCAL_ES_SOURCE + '{name}'.format(name=file_name),
                                                         path2=self.GUOXING_OUT))

    def print_error(info, e):
        print(info)
        traceback.print_exc()
        # raise Exception(info + repr(e))
