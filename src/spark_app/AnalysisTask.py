import os
import sys
import argparse
import traceback
import datetime
import logging
import json

import pandas as pd
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

from handler import *
from utils.elasticsearchUtil import ES_Utiles
from utils.logUtil import create_logger
from common import *

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = BASE_DIR + '/logs/'
request_logger = create_logger(log_dir=LOG_DIR,
                               logger_name='request_logger',
                               file_name='request.log')
task_logger = create_logger(log_dir=LOG_DIR,
                               logger_name='task_logger',
                               file_name='task.log')

class Sample:

    def __init__(self, field_id, index_cols, has_serire=False):
        partitions = self.get_table_partition(INDEX_TABLE)
        if not has_serire:
            # 默认取最新的数据
            self.df = self.get_pandas_df(
                field_id, index_cols, partitions[-1])
        else:
            # 取一个序列数据
            self.dfs = list(
                map(
                    lambda each_partition: (
                        datetime.datetime.strptime(each_partition, '%Y%m%d').strftime('%Y-%m'),
                        self.get_pandas_df(
                            field_id, index_cols, each_partition)),
                    partitions))
            self.df = self.dfs[-1][1]

    @staticmethod
    def get_table_partition(table):
        '''获取所有分区'''
        dts = sorted(map(
            lambda r: r.partition.split('=')[1],
            spark.sql('show partitions {}'.format(table)).collect()
        ))
        target_dt = []
        last_dt_month = ''
        for each_dt in dts:
            now_dt_month = datetime.datetime.strptime(each_dt, '%Y%m%d').strftime('%Y-%m')
            if now_dt_month != last_dt_month:
                target_dt.append(each_dt)
            else:
                pass
            last_dt_month = now_dt_month

        return target_dt

    @staticmethod
    def check_col_exist(index_cols, table_cols):
        '''判断字段是否存在'''
        s0 = set(['company_county', 'company_industry',
                  'company_province', 'company_type', 'openfrom'])
        s1 = set(index_cols)
        s2 = set(table_cols)
        return list(s2.intersection(s1.difference(s0)))

    def get_pandas_df(self, field_id, index_cols, table_dt):
        '''获取单个样本'''
        # 读取基础指标
        index_df = spark.sql("select * from {} where dt='{}'".format(INDEX_TABLE, table_dt))
        # 获取样本
        sample_hdfs_path = "{}/field_name_list_{}".format(HDFS_OUT, field_id)
        sample_df = spark.read.csv(
            sample_hdfs_path, sep="|"
        ).withColumnRenamed(
            '_c0', 'bbd_qyxx_id'
        ).withColumnRenamed(
            '_c1', 'company_name'
        )

        # 获取指标
        tid_df = sample_df.select(
            'bbd_qyxx_id'
        ).join(
            index_df,
            'bbd_qyxx_id'
        ).dropDuplicates(
            ['bbd_qyxx_id']
        ).select(
            index_df.city.alias('company_county'),
            index_df.company_industry,
            index_df.province.alias('company_province'),
            index_df.company_type,
            fun.when(
                index_df.esyear < 2, '0-2年'
            ).when(
                index_df.esyear < 7, '2-7年'
            ).when(
                index_df.esyear >= 7, '7年以上'
            ).otherwise(
                '0-2年'
            ).alias('openfrom'),
            *Sample.check_col_exist(index_cols, index_df.columns)
        )

        return tid_df.toPandas().fillna(0).fillna('-')

    def get_pandas_dfs(field_id, index_cols, table_dt):
        pass


class SampleTest:

    def __init__(self, field_id):
        self.df = pd.read_pickle(field_id)


class Synthesis:
    # 分析类型字典
    analysis_type = {
        'BEHAVIOR': 'behavioural_analysis',
        'TIME': 'time_series_analysis',
        'DISTRIBUTION': 'distribution_analysis'
    }
    analysis_type_reverse = {v: k for k, v in analysis_type.items()}

    # 分析方法字典
    analysis_method = {
        'FEATURE': 'total_analysis',
        'CORRELATION': 'correlation_analysis',
        'CLUSTERING': 'clustering_analysis',
        'TREND_HISTORY': 'history',
        'TREND_PREDICTION': 'prediction',
        'CORRELATION': 'correlation_analysis',
        'REGION': 'company_county',
        'INDUSTRY': 'company_industry',
        'COMPANY_TYPE': 'company_type',
        'AGE_LIMIT': 'openfrom'
    }
    analysis_method_reverse = {v: k for k, v in analysis_method.items()}

    @staticmethod
    def get_analysis_method(each_task):
        '''根据传入字段判断分析方法'''
        if each_task['ordinate'] != 'REGION':
            return Specific.analysis_method[each_task['ordinate']]
        else:
            # 如果省份不为空就按省份统计，要么就按城市统计
            province = [each_region['province'] for each_region in each_task['region'] if each_region['province']]
            city = [each_region['city'] for each_region in each_task['region'] if each_region['city']]
            if province:
                return 'company_province'
            elif city:
                return 'company_county'
            else:
                return 'company_province'

    @staticmethod
    def get_synthesis_args_obj(input_args, col_mapping):
        '''构造综合分析task参数'''
        obj = json.loads(input_args)
        field_id = obj['data']['taskModel'][0]['analysisList']
        tasks = []
        cols = []
        for each_task in obj['data']['taskModel']:
            # 构造计算参数
            arg = {
                'analysis_model': 'synthesis_analysis',
                'analysis_type': Synthesis.analysis_type[each_task['analysisType']],
                'analysis_method': Synthesis.analysis_method[each_task['analysisMethod']],
                'index_id': each_task['indexId'][0],
                'index_ids': each_task['indexId'],
                'index_type': col_mapping.get(each_task['indexId'][0], ''),
                'image_type': each_task.get('imageType', ''),
                'result_type': each_task['resultType'],
                'task_id': each_task['remoteTaskId'],
                'query': ''
            }
            cols.extend(each_task['indexId'])
            tasks.append(arg)

        return {
            'tasks': tasks, 'field_id': field_id, 'cols': list(set(cols)), 'total_task_id': obj['data']['taskId']
        }

    @staticmethod
    def format_synthesis_obj(result, total_task_id):
        '''构造综合分析响应参数'''
        return {
            'taskId': total_task_id,
            'resultPath': '',
            'taskStatus': 2 if 2 in [each_task['task_status'] for each_task in result] else 2,
            'resultInfo': [{
                'remoteTaskId': each_task['task_id'],
                'result': each_task['task_result'],
                'remoteTaskStatus': each_task['task_status'],
                'imageType': each_task['image_type'],
                'resultType': each_task['result_type']
            } for each_task in result]
        }

    @staticmethod
    def execut_task(tasks, total_task_id):
        '''执行task'''
        for arg in tasks:
            try:
                analysisTask = AnalysisTask(arg)
                result = analysisTask.get_statistical_information()
                arg['task_result'] = result
                # 根据返回数据判断状态
                if result == 'no analysis type' or 'ERROR' in result:
                    arg['task_status'] = 3
                else:
                    arg['task_status'] = 2

            except Exception as e:
                task_logger.error("子任务失败: {}".format(arg['task_id']), exc_info=True)
                traceback.print_exc()
                arg['task_result'] = "ERROR: " + repr(e).replace("\'", '')
                # 失败
                arg['task_status'] = 3
        return Synthesis.format_synthesis_obj(tasks, total_task_id)


class Specific:
    # 分析类型字典
    analysis_type = {
        'BEHAVIOR': 'behavioural_analysis',
        'TIME': 'time_series_analysis',
        'DISTRIBUTION': 'distribution_analysis'
    }
    analysis_type_reverse = {v: k for k, v in analysis_type.items()}

    # 分析方法字典
    analysis_method = {
        'FEATURE': 'total_analysis',
        'CORRELATION': 'correlation_analysis',
        'CLUSTERING': 'clustering_analysis',
        'TREND_HISTORY': 'history',
        'TREND_PREDICTION': 'prediction',
        'CORRELATION': 'correlation_analysis',
        'REGION': 'company_county',
        'INDUSTRY': 'company_industry',
        'COMPANY_TYPE': 'company_type',
        'AGE_LIMIT': 'openfrom'
    }
    analysis_method_reverse = {v: k for k, v in analysis_method.items()}

    @staticmethod
    def get_query(region):
        '''构造查询'''
        def get_item_query(item):
            province = item['province']
            city = item['city']
            if province and city:
                return "(company_province=='{}'&company_county=='{}')".format(province, city)
            elif province:
                return "company_province=='{}'".format(province)
            elif city:
                return "company_county=='{}'".format(city)
            else:
                return ''
        return '|'.join(filter(None, map(lambda item: get_item_query(item), region)))

    @staticmethod
    def get_analysis_method(each_task):
        '''根据传入字段判断分析方法'''
        if each_task['ordinate'] != 'REGION':
            return Specific.analysis_method[each_task['ordinate']]
        else:
            # 如果省份不为空就按省份统计，要么就按城市统计
            province = [each_region['province'] for each_region in each_task['region'] if each_region['province']]
            city = [each_region['city'] for each_region in each_task['region'] if each_region['city']]
            if province:
                return 'company_province'
            elif city:
                return 'company_county'
            else:
                return 'company_province'

    @staticmethod
    def get_specific_args_obj(input_args, col_mapping):
        '''构造专题分析参数'''
        obj = json.loads(input_args)
        field_id = obj['data']['taskModel']['fieldId']
        tasks = []
        cols = []
        for each_task in obj['data']['taskModel']['ruleList']:
            # 构造计算参数
            arg = {
                'analysis_model': 'specific_analysis',
                'analysis_type': Specific.analysis_type[each_task['analyseType']],
                'analysis_method': Specific.get_analysis_method(each_task),
                'index_id': each_task['indexCode'],
                'index_ids': [],
                'index_type': col_mapping.get(each_task['indexCode'], ''),
                'task_id': each_task['calcRuleUid'],
                'query': Specific.get_query(each_task['region'])
            }
            cols.append(each_task['indexCode'])
            tasks.append(arg)

        return {
            'tasks': tasks, 'field_id': field_id, 'cols': list(set(cols)), 'total_task_id': obj['data']['taskId']
        }

    @staticmethod
    def format_specific_obj(result, total_task_id):
        '''构造综合分析响应参数'''
        return {
            'taskId': total_task_id,
            'resultPath': '',
            'taskStatus': 2 if 2 in [each_task['task_status'] for each_task in result] else 3,
            'resultInfo': [{
                'remoteTaskId': each_task['task_id'],
                'result': each_task['task_result'],
                'remoteTaskStatus': each_task['task_status']
            } for each_task in result]
        }

    @staticmethod
    def execut_task(tasks, total_task_id):
        '''执行task'''
        for arg in tasks:
            try:
                analysisTask = AnalysisTask(arg)
                result = analysisTask.get_statistical_information()
                arg['task_result'] = result
                # 根据返回数据判断状态
                if result == 'no analysis type' or 'ERROR' in result:
                    arg['task_status'] = 3
                else:
                    arg['task_status'] = 2
            except Exception as e:
                task_logger.error("子任务失败: {}".format(arg['task_id']), exc_info=True)
                traceback.print_exc()
                arg['task_result'] = "ERROR: " + repr(e).replace("\'", '')
                # 失败
                arg['task_status'] = 3
        return Specific.format_specific_obj(tasks, total_task_id)


class AnalysisTask:
    '''分析任务'''

    def __init__(self, args):
        self.analysis_model = args['analysis_model']
        self.analysis_type = args['analysis_type']
        self.analysis_method = args['analysis_method']
        self.index_id = args['index_id']
        self.index_ids = args['index_ids']
        self.index_type = args['index_type']
        self.query = args['query']
        self.df = sample.df
        self.dfs = sample.dfs
        self.local_path = LOCAL_PATH
        self.hdfs_path = HDFS_PATH

    def get_statistical_information(self):
        if self.analysis_model == 'specific_analysis':

            # 行为分析
            if self.analysis_type == 'behavioural_analysis':
                grouped = BehaviouralAnalysis.grouped_analysis(
                    self.df, self.index_id,
                    self.analysis_method, self.query)
                if self.index_type == 'disperse':
                    return CalculateMethod.value_counts(grouped)
                elif self.index_type == 'continuous':
                    return CalculateMethod.group_info(grouped)
                else:
                    return {}

            # 时间序列分析
            if self.analysis_type == 'time_series_analysis':
                grouped_series = TimeSeriesAnalysis.grouped_analysis(
                    map(lambda t: t[1], self.dfs), self.index_id,
                    self.analysis_method, self.query)
                result = map(
                    CalculateMethod.mean,
                    grouped_series)
                result_series = dict(zip(map(lambda t: t[0], self.dfs), result))
                return CalculateMethod.format_dict_2(result_series)

            # 分布分析
            if self.analysis_type == 'distribution_analysis':
                grouped = DistributionAnalysis.grouped_analysis(
                    self.df, self.index_id,
                    self.analysis_method, self.query)
                if self.index_type == 'disperse':
                    return CalculateMethod.value_counts(grouped)
                elif self.index_type == 'continuous':
                    return CalculateMethod.group_info(grouped)
                else:
                    return {}

            return 'no analysis type'

        elif self.analysis_model == 'synthesis_analysis':

            # 行为分析
            if self.analysis_type == 'behavioural_analysis':
                # 行为特征分析
                if self.analysis_method == 'total_analysis':
                    feature = BehaviouralAnalysis.total_analysis(self.df, self.index_id)
                    if self.index_type == 'disperse':
                        return CalculateMethod.value_counts(feature)
                    elif self.index_type == 'continuous':
                        return CalculateMethod.describe_info(feature)
                    else:
                        return {}

                # 行为相关性分析
                if self.analysis_method == 'correlation_analysis':
                    path = BehaviouralAnalysis.correlation_analysis(
                        self.df,
                        self.index_ids,
                        self.local_path,
                        self.hdfs_path
                    )
                    return path

                # 行为聚类分析
                if self.analysis_method == 'clustering_analysis':
                    path = BehaviouralAnalysis.correlation_analysis(
                        self.df,
                        self.index_ids,
                        self.local_path,
                        self.hdfs_path
                    )
                    return path

            # 时序分析
            if self.analysis_type == 'time_series_analysis':

                # 历史趋势分析
                if self.analysis_method == 'history':
                    feature_series = TimeSeriesAnalysis.total_analysis(
                        map(lambda t: t[1], self.dfs),
                        self.index_id)
                    result = map(
                        CalculateMethod.mean,
                        feature_series)
                    result_series = dict(zip(map(lambda t: t[0], self.dfs), result))
                    return CalculateMethod.format_dict_2(result_series)

                # 趋势预测分析
                if self.analysis_method == 'prediction':
                    feature_series = TimeSeriesAnalysis.total_analysis(
                        map(lambda t: t[1], self.dfs),
                        self.index_id)
                    result = map(
                        CalculateMethod.mean,
                        feature_series)
                    now_series = dict(zip(map(lambda t: t[0], self.dfs), result))
                    return TimeSeriesAnalysis.prediction_analysis(now_series)

                # 时序相关性分析
                if self.analysis_method == 'correlation_analysis':
                    time_series = []
                    for each_index in self.index_ids:
                        feature_series = TimeSeriesAnalysis.total_analysis(
                            map(lambda t: t[1], self.dfs),
                            each_index)
                        result = map(
                            CalculateMethod.mean,
                            feature_series)
                        time_series.append(dict(zip(map(lambda t: t[0], self.dfs), result)))
                    cor_df = pd.DataFrame(time_series).fillna(0).T
                    return TimeSeriesAnalysis.correlation_analysis(cor_df, self.local_path, self.hdfs_path)

            # 分布分析
            if self.analysis_type == 'distribution_analysis':
                grouped = DistributionAnalysis.grouped_analysis(
                    self.df, self.index_id, self.analysis_method)
                if self.index_type == 'disperse':
                    return CalculateMethod.value_counts(grouped)
                elif self.index_type == 'continuous':
                    return CalculateMethod.group_info(grouped)
                else:
                    return {}

            return 'no analysis type'

        else:
            return {}


def get_spark_session():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 5)
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.executor.instances", 10)
    conf.set("spark.executor.cores", 2)
    conf.set("spark.python.worker.memory", "3g")
    conf.set("spark.default.parallelism", 600)
    conf.set("spark.sql.shuffle.partitions", 600)
    conf.set("spark.broadcast.blockSize", 1024)
    conf.set("spark.executor.extraJavaOptions",
             "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    conf.set("spark.submit.pyFiles",
             "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/dafei_keyword.py")

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def get_col_continuity(mapping_table):
    # 获取指标类型
    mapping = spark.sql("select * from {}".format(mapping_table))
    data = mapping.select(
        'index',
        fun.when(
            mapping.continuity == '离散', 'disperse'
        ).when(
            mapping.continuity == '连续', 'continuous'
        ).alias('continuity')
    ).collect()
    return {row.asDict()['index']: row.asDict()['continuity'] for row in data}


def get_and_save_es_data(field_id):
    '''从es获取样本，下载到本地，并上传HDFS'''
    es = Elasticsearch([{'host': ES_NODES, 'port': ES_PORT}])
    es_utils = ES_Utiles(es, LOCAL_ES_SOURCE, HDFS_OUT, HDFS_IN)
    es_utils.get_and_save_es_data('common_company_field', ['bbd_qyxx_id', 'company_name'],
                                  'field_name_list_' + field_id,
                                  'bbd_qyxx_id', 'fieldId', field_id)


class Test:
    args1 = {
        'analysis_model': 'specific_analysis',
        'analysis_type': 'behavioural_analysis',
        'analysis_method': 'company_province',
        'index_id': 'punish_type',
        'index_ids': [],
        'index_type': 'disperse'
    }

    args2 = {
        'analysis_model': 'specific_analysis',
        'analysis_type': 'distribution_analysis',
        'analysis_method': 'company_industry',
        'index_id': 'frgd_cxsj_avg',
        'index_ids': [],
        'index_type': 'continuous'
    }

    args3 = {
        'analysis_model': 'specific_analysis',
        'analysis_type': 'time_series_analysis',
        'analysis_method': 'company_province',
        'index_id': 'ruanzhu_cnt',
        'index_ids': [],
        'index_type': 'disperse'
    }

    args4 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'behavioural_analysis',
        'analysis_method': 'total_analysis',
        'index_id': 'ruanzhu_cnt',
        'index_ids': [],
        'index_type': 'disperse'
    }

    args5 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'distribution_analysis',
        'analysis_method': 'company_province',
        'index_id': 'ns_status',
        'index_ids': [],
        'index_type': 'disperse'
    }

    args6 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'behavioural_analysis',
        'analysis_method': 'correlation_analysis',
        'index_id': None,
        'index_ids': ['ruanzhu_cnt', 'zhuanli_cnt'],
        'index_type': ''
    }

    args7 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'behavioural_analysis',
        'analysis_method': 'clustering_analysis',
        'index_id': None,
        'index_ids': ['ruanzhu_cnt', 'zhuanli_cnt'],
        'index_type': ''
    }

    args8 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'time_series_analysis',
        'analysis_method': 'correlation',
        'index_id': 'ruanzhu_cnt',
        'index_ids': ['ruanzhu_cnt', 'zhuanli_cnt'],
        'index_type': ''
    }

    args9 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'time_series_analysis',
        'analysis_method': 'history',
        'index_id': 'ruanzhu_cnt',
        'index_ids': ['ruanzhu_cnt', 'zhuanli_cnt'],
        'index_type': ''
    }

    args10 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'time_series_analysis',
        'analysis_method': 'prediction',
        'index_id': 'ruanzhu_cnt',
        'index_ids': [],
        'index_type': ''
    }


if __name__ == '__main__':

    LOCAL_PATH = "/home/bbders/zhaoyunfeng"
    HDFS_PATH = '/user/bbders/zhaoyunfeng/'
    HDFS_IN = '/user/bbders/zhaoyunfeng/guoxing_in/'
    HDFS_OUT = '/user/bbders/zhaoyunfeng/guoxing_out/'
    LOCAL_ES_SOURCE = '{}/es_data/'.format(LOCAL_PATH)

    # 任务获取地址
    INTF_ADDR = 'http://10.28.103.21:8899'

    # 基础指标库
    INDEX_TABLE = 'guoxin.test'
    # 指标映射表
    INDEX_MAPPING_TABLE = 'guoxin.continuity'

    # es连接信息配置
    ES_NODES = '10.28.103.20'
    CLUSTERS_NAME = 'test-opslog'
    ES_PORT = '39200'

    # 接受到的任务参数
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-task_type", help="任务类型")
    parser.add_argument(
        "-task_text", help="任务参数")
    args = parser.parse_args()

    task_type = args.task_type
    task_text = args.task_text

    print(task_text)
    # spark-session
    spark = get_spark_session()

    # 获取指标元数据
    col_mapping = get_col_continuity(INDEX_MAPPING_TABLE)

    # 0、
    try:
        if task_type == 'Synthesis':
            tasks_info = Synthesis.get_synthesis_args_obj(task_text, col_mapping)
        elif task_type == 'Specific':
            tasks_info = Specific.get_specific_args_obj(task_text, col_mapping)
        field_id = tasks_info['field_id']
        cols = tasks_info['cols']
        task_logger.info("任务参数解析成功：{}".format(json.dumps(tasks_info, indent=4)))
    except Exception as e:
        task_logger.error("任务参数解析失败：{}".format(task_text), exc_info=True)


    # 1、从ES下载样本文件并上传HDFS
    get_and_save_es_data(field_id)
    # 2、获得样本spark DATAFRAME
    try:
        sample = Sample(field_id, cols, has_serire=True)
        task_logger.info("获取样本+指标成功：field_id={}".format(field_id))
    except Exception as e:
        task_logger.error("获取样本+指标失败：field_id={}".format(field_id), exc_info=True)

    # 3、执行,回调
    if task_type == 'Synthesis':
        callback = Synthesis.execut_task(tasks_info['tasks'], tasks_info['total_task_id'])
        return_task_result(INTF_ADDR, 'comprehensiveanalysis', callback)
    elif task_type == 'Specific':
        callback = Specific.execut_task(tasks_info['tasks'], tasks_info['total_task_id'])
        return_task_result(INTF_ADDR, 'specificanalysis', callback)

    # 4、结果
    print(callback)




    # args = Test.args10
    # if args['index_id']:
    #     cols = args['index_ids']
    #     cols.append(args['index_id'])
    # else:
    #     cols = args['index_ids']
    # spark = get_spark_session()
    #
    # sample = Sample(field_id, cols, has_serire=True)
    # #sample = SampleTest("/home/bbders/zhaoyunfeng/test.pickle")
    #
    # analysisTask = AnalysisTask(args)
    # result = analysisTask.get_statistical_information()
    # print(result)
