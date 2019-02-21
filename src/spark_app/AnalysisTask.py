import os
import pandas as pd

from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row
from pyspark.sql import SparkSession

from ..common import *

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
                        each_partition,
                        self.get_pandas_df(
                            field_id, index_cols, each_partition)),
                    partitions))
            self.df = self.dfs[-1][1]

    @staticmethod
    def get_table_partition(table):
        '''获取所有分区'''
        dts = map(
            lambda r: r.partition.split('=')[1],
            spark.sql('show partitions {}'.format(INDEX_TABLE)).collect()
        )
        return list(dts)

    @staticmethod
    def check_col_exist(index_cols, table_cols):
        '''判断字段是否存在'''
        s1 = set(index_cols)
        s2 = set(table_cols)
        return list(s2.intersection(s1))

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
        ).select(
            index_df.city.alias('company_county'),
            index_df.company_industry,
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

        return tid_df.toPandas()

    def get_pandas_dfs(field_id, index_cols, table_dt):
        pass

class SampleTest:

    def __init__(self, field_id):
        self.df = pd.read_pickle(field_id)


class AnalysisTask:
    '''分析任务'''

    def __init__(self, args):
        self.analysis_model = args['analysis_model']
        self.analysis_type = args['analysis_type']
        self.analysis_method = args['analysis_method']
        self.index_id = args['index_id']
        self.index_ids = args['index_ids']
        self.index_type = args['index_type']
        self.df = sample.df
        self.dfs = sample.dfs
        self.local_path = LOCAL_PATH
        self.hdfs_path = HDFS_PATH

    def get_statistical_information(self):
        if self.analysis_model == 'specific_analysis':

            # 行为分析
            if self.analysis_type == 'behavioural_analysis':
                grouped = BehaviouralAnalysis.grouped_analysis(
                    self.df, self.index_id, self.analysis_method)
                if self.index_type == 'disperse':
                    return CalculateMethod.value_counts(grouped)
                elif self.index_type == 'continuous':
                    return CalculateMethod.group_info(grouped)
                else:
                    return {}

            # 时间序列分析
            if self.analysis_type == 'time_series_analysis':
                grouped_series = TimeSeriesAnalysis.grouped_analysis(
                    map(lambda t: t[1], self.dfs),
                    self.index_id, self.analysis_method)
                result = map(
                    CalculateMethod.mean,
                    grouped_series)
                result_series = dict(zip(map(lambda t: t[0], self.dfs), result))
                return CalculateMethod.format_dict_2(result_series)

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
                        self.local_path,
                        self.hdfs_path,
                        self.index_ids
                    )
                    return path

                # 行为聚类分析
                if self.analysis_method == 'clustering_analysis':
                    path = BehaviouralAnalysis.correlation_analysis(
                        self.df,
                        self.local_path,
                        self.hdfs_path,
                        self.index_ids
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
                if self.analysis_method == 'correlation':
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
        .appName("zhaoyunfeng") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


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
        'index_type': []
    }

    args7 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'behavioural_analysis',
        'analysis_method': 'clustering_analysis',
        'index_id': None,
        'index_ids': ['ruanzhu_cnt', 'zhuanli_cnt'],
        'index_type': []
    }

    args8 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'time_series_analysis',
        'analysis_method': 'correlation',
        'index_id': 'ruanzhu_cnt',
        'index_ids': ['ruanzhu_cnt', 'zhuanli_cnt'],
        'index_type': []
    }

    args9 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'time_series_analysis',
        'analysis_method': 'history',
        'index_id': 'ruanzhu_cnt',
        'index_ids': ['ruanzhu_cnt', 'zhuanli_cnt'],
        'index_type': []
    }

    args10 = {
        'analysis_model': 'synthesis_analysis',
        'analysis_type': 'time_series_analysis',
        'analysis_method': 'prediction',
        'index_id': 'ruanzhu_cnt',
        'index_ids': None,
        'index_type': []
    }


if __name__ == '__main__':
    LOCAL_PATH = "/home/bbders/zhaoyunfeng"
    HDFS_PATH = '/user/bbders/zhaoyunfeng/'
    HDFS_OUT = '/user/bbders/zhaoyunfeng/guoxing_out/'
    INDEX_TABLE = 'guoxin.test'
    args = Test.args8
    if args['index_id']:
        cols = args['index_ids']
        cols.append(args['index_id'])
    else:
        cols = args['index_ids']
    spark = get_spark_session()

    sample = Sample('5ee8105508474fd3b699407814804edb', cols, has_serire=True)
    #sample = SampleTest("/home/bbders/zhaoyunfeng/test.pickle")

    analysisTask = AnalysisTask(args)
    result = analysisTask.get_statistical_information()
    print(result)