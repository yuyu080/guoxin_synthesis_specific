import uuid
import os
import traceback
import logging

task_logger = logging.getLogger('task_logger')


class BehaviouralAnalysis:

    @staticmethod
    def total_analysis(df, analysis_col):
        '''行为特征'''
        return df[analysis_col]

    @staticmethod
    def grouped_analysis(df, analysis_col, group_col):
        '''地区、成立年限、公司类型'''
        return df[analysis_col].groupby(df[group_col])

    @staticmethod
    def correlation_analysis(df, analysis_cols, local_path, hdfs_path):
        '''行为相关性分析'''
        # 保存数据，后端画图
        file_name = 'correlation_analysis_'+str(uuid.uuid1())+'.pickle'
        try:
            df[analysis_cols].fillna(0).to_pickle(os.path.join(local_path, file_name))
            os.system(
                '''
                hadoop fs -put {} {}
                '''.format(
                    os.path.join(local_path, file_name),
                    hdfs_path
                )
            )
            os.system(
                '''
                rm {}
                '''.format(file_name)
            )
            return os.path.join(hdfs_path, file_name)
        except Exception as e:
            task_logger.error("子任务失败", exc_info=True)
            traceback.print_exc()
            return "ERROR: " + repr(e).replace("\'", '')

    @staticmethod
    def clustering_analysis(df, analysis_cols, local_path, hdfs_path):
        '''聚类分析'''
        # 保存数据，后端画图
        file_name = 'clustering_analysis_' + str(uuid.uuid1()) + '.pickle'
        try:
            df[analysis_cols].fillna(0).to_pickle(os.path.join(local_path, file_name))
            os.system(
                '''
                hadoop fs -put {} {}
                '''.format(
                    os.path.join(local_path, file_name),
                    hdfs_path
                )
            )
            os.system(
                '''
                rm {}
                '''.format(os.path.join(local_path, file_name))
            )
            return os.path.join(hdfs_path, file_name)
        except Exception as e:
            task_logger.error("子任务失败", exc_info=True)
            traceback.print_exc()
            return "ERROR: " + repr(e).replace("\'", '')
