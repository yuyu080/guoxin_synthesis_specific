import uuid
import os


class TimeSeriesAnalysis:

    @staticmethod
    def grouped_analysis(dfs, analysis_col, group_col):
        '''地区、成立年限、公司类型'''
        return map(
            lambda df: df[analysis_col].groupby(df[group_col]),
            dfs
        )

    @staticmethod
    def total_analysis(dfs, analysis_col):
        return map(
            lambda df: df[analysis_col],
            dfs
        )

    @staticmethod
    def prediction_analysis(now_series):
        '''趋势预测分析'''
        d_list = [(k, v) for k, v in now_series.items()]
        predic_series = []
        for i, (k, v) in enumerate(d_list):
            if i >= 3:
                predic_series.append(
                    (k, 0.5 * d_list[i - 1][1] + 0.3 * d_list[i - 2][1] + 0.2 * d_list[i - 3][1])
                )
        predic_series.append(
            ("get_time_name", 0.5 * d_list[-1][1] + 0.3 * d_list[-2][1] + 0.2 * d_list[-3][1])
        )
        return {
            'predic_series': dict(predic_series),
            'now_series': now_series
        }

    @staticmethod
    def correlation_analysis(df, local_path, hdfs_path):
        '''关联分析'''
        # 保存数据，后端画图
        file_name = str(uuid.uuid1()) + '.pickle'
        try:
            df.fillna(0).to_pickle(file_name)
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
        except:
            return os.path.join(hdfs_path, file_name)