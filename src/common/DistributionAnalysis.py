
class DistributionAnalysis:

    @staticmethod
    def total_analysis(df, analysis_col):
        return df[analysis_col]

    @staticmethod
    def grouped_analysis(df, analysis_col, group_col):
        '''地区、成立年限、公司类型'''
        return df[analysis_col].groupby(df[group_col])