
class DistributionAnalysis:

    @staticmethod
    def total_analysis(df, analysis_col):
        return df[analysis_col]

    @staticmethod
    def grouped_analysis(df, analysis_col, group_col, query=''):
        '''地区、成立年限、公司类型'''
        if query:
            return df.query(query)[analysis_col].groupby(df[group_col])
        else:
            return df[analysis_col].groupby(df[group_col])