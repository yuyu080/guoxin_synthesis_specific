
import collections


class CalculateMethod:

    @staticmethod
    def value_counts(feature):
        '''分布/占比统计'''
        range_count = feature.value_counts()
        range_rate =  range_count / feature.count()
        return {
            'range_count': range_count.to_dict(),
            'range_rate': range_rate.to_dict()
        }

    @staticmethod
    def describe(feature):
        '''分布描述'''
        return feature.describe().fillna(0).to_dict()

    @staticmethod
    def mean(feature):
        '''均值'''
        try:
            return feature.mean().fillna(0).to_dict()
        except:
            return feature.mean()


    @staticmethod
    def group_info(grouped):
        '''原始分布'''
        return {each_group: v.values for each_group, v in grouped}

    @staticmethod
    def group_describe(grouped):
        '''分区分布描述'''
        return {each_group: v.describe().fillna(0).to_dict() for each_group, v in grouped}

    @staticmethod
    def format_dict(result):
        '''格式化value的结构'''
        try:
            format_result = collections.defaultdict(dict)
            for (k, r) ,v in result.items():
                result[k][r] = v
            return format_result
        except:
            return result

