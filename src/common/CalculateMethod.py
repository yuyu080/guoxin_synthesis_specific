
import collections


class CalculateMethod:

    @staticmethod
    def value_counts(feature):
        '''分布/占比统计'''
        range_count = feature.value_counts()
        range_rate = range_count / feature.count()
        return CalculateMethod.format_dict(range_count.to_dict())

    @staticmethod
    def describe(feature):
        '''分布描述'''
        return feature.describe().fillna(0).to_dict()

    @staticmethod
    def describe_info(feature):
        '''原始分布描述'''
        return {
            'y': list(filter(None, feature.fillna(0))),
            's': feature.describe().fillna(0).to_dict()
        }

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
        return {
            'x': [each_group for each_group, v in grouped],
            'y': [list((filter(None, v.fillna(0).values))) for each_group, v in grouped],
            's': [v.describe().fillna(0).to_dict() for each_group, v in grouped]
        }

    @staticmethod
    def group_describe(grouped):
        '''分区分布描述'''
        return {each_group: v.describe().fillna(0).to_dict() for each_group, v in grouped}

    @staticmethod
    def format_dict(result):
        '''格式化value的结构,柱状图'''
        try:
            format_result = collections.defaultdict(dict)
            prd_x = []
            raw_y = []

            for (k, r), v in result.items():
                format_result[k][r] = v

            for (k, v) in format_result.items():
                prd_x.append(k)
                raw_y.append(v)

            keys = raw_y[0].keys()
            prd_y = [
                {
                    'name': key,
                    'data': [each_item.get(key, 0) for each_item in raw_y]
                }
                for key in keys
            ]

            return {'x': prd_x, 'y': prd_y}
        except:
            return {
                'x': [k for k, v in result.items()],
                'y': [v for k, v in result.items()]
            }

    def format_dict_2(result):
        '''格式化value的结构,时序图'''
        try:
            prd_x = list(result.keys())
            raw_y = list(result.values())

            keys = set(raw_y[0].keys()).union(raw_y[1].keys())
            prd_y = [
                {
                    'name': key,
                    'data': [each_item.get(key, 0) for each_item in raw_y]
                }
                for key in keys
            ]

            return {'x': prd_x, 'y': prd_y}
        except:
            return {
                'x': [k for k, v in result.items()],
                'y': [v for k, v in result.items()]
            }

