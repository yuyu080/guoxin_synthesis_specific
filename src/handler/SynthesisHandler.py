import json


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
        'COMPANY_TYPE': 'company_type'
    }
    analysis_method_reverse = {v: k for k, v in analysis_method.items()}

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
                'task_id': each_task['remoteTaskId']
            }
            cols.extend(each_task['indexId'])
            tasks.append(arg)

        return {
            'tasks': tasks, 'field_id': field_id, 'cols': cols, 'total_task_id': obj['data']['taskId']
        }

    @staticmethod
    def format_synthesis_obj(result, total_task_id):
        '''构造综合分析响应参数'''
        return {
            'taskId': total_task_id,
            'resultPath': '',
            'taskStatus': 3 if 3 in [each_task['task_status'] for each_task in result] else 2,
            'resultInfo': [{
                'remoteTaskId': each_task['task_id'],
                'result': each_task['task_result'],
                'status': each_task['task_status'],
                'image_type': each_task['image_type'],
                'result_type': each_task['result_type']
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
                # 成功
                if result == 'no analysis type':
                    arg['task_status'] = 3
                else:
                    arg['task_status'] = 2

            except Exception as e:
                arg['task_result'] = repr(e).replace("\'", '')
                # 失败
                arg['task_status'] = 3
        return Synthesis.format_synthesis_obj(tasks, total_task_id)