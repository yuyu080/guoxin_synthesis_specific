
import requests
from requests.adapters import HTTPAdapter
import json
import logging

request_logger = logging.getLogger('request_logger')


def get_task_info(addr, task_type):
    url = addr + '/api/nationalcredit/task/compute/{}'.format(task_type)
    headers = {'content-type': 'application/json'}
    try:
        my_request = requests.Session()
        my_request.mount('http://', HTTPAdapter(max_retries=3))
        my_request.mount('https://', HTTPAdapter(max_retries=3))
        ret = my_request.get(url, headers=headers)
        request_logger.info("获取任务："+ret.text)
        # 获取data
        if json.loads(ret.text)['success'] and json.loads(ret.text)['data']:
            return ret
        else:
            return ''
    except:
        request_logger.error("获取任务失败：", exc_info=True)
        return ''


def return_task_result(addr, task_type, callback):
    url = addr + '/api/nationalcredit/task/callback/{}'.format(task_type)
    headers = {'content-type': 'application/json'}
    try:
        my_request = requests.Session()
        my_request.mount('http://', HTTPAdapter(max_retries=3))
        my_request.mount('https://', HTTPAdapter(max_retries=3))
        request_logger.info("任务回调内容：{}".format(callback))
        # 任务回调
        ret = my_request.post(url, json=callback, headers=headers)
        request_logger.info("任务回调结果：" + ret.text)

        if json.loads(ret.text)['success']:
            return ret
        else:
            return ''
    except:
        request_logger.error("任务回调失败", exc_info=True)
        return ''


if __name__ == '__main__':
    INTF_ADDR = 'http://10.28.103.21:8899'
    request_result = get_task_info(INTF_ADDR, 'comprehensiveanalysis')
    model = json.loads(request_result.text)