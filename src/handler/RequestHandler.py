
import requests
from requests.adapters import HTTPAdapter
import json


def get_task_info(addr, task_type):
    url = addr + '/api/nationalcredit/task/compute/{}'.format(task_type)
    headers = {'content-type': 'application/json'}
    try:
        my_request = requests.Session()
        my_request.mount('http://', HTTPAdapter(max_retries=3))
        my_request.mount('https://', HTTPAdapter(max_retries=3))
        ret = my_request.get(url, headers=headers)
        # 获取data
        if json.loads(ret.text)['success'] and json.loads(ret.text)['data']:
            return ret
        else:
            return ''
    except:
        return ''


def return_task_result(addr, task_type, callback):
    url = addr + '/api/nationalcredit/callback/compute/{}'.format(task_type)
    headers = {'content-type': 'application/json'}
    try:
        my_request = requests.Session()
        my_request.mount('http://', HTTPAdapter(max_retries=3))
        my_request.mount('https://', HTTPAdapter(max_retries=3))
        ret = my_request.post(url, json=callback, headers=headers)
        # 任务回调
        if json.loads(ret.text)['success']:
            return ret
        else:
            return ''
    except:
        return ''


if __name__ == '__main__':
    INTF_ADDR = 'http://10.28.103.21:8899'
    request_result = get_task_info(INTF_ADDR, 'comprehensiveanalysis')
    model = json.loads(request_result.text)