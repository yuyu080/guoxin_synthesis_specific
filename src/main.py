import datetime
import os
import sys
sys.path.append(os.getcwd())
import subprocess
from random import choice
from time import sleep
from concurrent.futures import  ProcessPoolExecutor
import copy

from handler.RequestHandler import *
from utils.logUtil import create_logger


request_logger = create_logger(logger_name='request_logger', file_name='request.log')
# 打包spark依赖
os.system("rm ./pyfiles4spark.zip; zip -r ./pyfiles4spark.zip .")


def submit_task(api_task_type, spark_task_type):
    # 从接口获得任务
    ret = get_task_info(INTF_ADDR, api_task_type)
    if ret:
        # 启动spark-app
        os.system(
            '''
            PYSPARK_PYTHON=/home/bbders/anaconda3/bin/python spark2-submit \
            --master yarn \
            --deploy-mode client \
            --driver-memory 5g \
            --driver-cores 4 \
            --queue users.bbders \
            --name {task_type} \
            --py-files pyfiles4spark.zip \
            ./spark_app/AnalysisTask.py \
            -task_type '{task_type}' \
            -task_text '{task_text}'
            '''.format(task_type=spark_task_type,
                       task_text=ret.text)
        )


def main():
    executor = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    while True:

        api_task_type, spark_task_type = choice(TASK_TYPE)
        executor.submit(submit_task, api_task_type, spark_task_type)

        sleep(5)



if __name__ == '__main__':
    INTF_ADDR = 'http://10.28.103.21:8899'
    # （api的模块URI，对应的spark分析模块）
    TASK_TYPE= [('specificanalysis', 'Specific'),
                ('comprehensiveanalysis', 'Synthesis')]
    # 同时执行的计算
    MAX_WORKERS = 3

    main()

