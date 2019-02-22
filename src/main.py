import datetime
import os
import sys
sys.path.append(os.getcwd())
import subprocess

from handler.RequestHandler import *
from utils.logUtil import create_logger


request_logger = create_logger(logger_name='request_logger', file_name='request.log')


def main():
    ret = get_task_info(INTF_ADDR, 'comprehensiveanalysis')
    # 打包spark依赖
    os.system("rm ./pyfiles4spark.zip; zip -r ./pyfiles4spark.zip .")

    execute_result = subprocess.call(
        '''
        PYSPARK_PYTHON=/home/bbders/anaconda3/bin/python spark2-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 5g \
        --driver-cores 4 \
        --queue users.bbders \
        --py-files pyfiles4spark.zip \
        ./spark_app/AnalysisTask.py \
        -task_type '{task_type}' \
        -task_text '{task_text}'
        '''.format(task_type='Synthesis',
                   task_text=ret.text),
        shell=True
    )



if __name__ == '__main__':
    INTF_ADDR = 'http://10.28.103.21:8899'

    main()

