import datetime
import os
import subprocess

from .handler.RequestHandler import *

def main():
    ret = get_task_info(INTF_ADDR, 'comprehensiveanalysis')
    os.system("rm ./pyfiles4spark.zip; zip -r ./pyfiles4spark.zip .")

    execute_result = subprocess.call(
        '''
        PYSPARK_PYTHON=/home/bbders/anaconda3/bin/python spark2-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 5g \
        --driver-cores 4 \
        --queue users.bbders \
        --py-files pyfiles4spark.zip\
        ./spark_app/AnalysisTask.py {} {}
        '''.format(task_type='Synthesis',
                   task_text=ret.text,
        shell=True
    ))



if __name__ == '__main__':
    INTF_ADDR = 'http://10.28.103.21:8899'

    main()

