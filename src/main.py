import datetime
import os

from elasticsearch import Elasticsearch
from utils.elasticsearchUtil import ES_Utiles




if __name__ == '__main__':
    nowMonth = datetime.datetime.today().strftime('%Y%m')
    # C6
    # GUOXING_IN = '/user/xiewenbing/guoxing_in/'
    # GUOXING_OUT = '/user/xiewenbing/guoxing_out/%s/' % nowMonth
    # 国信测试环境
    GUOXING_IN = '/user/bbders/zhaoyunfeng/guoxing_in/'
    GUOXING_OUT = '/user/bbders/zhaoyunfeng/guoxing_out/'
    LOCAL_ES_SOURCE = '/home/bbders/zhaoyunfeng/es_data/'
    # 大数据接口域名
    # intf_addr = 'http://internal.bigdata.cegn.cn'
    intf_addr = 'http://10.28.103.21:8899'
    # es连接信息配置
    # ES_NODES = '10.28.100.27'
    # CLUSTERS_NAME = 'test-opslog'
    # ES_PORT = '39204'
    ES_NODES = '10.28.103.20'
    CLUSTERS_NAME = 'test-opslog'
    ES_PORT = '39200'
    es = Elasticsearch([{'host': ES_NODES, 'port': ES_PORT}])
    es_utils = ES_Utiles(es, LOCAL_ES_SOURCE, GUOXING_OUT, GUOXING_IN)

    field_id = "5ee8105508474fd3b699407814804edb"
    es_utils.get_and_save_es_data('common_company_field', ['bbd_qyxx_id', 'company_name'],
                                  'field_name_list_' + field_id,
                                  'bbd_qyxx_id', 'fieldId', field_id)