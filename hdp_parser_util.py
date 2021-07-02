###########################################################################
#Global Name: hdp_parser_util.py                                          #
#Description: utility script to use in spark_hdp_generic_parser.py        #
#Author:      Biswadeep Upadhyay                                          #
###########################################################################

from datetime import datetime
import subprocess
import os
import time
import glob
import json
from subprocess import Popen, PIPE
import sys
#sys.path.append("/hadoopData/global_shared/hifi/python")
#from utilities.exeCmd import *
#from utilities.logger_module import *
from py4j.protocol import Py4JJavaError
from sys import argv
import re


class parseUtil(object):
    def __init__(self,job_name,inc):
        self.job_name = job_name
        self.inc = inc

    def printDetail(self):
        return f'Job Name: {self.job_name} , Parsing Indicator: {self.inc}'

    def get_query(self):
        func_to_call = self.inc
        call_func = getattr(self,func_to_call,None)
        if call_func is None:
            return None
        else:
            return call_func()

    def omni_product(self):
        cust_query = '''put your sql here'''
        return cust_query

    def product_content(self):
        cust_query = f'''select
        PRODUCT_ID,
        replace(PRODUCT_NAME, '|','') AS PRODUCT_NAME,
        LAST_BREADCRUMB,
        CATALOG_NAME,
        LINK_URL,
        ITEM_NUMBER,
        VENDOR_NUMBER,
        MODEL_ID,
        BARCODE,
        replace(PRODUCT_DESCRIPTION, '|', '') AS PRODUCT_DESCRIPTION,
        '' AS PRODUCT_PRICE,
        '' AS WAS_PRICE,
        PRODUCT_IMAGE,
        BRAND_NAME,
        SHIP_WEIGHT,
        COLOR,
        HEIGHT,
        LOWES_MERCHANT_DIVISION,
        LOWES_SUB_DIVISION,
        PROMOTION_MESSAGE,
        PRODUCT_GROUP,
        CASE WHEN PRODUCT_STATUS = 'ANY' OR PRODUCT_STATUS = 'INTERNETONLY' OR PRODUCT_STATUS = 'ALL'
            THEN 'Yes' ELSE 'No' END AS PARCEL_AVAILABILITY,
        '' AS SHIPPING_COST,
        ITEM_TYPE,
        PRODUCT_STATUS,
        BUYABLE,
        '' AS FREE_TRUCK_DELIVERY,
        BULLETS,
        '' AS PRICE_DISTRIBUTION,
        VENDOR_DIRECT,
        SHGC,
        UVALUE,
        MOBILE_URL,
        '' AS STOCK_QTY,
        '' AS LOCAL_AVAILIABILITY,
        '' AS THRU_DATE,
        '' AS PARCEL_SHIPPINGCOST,
        '' AS TRUCK_DELIVERYCOST,
        PRODUCT_DISCLAIMER,
        PRODUCT_GROUP_NUMBER AS PRODUCT_GRP_NUMBER,
        ASSORTMENT_NUMBER,
        ADDITIONAL_IMAGE_1_URL AS ADDITIONAL_IMG_1_URL,
        ADDITIONAL_IMAGE_2_URL AS ADDITIONAL_IMG_2_URL,
        ADDITIONAL_IMAGE_3_URL AS ADDITIONAL_IMG_3_URL,
        ADDITIONAL_IMAGE_4_URL AS ADDITIONAL_IMG_4_URL,
        ADDITIONAL_IMAGE_5_URL AS ADDITIONAL_IMG_5_URL
        FROM {self.inc}'''

        return cust_query


        

'''
util_1 = parseUtil('JMH_ETLXXXXD_Active_Product_Parser_HDFS_HDFS','product_contentza')

print(util_1.printDetail())

print(util_1.get_query())
'''






