###########################################################################
#Global Name: wrapper_HDP_Generic_Parser.py                               #
#Description: wrapper script to create spark-submit for the spark script  #
#             spark_HDP_Generic_Parser.py                                 #
#             as Generic Script                                           #
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
sys.path.append("/hadoopData/global_shared/hifi/python")
from utilities.exeCmd import *
from utilities.logger_module import *
from py4j.protocol import Py4JJavaError
from sys import argv
import re
from hdp_parser_util import parseUtil





def spark_submit():
    """This function creates the spark submit command and submits the applcaition to the cluster. 
    :param config - The input dicotoanry returned from doing json.load of theinput configuration files
    :return: True or False
    """
    hdp_cmd = 'spark-submit  --master yarn --deploy-mode cluster --conf "spark.yarn.maxAppAttempts=1"  --conf "spark.pyspark.python=/usr/bin/python3" --conf "spark.pyspark.driver=/usr/bin/python3" '
    hdp_cmd = hdp_cmd + " --files " + v_config_file + "#" + os.path.basename(v_config_file)
    hdp_cmd = hdp_cmd + " --py-files /hadoopData/global_shared/global_feed/python/hdp_parser_util.py"
    hdp_cmd = hdp_cmd + " --executor-cores " + v_executorCores + " --executor-memory " + v_executorMemory  + ' --driver-memory ' + v_driverMemory  + ' --conf "spark.shuffle.service.enabled=true" --conf "spark.dynamicAllocation.enabled=true" --conf "spark.dynamicAllocation.maxExecutors=' + v_numExecutors  +  '" --conf "spark.dynamicAllocation.minExecutors=1" '
    hdp_cmd = hdp_cmd + "/hadoopData/global_shared/global_feed/python/global_inventory_products_parser.py "  + os.path.basename(v_config_file) + " " + v_job_name + " " + v_parse_idc
    logging.info("Processing of the spark-submit command - " + hdp_cmd)
    logging.debug("Processing of the spark-submit command - " + hdp_cmd)
    command_exec = executeCmd(hdp_cmd)
    if command_exec.execute():
        if command_exec.getcommandoutput():
            log_out = command_exec.getcommandoutput()
            logging.info(f"yarn log out from the successful completion of spark submit: {log_out}")
        else:
            log_out = command_exec.getcommanderror()
            logging.info(f"yarn error out from the successful completion of spark submit: {log_out}")
        extractYarnLog(log_out)
        return True
    else:
        if command_exec.getcommandoutput():
            log_out = command_exec.getcommandoutput()
            logging.info(f"yarn log out from the failed spark submit: {log_out}")
        else:
            log_out = command_exec.getcommanderror()
            logging.info(f"error out from the failed spark submit: {log_out}")
        extractYarnLog(log_out)
        return False


def purgeLogFile(dir, pattern):
    """Purge Old Files Created by this process.

    :param: 
        dir - Provide the Directoy name where the logfile is places
        pattern - Provide the pattern of the file name to be removed.
    :return: None - removed the files
    """
    logging.info(f"looking for log files with the pattern '{pattern}' older than 7 days in the dir '{dir}' for purging.")
    cnt = 0
    for f in os.listdir(dir):
        f = os.path.join(dir, f)
        if re.search(pattern, f) and os.stat(f).st_mtime < now - 7 * 86400:
            cnt += 1
            print("Housekeeping of old log files - Removing " + f )
            logging.info(f"Housekeeping of old log files - Removing : {f}")
            os.remove(f)
    if cnt == 0:
        logging.info('No log files found for purging. Ignoring purging.')



def initializeLog(logPath,etl_jobname):
    """This fucntion executes the intiates the log for the python process.
    It uses the processname and python filename to look at the logs
    from utilities import logger_module this logges module in the generic scripts is utilized for the processing.
    :param: logPath - The logpath is the log folder where logs to be craeted
        provide the processs name and logPath folder. T
    :return: True or False
    """
    if not logPath:
        print("ERROR: initializeLog Module Error. No log Path defined")
        sys.exit(1)

    processname = "spark_"  + etl_jobname + '_' + v_parse_idc + '_' + startTime
    #pattern_to_remove = os.path.basename(__file__) + "_" +  etl_jobname
    pattern_to_remove = etl_jobname
    if processname is None:
        return False
    print("INFO: Log file path is - " + logPath + "/" + processname + ".log")
    
    intializeLogger(logPath, logging.INFO, processname)
    os.system(f"echo `date '+%Y-%m-%d %H:%M:%S'`::[INFO]:The spark Log initialized at: {logPath}/{processname}.log for this session. >> {v_unix_log}")
    logging.info(f"The spark Log initialized at: {logPath}/{processname}.log for this session.")
    purgeLogFile(logPath,pattern_to_remove)
    os.chmod(logPath + "/" + processname + ".log", 0o666)

def extractYarnLog(cmdout):
    try:
        yarn_application_ids = re.findall(r'application_\d{13}_\d{4}', cmdout)
        if len(yarn_application_ids):
            yarn_application_id = yarn_application_ids[0]
            logging.info(f'The yarn application id for this session: {yarn_application_id}')
            yarn_command = 'yarn logs -applicationId ' + yarn_application_id + " -log_files stdout"
            logging.info('Yarn log extraction command - ' + yarn_command)
            command_exec = executeCmd(yarn_command)
            if command_exec.execute():
                logging.info('Yarn log extraction completed for - ' + yarn_application_id )
                logging.info('=========================================adding the yarn log here.=====================================')
                if command_exec.getcommandoutput():
                    log_out = command_exec.getcommandoutput()
                    logging.info(f"yarn log out: {log_out}")
                else:
                    log_out = command_exec.getcommanderror()
                    logging.info(f"yarn log out: {log_out}")
            else:
                logging.error('Yarn log extraction failed for  - ' + yarn_application_id )
                return False
        else:
            logging.warning("Unable to locate Yarn log extraction ")
            return False
    except Exception as e:
        logging.error('Yarn log extraction failed for this session.')
        logging.error(str(e))
        return True


from datetime import datetime
import ast
startTime = datetime.now().strftime("%Y%m%d_%H%M%S")
now = time.time()

v_job_name = argv[1]
v_parse_idc = argv[2]
v_unix_log = argv[3]
v_config_file = argv[4]

old_print = print
def timestamped_print(*args, **kwargs):
    logTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    printheader = logTime + " " + "HDP" + " " + "Generic Parser" + " - "
    old_print(printheader, *args, **kwargs)
print = timestamped_print

print('The HDP Generic Feed Parser has started.')


try:
    with open(v_config_file,'r') as attr_conf:
        attr_map = json.load(attr_conf)
        print('The config file read is successful.')
except Exception as e:
    tb = sys.exc_info()[2]
    lineno = tb.tb_lineno
    #LOGGER.error(f'Code Error at LINE: {lineno}.')
    #LOGGER.error(f'The process failed due to invalid Attribute list config Json: {e}')
    print(f'the parser failed due to invalid Attribute list config Json file: {v_config_file}')
    os.system(f"echo `date '+%Y-%m-%d %H:%M:%S'`::[ERROR]:The process failed due to invalid Attribute list config Json file {v_config_file} . >> {v_unix_log}")
    exit(1)  


if str(v_parse_idc).strip() != '':
    try:
        attr_dict = [x for x in attr_map if str(x['indicator']).strip() == v_parse_idc.strip()][0]
        print(f'the attribute mapping for the parsing idicator : {v_parse_idc} is : {attr_dict}')
    except Exception as e:
        print(f'the key "indicator" was not found in input config file for given parsing indicator {v_parse_idc}. Exiting with error code 1.')
        os.system(f"echo `date '+%Y-%m-%d %H:%M:%S'`::[ERROR]:the key indicator was not found for given parsing indicator {v_parse_idc} >> {v_unix_log}")
        exit(1)
else:
    print('The parser failed because no parsing indicator was provided in prm file. Please Provide the parsing indicator in the input parameter -i.')
    os.system(f"echo `date '+%Y-%m-%d %H:%M:%S'`::[ERROR]:The script failed to parse. Please Provide the parsing indicator in the input parameter -i. >> {v_unix_log}")
    exit(1)

if str(attr_dict.get('logDir','')).strip() != '':
    try:
        log_dir = '/' + str(attr_dict['logDir']).strip().strip('/') 
        print(f"user given log dir : {log_dir}")
    except Exception as e:
        tb = sys.exc_info()[2]
        lineno = tb.tb_lineno
        os.system(f"echo `date '+%Y-%m-%d %H:%M:%S'`::[ERROR]:error at {os.path.abspath(__file__)} , line number: {lineno}  >> {v_unix_log}")
        os.system(f"echo `date '+%Y-%m-%d %H:%M:%S'`::[ERROR]:Unable to parse log directory information from parm config file. >> {v_unix_log}")
        exit(1)
else:
    try:
        print('No LogDir provided in the config file.')
        log_dir = '/' + '/'.join(v_config_file.split('/')[1:4]) + '/logs'
        print(f'the directory used for the logs : {log_dir}')
    except Exception as e:
        tb = sys.exc_info()[2]
        lineno = tb.tb_lineno
        print(f"error at {os.path.abspath(__file__)} , line number: {lineno}")


initializeLog(log_dir,v_job_name)
logging.info(f"HDP Generic Feed Parser for the parse indicator {v_parse_idc}")

logging.info(f"Starting validation for the input config file: {v_config_file}")

if str(attr_dict.get('srcDir','')).strip() != '':
    logging.info(f"Source Feed: {attr_dict['srcDir']}")
else:
    logging.error('No Source Feed Provided. Please provide SrcDir in config file.exiting with exit code 1.')
    exit(1)

if str(attr_dict.get('tgtDir','')).strip() != '':
    logging.info(f"Target Feed Directory: {attr_dict['tgtDir']}")
else:
    logging.error('No Target Feed Directory Provided. Please provide tgtDir in config file.exiting with exit code 1.')
    exit(1)


if str(attr_dict.get('ipFeedType','')).strip() != '':
    logging.info(f"Input Feed Type: {attr_dict['ipFeedType']}")
    if str(attr_dict.get('ipFeedType','')).strip().lower() == 'flat':
        try:
            ipFlatDel = attr_dict['inputDelimiter']
            logging.info(f"for input feed type {attr_dict['ipFeedType']} given input delimiter: {ipFlatDel}")
        except Exception as e:
            tb = sys.exc_info()[2]
            lineno = tb.tb_lineno
            logging.error(f"error at {os.path.abspath(__file__)} , line number: {lineno}")
            logging.error(f"parsing source flat feed delimition failed with the error: {e}")
            exit(1)
else:
    logging.error('No Input Feed Type Provided. Accepted Feed Types are JSON and Flat. Please provide ipFeedType in config file.exiting with exit code 1.')
    exit(1)

if str(attr_dict.get('executorMemory','')).strip() != '':
    logging.info(f"Input executorMemory: {attr_dict['executorMemory']}")
    v_executorMemory = str(attr_dict['executorMemory']).strip()
else:
    logging.info('No executorMemory was provided. By Default --executor-memory 1G will be selected for spark-submit.')
    v_executorMemory = '1G'
    

if str(attr_dict.get('numExecutors','')).strip() != '':
    logging.info(f"Input numExecutors: {attr_dict['numExecutors']}")
    v_numExecutors = str(attr_dict['numExecutors']).strip()
else:
    logging.info('No numExecutors was provided. By Default --num-executors 1  will be selected for spark-submit.')
    v_numExecutors = '1'

if str(attr_dict.get('executorCores','')).strip() != '':
    logging.info(f"Input executorCores: {attr_dict['executorCores']}")
    v_executorCores = str(attr_dict['executorCores']).strip()
else:
    logging.info('No executorCores was provided. By Default --executor-cores 1  will be selected for spark-submit.')
    v_executorCores = '1'

if str(attr_dict.get('driverMemory','')).strip() != '':
    logging.info(f"Input driverMemory: {attr_dict['driverMemory']}")
    v_driverMemory = str(attr_dict['driverMemory']).strip()
else:
    logging.info('No driverMemory was provided. By Default --driver-memory 1G  will be selected for spark-submit.')
    v_driverMemory = '1G'


logging.info('Calling the spark submit for this session.')

if spark_submit():
    logging.info('The processing of the spark-submit is complete for this session.')
    logging.info('Final Status : SUCCESS')
    exit(0)
else:
    logging.error('The processing of the spark-submit failed for this session.Check the spark log file for details.')
    logging.error('Final Status : FAILED')
    exit(1)

'''
command_exec = executeCmd('python3 /hadoopData/global_shared/global_feed/python/py_hierarchy_test.py')

if command_exec.execute():
    logging.info('back to wrapper after success.')
    logging.info('Chandler: Success.')
else:
    logging.info('the nested script failed.')
    logging.info('Rachel: Fail.')

processname = os.path.basename(__file__)

print(f'the processname is : {processname}')

print(os.path.abspath(__file__))
'''

