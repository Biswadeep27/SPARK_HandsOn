###########################################################################
#Global Name: global_inventory_products_parser.py                         #
#Description: Generic Script to Parse the Global Products/Inventory JSON  #
#             Feeds extracted from GCP and staging as flat feeds over hdfs#
#             as part of GLOBAL FEEDS                                     #
#Author:      Biswadeep Upadhyay                                          #
###########################################################################

###########################################################################
# import Spark context, Spark session and initiate logger for the script  #
###########################################################################

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
import sys
import json

v_app_name=sys.argv[3]

conf = SparkConf().setMaster("yarn").setAppName(v_app_name).set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

sc.setLogLevel('INFO')
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("==============================pyspark script logger initialized================================")


spark=SparkSession.builder.appName(v_app_name).config("spark.debug.maxToStringFields", "10000").getOrCreate()

#############################################################################################
# Input parameters to the script                                                            #
#############################################################################################
v_src_hdfs = sys.argv[1]
v_tgt_hdfs = sys.argv[2]
v_num_part = int(sys.argv[4])
v_parse_idc = sys.argv[5]
v_attr_lst = sys.argv[6]

print(f'Parameters: Source Feed: {v_src_hdfs}, Traget Feed: {v_tgt_hdfs}.')
LOGGER.info(f'Parameters: Source Feed: {v_src_hdfs}, Traget Feed: {v_tgt_hdfs}.')

print(f'Parameters: Parse Indicator: {v_parse_idc}.\nParameters: Attribute list config Json: {v_attr_lst}.')
LOGGER.info(f'Parameters: Parse Indicator: {v_parse_idc}.\nParameters: Attribute list config Json: {v_attr_lst}.')

#############################################################################################
# Function to parse the user input config file                                              #
#############################################################################################

def parseInputConfig(map_detail):
    map_list = []
    ren_col_list = []
    attri_list = []
    #i=0
    for key,value in map_detail.items():
        map_list.append((key,value))
        ren_col_list.append(value)
        attri_list.append(key)
        #i+=1

    enum_map_list = list(enumerate(map_list))

    print('the enum map list')
    print(enum_map_list)
    print('just like a waving flag')
    #print(ren_col_list)
    #print('old me')
    #print(attri_list)

    #to check if there is any nested hierarchy to parse
    returnCnt = 0
    for _iter in attri_list:
        if '.' in _iter:
            returnCnt+=1	

    ##parsing begins from here
    seen = set()
    seen_add = seen.add

    df_parser_attri_list = []



    parser_attri_dict = {}
    columns = []
    ren_col = {}

    for i in range(len(enum_map_list)):
        first_attr = enum_map_list[i][1][0].split('.')[0]
        if '[' in first_attr:
            clean_attr = first_attr[:first_attr.index('[')]
            #parser_attri_dict['explode_attr'] = clean_attr
            columns.append(clean_attr)
            ren_col[clean_attr] = clean_attr
        else:
            columns.append(first_attr)
            if '.' in enum_map_list[i][1][0]:
                ren_col[first_attr] = first_attr
            else:
                ren_col[first_attr] = enum_map_list[i][1][1]



    uniq_columns = [x for x in columns if not (x in seen or seen_add(x))]

    #parser_attri_dict['index'] = enum_map_list[i][0]

    parser_attri_dict['columns'] = uniq_columns
    parser_attri_dict['rename'] = ren_col
    parser_attri_dict['explode_attr'] = ''

    df_parser_attri_list.append(parser_attri_dict)


    if returnCnt == 0:
        print(df_parser_attri_list)
        return df_parser_attri_list
    else:
        print('picture abhi baki hai mere dost!')
        print(df_parser_attri_list)

        print("=================================================================================================")
        #escape condition for while true loop
        #break_dict = {attri_val.split('.')[0].strip('[]') : (0,0) for attri_val in attri_list if '.' in attri_val} 
        break_dict = {attri_val.split('.')[0] : (0,0) for attri_val in attri_list if '.' in attri_val}

        for _iter in enum_map_list:
            if '.' in _iter[1][0]:
                dot_count = _iter[1][0].count('.')
                #key_val = _iter[1][0].split('.')[0].strip('[]')
                key_val = _iter[1][0].split('.')[0]

                if dot_count > break_dict[key_val][1]:
                    break_dict[key_val] = (_iter[0],dot_count)


        print(break_dict)
        print(list(break_dict.values()))
        print(list(break_dict.keys()))
        break_string = [x[1][1] for x in enum_map_list if x[0] == max(break_dict.values())[0]][0]
        print(break_string)
        nested_attri_keys = list(break_dict.keys())
        print("=================================================================================================")
        iter_columns = uniq_columns[:]
        ########
        test = []
        test.append(iter_columns)
        ########
        iter_dict = {}
        iter_rename_dict = {}
        for attri in nested_attri_keys:
            ##
            nested_attri_list = [[ 'c' + str(x[0]) + '*' + item for item in x[1][0].split('.')] for x in enum_map_list if x[1][0].split('.')[0].strip('[]') == attri.strip('[]') ]
            print("nested_attri_list: ",nested_attri_list)
            #max_length = max([len(i) for i in nested_attri_list])
            max_length = break_dict[attri][1]
            #print(max_length)
            attri_stack = [attri]
            #print("initial attri_stack: ", attri_stack)
            #while any(attri_stack[-1] in sublist for sublist in nested_attri_list) or any(attri_stack[-1] + '[]' in sublist for sublist in nested_attri_list):
            
            while len(attri_stack)>0:
                iter_dict = {}
                iter_rename_dict = {}
                attri_element = attri_stack.pop()
                if '[]' in attri_element:
                    iter_dict['explode_attr'] = attri_element.strip('[]')
                else:
                    iter_dict['explode_attr'] = ''
                #print("attri_element: ",attri_element)
                attri_indices = [(ix,iy) for ix, row in enumerate(nested_attri_list) for iy, i in enumerate(row) if i.split('*')[1] == attri_element or i == attri_element + '[]']
                #print("attri_indices: ", attri_indices) 
                if nested_attri_list[attri_indices[0][0]][-1].split('*')[1].strip('[]') == attri_element.strip('[]'):
                    #print("the continue statement: ", attri_element.strip('[]'))
                    continue
                #print("did not continue")
                next_attributes = [nested_attri_list[i][j+1].split('*')[1] for i,j in attri_indices]
                index_last_attribute_chk = [nested_attri_list[i][j+1] for i,j in attri_indices]
                print("index_last_attribute_chk: ",index_last_attribute_chk)
                for cols in index_last_attribute_chk:
                    col_index,col_name = cols.split('*')
                    col_index = int(col_index.strip('c'))
                    if col_name.strip('[]') == [x[1][0].split('.')[-1].strip('[]') for x in enum_map_list if x[0] == col_index][0]:
                        iter_rename_dict[col_name.strip('[]')] = [x[1][1] for x in enum_map_list if x[0] == col_index][0]

                #uniq_next_attributes = [x.strip('[]') for x in next_attributes if not (x in seen or seen_add(x))]
                uniq_next_attributes = [i for n, i in enumerate(next_attributes) if i not in next_attributes[:n]]
                #print("uniq_next_attributes: ",uniq_next_attributes)
                attri_stack += uniq_next_attributes
                #print("in this stage:")
                #print("attri_stack: ",attri_stack)
                #####################
                df_cols = [attri_element.strip('[]') + '.' + i.strip('[]') for i in uniq_next_attributes]
                print("the current col for df: ",df_cols)
                test.append(df_cols)
                #####################
                prev_columns = df_parser_attri_list[-1]['columns']
                print("prev_columns: ",prev_columns)
                prev_rename = df_parser_attri_list[-1]['rename']

                for p_col in prev_columns:
                    if p_col.split('.')[-1] in [c_col.split('.')[-0] for c_col in df_cols]:
                        continue
                    elif p_col.split('.')[-1] in prev_rename.keys():
                        df_cols.append(prev_rename[p_col.split('.')[-1]])
                    else:
                        df_cols.append(p_col.split('.')[-1])


                iter_dict['columns'] = df_cols
                iter_dict['rename'] = iter_rename_dict
                df_parser_attri_list.append(iter_dict)
                print("current loop ends here")
                print("==========================================================")
                
        #print(test)
        df_parser_attri_list.append({"columns": ren_col_list , "rename" : '' , "explode_attr": ''})
        #print(df_parser_attri_list)
        return df_parser_attri_list

#############################################################################################
# Function to apply user defined filter on Input Feed                                       #
#############################################################################################

def applyFilter(filter_dict,parse_idc,sparkDf):
        if isinstance(filter_dict, dict):
            try:
                sparkDf.createOrReplaceTempView(parse_idc)
                i = 0
                custom_filter = ''
                for key,value in filter_dict.items():
                    if i!=0:
                        custom_filter +=' AND'
                    if isinstance(value, int):
                        custom_filter += " "  + key + " = " + str(value).strip()
                    if isinstance(value, str):
                        if ';' in value:
                            date_val,date_format = value.split(';')
                            custom_filter += " "  + key + " = to_date('" + str(date_val).strip() + "','" + str(date_format).strip() + "')"
                        else:
                            custom_filter += " "  + key + " = '" + str(value).strip() + "'"
                    i+=1
                custom_query = f"SELECT * FROM {parse_idc} WHERE {custom_filter}"

                print(custom_query)
                return_df = spark.sql(custom_query)
                return return_df


            except Exception as e:
                tb = sys.exc_info()[2]
                lineno = tb.tb_lineno
                LOGGER.error(f'Code Error at LINE: {lineno}.')
                LOGGER.error(f'The application of Filter on spark DataFrame Faile with the error : {e}')
                exit(1)  
        else:
            print(f"input filter format is wrong. please provide in dict format")
            LOGGER.error(f"input filter format is wrong. please provide in dict format")
            exit(1)


#############################################################################################
# Function to rename the columns of spark dataframe                                         #
#############################################################################################

def sparkRenameColumns(df_data, columns):
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df_data = df_data.withColumnRenamed(old_name, new_name)
        return df_data
    else:
        LOGGER.error("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
        exit(1)

#############################################################################################
# Function to FLaten the Spark JSON DF into Spark FLat DF                                   #
#############################################################################################

def sparkParseJSON(data_df,parse_config):
    if isinstance(parse_config, list):
        print("as expected the input parse config is a list.")
        try:
            print("JSON to FLat Fle: parsing starts.")
            LOGGER.info("JSON to FLat Fle: parsing starts.")
            for iter_config in parse_config:
                v_columns = iter_config['columns']
                if bool(iter_config['explode_attr']) or iter_config['explode_attr'].strip() != '':
                    v_explode_attr = iter_config['explode_attr'].strip()
                    data_df = data_df.withColumn(v_explode_attr,F.explode(data_df[v_explode_attr])).select(v_columns)
                else:
                    data_df = data_df.select(v_columns)
                if bool(iter_config['rename']):
                    v_rename = iter_config['rename']
                    data_df = sparkRenameColumns(data_df,v_rename)
            
            return data_df

        except Exception as e:
            tb = sys.exc_info()[2]
            lineno = tb.tb_lineno
            LOGGER.error(f'Code Error at LINE: {lineno}.')
            LOGGER.error(f'JSON to FLat Fle: parsing failed with the error: . {e}')
            exit(1)

    else:
        LOGGER.error("The parse config obtained is incorrect. please valudate your input config json.")
        exit(1)





#############################################################################################
# Reading input  Attribute list config Json                                                 #
#############################################################################################
LOGGER.info(f'Reading input  Attribute list config Json.')

try:
    with open(v_attr_lst,'r') as attr_conf:
        attr_map = json.load(attr_conf)
except Exception as e:
    tb = sys.exc_info()[2]
    lineno = tb.tb_lineno
    LOGGER.error(f'Code Error at LINE: {lineno}.')
    LOGGER.error(f'The process failed due to invalid Attribute list config Json: {e}')
    exit(1)    

if v_parse_idc.strip() != '':
    attr_dict = [x for x in attr_map if x['indicator'] == v_parse_idc.lower()][0]
    LOGGER.info(f'the attribute mapping for the parsing idicator : {v_parse_idc} is : {attr_dict}')
    print(attr_dict)

    if attr_dict['jsonString'] == 0:
        print("Input feed is a flat feed.")
        LOGGER.info('Input feed is a flat feed.')

        if attr_dict['delimiter'].strip() == '':
            print("no delimiter provided")
            LOGGER.error('no delimiter provided for the input flat feed. Exitig with error code 1. Please provide delimter value in config json.')
            exit(1)
        else:
            print(f"the delimiter provided: {attr_dict['delimiter']}") 
            LOGGER.info(f"the delimiter provided: {attr_dict['delimiter']}")

            try:
                ip_data = sc.textFile(v_src_hdfs)
                datafields = ip_data.map(lambda l: l.split(str(attr_dict['delimiter'].strip()))) 
                ip_columns = datafields.first()
                ip_rows=datafields.filter(lambda l: l!=ip_columns)
                data_df = ip_rows.toDF(ip_columns)
                print('flat file parsed into pyspark dataframe.')


            except Exception as e:
                tb = sys.exc_info()[2]
                lineno = tb.tb_lineno
                LOGGER.error(f'Code Error at LINE: {lineno}.')
                LOGGER.error(f'The raw input flat feed read failed with the error: {e}')
                exit(1) 

            try:
                if 'filter' in attr_dict.keys():
                    filter_dict = attr_dict['filter']
                    if bool(filter_dict):
                        data_df = applyFilter(filter_dict,v_parse_idc,data_df)
                        data_df.show()
                    else:
                        print('no filteration provided for the input flat feed.')
                else:
                    print('no filteration provided for the input flat feed.')

            except Exception as e:
                tb = sys.exc_info()[2]
                lineno = tb.tb_lineno
                LOGGER.error(f'Code Error at LINE: {lineno}.')
                LOGGER.error(f'filtering the input flat feed failed with the error : {e}')
                exit(1)

            

    else:
        try:
            print("Input Feed is a Json String")
            rddjson = sc.textFile(v_src_hdfs)
            data_df = sqlContext.read.json(rddjson)
            LOGGER.info(f'Spark DataFrame Conversion For input raw json string feed Successful.')

        except Exception as e:
            tb = sys.exc_info()[2]
            lineno = tb.tb_lineno
            LOGGER.error(f'Code Error at LINE: {lineno}.')
            LOGGER.error(f'The raw input json feed read failed with the error: {e}')
            exit(1)  

        try:
            if 'filter' in attr_dict.keys():
                filter_dict = attr_dict['filter']
                if bool(filter_dict):
                    data_df = applyFilter(filter_dict,v_parse_idc,data_df)
                    #data_df.show()
                else:
                    print('no filteration provided for the input flat feed.')
            else:
                print('no filteration provided for the input flat feed.')

        except Exception as e:
            tb = sys.exc_info()[2]
            lineno = tb.tb_lineno
            LOGGER.error(f'Code Error at LINE: {lineno}.')
            LOGGER.error(f'filtering the input flat feed failed with the error : {e}')
            exit(1)

        try:
            if bool(attr_dict['mapDetail']):
                ip_df_parser_attri_list = parseInputConfig(attr_dict['mapDetail'])
                print("ip_df_parser_attri_list: ",ip_df_parser_attri_list)
                ###
                print("we will call the parser function now") 
                data_df = sparkParseJSON(data_df,ip_df_parser_attri_list)
                data_df.show()
            else:
                print("No nested attribute.parse everything from input json.")

        except Exception as e:
            tb = sys.exc_info()[2]
            lineno = tb.tb_lineno
            LOGGER.error(f'Code Error at LINE: {lineno}.')
            LOGGER.error(f'The JSON Feed parsing failed with the error: {e}')
            exit(1)

    #to write the dataframe into hdfs file
    #############################################################################################
    # to write the flat data frame into hdfs                                                    #
    #############################################################################################

    LOGGER.info(f'Writing into the target feed: {v_tgt_hdfs}')

    try:
        data_df.repartition(v_num_part).write.save(v_tgt_hdfs,sep='|',format='csv',mode='overwrite',header=True)
        LOGGER.info(f'Target pricing Flat feed generation is successful.')

    except Exception as e:
        tb = sys.exc_info()[2]
        lineno = tb.tb_lineno
        LOGGER.error(f'Code Error at LINE: {lineno}.')
        LOGGER.error(f'TTarget pricing Flat feed generation failed with exception : {e}')
        exit(1)

else:
    LOGGER.error(f'The script failed to parse.Please Provide the parsing indicator in the input parameter -i.')
    exit(1) 

exit(0)
