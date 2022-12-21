# Databricks notebook source
import json

import pandas as pd
import numpy as np
from datetime import datetime
import configparser

# MAGIC %run /Shared/DFP/RuleEngine/RuleEngineDev/CleanupFiles

# COMMAND ----------

# MAGIC %run /Shared/DFP/RuleEngine/RuleEngineDev/QueryBuilder

# COMMAND ----------

# MAGIC %run /Shared/DFP/RuleEngine/RuleEngineDev/JsonParser

# COMMAND ----------

# MAGIC %run /Shared/DFP/RuleEngine/RuleEngineDev/CsvParser

# COMMAND ----------

# MAGIC %run /Shared/DFP/RuleEngine/RuleEngineDev/Utility

# COMMAND ----------

# fileName geting from ADF pipeline

from CsvParser import getDFfromCsv, getDFfromXlsxMerge, getDFfromXls, check_dtype, check_ruleValidation
from JsonParser import GetAllValueByKey, GetRules
from Utility import getUniqueValueList, list_contains


async def run_driver(request, files):
    request = json.loads(request)
    configFilePathCntnr = request.get("configFilePathContainer")
    print(configFilePathCntnr)

    # COMMAND ----------

    # Read the App configuraiton ruleEngineConfiguraitonXls.ini
    mountPath = '/dbfs/mnt'
    mountPathXlsx = 'dbfs:/mnt'
    configFilePath = mountPath + configFilePathCntnr
    stage_dir = "/dbfs/mnt/sgre-apps/data_validation_engine/"
    subject = 'Tower'
    parser = configparser.ConfigParser()
    parser.read(configFilePath)

    ruleFilePath = request.get('RULE_FILE_PATH')
    outputDir = parser.get('APP', 'OUTPUT_FILE_PATH')
    reportOutputDir = outputDir + 'report/'
    errorOutputDir = outputDir + 'error/'
    rules = GetRules(mountPath + ruleFilePath)
    print("Total rules in the rule file - ", len(rules))

    date = datetime.now().strftime("%Y%m%d_%I%M%S")
    TablesName = GetAllValueByKey(rules, "DataObject")
    TableList = getUniqueValueList(TablesName)

    # Data type validatoin
    sysValidDataTypeList = ['int', 'string', 'bool', 'float']
    temp = GetAllValueByKey(rules, "DataType")
    dataTypeList = getUniqueValueList(temp)
    dataTypeValidation = list_contains(sysValidDataTypeList, dataTypeList)
    print("Data type in rule file is valid - " + str(dataTypeValidation))

    # Operator validation
    sysValOperatorList = ['regex', 'reference', 'notnull', ]
    temp = GetAllValueByKey(rules, "ValidationOperator")
    operatorList = getUniqueValueList(temp)
    operatorValidator = list_contains(sysValOperatorList, operatorList)
    print("Operator validation in rule file is valid - " + str(operatorValidator))

    SOURCE_TYPE = parser.get('APP', 'SOURCE_TYPE')
    SOURCE_DATA_FILE_PATH = mountPath + parser.get('SOURCE', 'SOURCE_DATA_FILE_PATH')
    SOURCE_DATA_FILE_PATH_XLS = mountPathXlsx + parser.get('SOURCE', 'SOURCE_DATA_FILE_PATH')
    SKIP_ROWS = parser.get('SOURCE', 'SKIP_ROWS')

    csvdf = pd.DataFrame()
    # Read the CSV
    if SOURCE_TYPE == 'CSV':
        csvdf = getDFfromCsv(SOURCE_DATA_FILE_PATH, SKIP_ROWS)

    # Read the XLS
    if SOURCE_TYPE == 'XLS':
        csvdf = getDFfromXls(SOURCE_DATA_FILE_PATH, SKIP_ROWS)

    # Read the XLS
    if SOURCE_TYPE == 'XLSX':
        csvdf = getDFfromXlsxMerge(SOURCE_DATA_FILE_PATH, SKIP_ROWS)

    dtp = []
    for colName in csvdf.columns.tolist():
        dtp.append(check_dtype(csvdf, colName))

    ruleColList = GetAllValueByKey(rules, "DataAttribute")
    ruleColList = getUniqueValueList(ruleColList)

    csvColList = csvdf.columns
    csvColList = getUniqueValueList(csvColList)
    csvcolValidator = list_contains(ruleColList, csvColList)
    rulcolValidator = list_contains(csvColList, ruleColList)

    df_rule = pd.DataFrame(rules)
    df_rdf = pd.DataFrame()
    df_rdf = df_rule[['RuleID', 'sequence', 'DataAttribute', 'DataType']]
    df_rdf = df_rdf.loc[df_rdf['sequence'].notna()]
    df_rdf.sort_values(by=['sequence'], ascending=False)
    df_rdf = df_rdf.drop_duplicates(['DataAttribute', 'DataType'], keep='first').reset_index(drop=True)

    df_csv = pd.DataFrame(list(zip(csvColList, dtp)),
                          columns=['CSV_Col_Name', 'CsvDataType'])

    bigdata = pd.concat([df_rdf, df_csv], axis=1).reindex(df_rdf.index)

    bigdata['CsvDataType'] = np.where(bigdata['CsvDataType'] == 'pass', bigdata['DataType'], bigdata['CsvDataType'])
    bigdata['CsvDataType'] = np.where(bigdata['DataType'] == 'date', bigdata['DataType'], bigdata['CsvDataType'])

    bigdata['Data_Type_Match'] = np.where(
        bigdata['DataType'] == bigdata['CsvDataType'], 'True', 'False')
    bigdata['Column_Match'] = np.where(
        df_rdf['DataAttribute'] == bigdata['CSV_Col_Name'], 'True', 'False')

    # Write bigdata dataframe to a csv
    Rule_Summary = pd.DataFrame(columns=['Severity', 'RuleId', 'RuleName', 'RuleDescription', 'ColumnName', 'Result'],
                                index=['0', '1'])

    if "False" in list(bigdata['Data_Type_Match']):
        Data_Type_Match_Flag = "Fail"
    else:
        Data_Type_Match_Flag = "Pass"

    if "False" in list(bigdata['Column_Match']):
        Column_Match_Flag = "Fail"
    else:
        Column_Match_Flag = "Pass"

    Rule_Summary.loc['0'] = ['1', 'Precheck-1', 'Precheck-Schema', 'NA', 'NA', Column_Match_Flag]
    Rule_Summary.loc['1'] = ['1', 'Precheck-2', 'Precheck-Datatype', 'NA', 'NA', Data_Type_Match_Flag]

    if "Fail" == Data_Type_Match_Flag or "Fail" == Column_Match_Flag:
        fileName = "Report_" + subject + "_" + date + ".csv"
        Rule_Summary.to_csv(mountPath + reportOutputDir + fileName, index=False)
        # dbutils.notebook.exit("Primary checks failed. Stop Execution")

    errFileName = "Error_" + subject + "_" + date + ".csv"

    err_out_df = pd.DataFrame()

    for rule in rules:
        if rule["ValidationOperator"] == 'None':
            continue
        else:
            print(rule["DataAttribute"], rule["RuleID"], rule["ValidationOperator"], rule["ValueToBeMatch"])
            var = check_ruleValidation(csvdf, rule["DataAttribute"], rule["RuleID"], rule["ValidationOperator"],
                                       rule["ValueToBeMatch"])
            'Severity', 'RuleId', 'RuleName', 'RuleDescription', 'ColumnName', 'Result'
            df = {'Severity': rule["Severity"], 'RuleId': rule["RuleID"], 'RuleName': rule["RuleName"],
                  'RuleDescription': rule["RuleDescription"], 'ColumnName': rule["DataAttribute"], 'Result': var}
            Rule_Summary = Rule_Summary.append(df, ignore_index=True)

    fileName = "Report_" + subject + "_" + date + ".csv"
    Rule_Summary.to_csv(mountPath + reportOutputDir + fileName, index=False)

    ##Write the same report csv as html file
    fileName = "Report_" + subject + "_" + date + ".html"
    html = Rule_Summary.to_html()
    filePath = mountPath + reportOutputDir
    text_file = open(filePath + fileName, "w")
    text_file.write(html)
    text_file.close()

    stage_file = stage_dir + 'error_Log.csv'
    df = pd.read_csv(stage_file)
    df.drop_duplicates().to_csv(mountPath + errorOutputDir + errFileName, index=False)
