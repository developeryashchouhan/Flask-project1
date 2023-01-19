# Databricks notebook source
import pandas as pd
import re

err_summary = {"Rule_ID": [], "colname": [], "err_identifier": []}


def find_erred(colname, act_dtype, Rule_ID):
    exdf = pd.read_excel("C:/Users/s.mukherjee.ext/Desktop/ruleEngineDev/towersxlhdr.xlsx", header=3)

    for i in list(exdf[colname]):
        try:

            if act_dtype == 'string':
                if re.search(r"[a-zA-Z]", str(i)):
                    err_summary["Rule_ID"].append(Rule_ID)
                    err_summary["colname"].append(colname)
                    err_summary["err_identifier"].append(i)
                    print(Rule_ID, colname, i)

            if act_dtype == 'int':
                if str(i).isdigit():
                    err_summary["Rule_ID"].append(Rule_ID)
                    err_summary["colname"].append(colname)
                    err_summary["err_identifier"].append(i)
                    print(i)

            if act_dtype == 'float':
                if float(i):
                    err_summary["Rule_ID"].append(Rule_ID)
                    err_summary["colname"].append(colname)
                    err_summary["err_identifier"].append(i)
                    print(i)

            if act_dtype == 'bool':
                if i == 'No' or i == 'Yes':
                    err_summary["Rule_ID"].append(Rule_ID)
                    err_summary["colname"].append(colname)
                    err_summary["err_identifier"].append(i)
                    print(i)
        except:
            print


def log_string_to_int_mismatch():
    df = pd.read_csv("C:/Users/s.mukherjee.ext/Desktop/ruleEngineDev/ruleenginesummary.csv")
    filt_df = df[['DataAttribute', 'RuleID', 'CsvDataType']][
        (df['Data_Type_Match'].astype(str) == "False") & (df['DataType'].astype(str) == "int")]
    print(filt_df)
    for index, row in filt_df.iterrows():
        # print (row["DataAttribute"], row["CsvDataType"], row["RuleID"])
        find_erred(row["DataAttribute"], row["CsvDataType"], row["RuleID"])
    err_out = pd.DataFrame(err_summary)
    return err_out
