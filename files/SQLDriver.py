# Databricks notebook source
import os

import adal
import pandas as pd
import struct
import pyodbc


def GetSQLConnection(config):
    # input params
    tenant = os.getenv('TENANT-ID')
    server = os.getenv('SQL-SERVER')
    database = os.getenv('SQL-DATABASE')
    username = os.getenv('SQL-USERNAME')

    resource_app_id_url = os.getenv('RESOURCE-APP-ID-URL')
    authority_host_url = os.getenv('AUTHORITY-HOST-URL')
    authority_url = authority_host_url + '/' + tenant
    client_id = os.getenv('CLIENT-ID')

    context = adal.AuthenticationContext(authority_url, api_version=None)

    code = context.acquire_user_code(resource_app_id_url, client_id)
    print(code['message'])
    token = context.acquire_token_with_device_code(
        resource_app_id_url, code, client_id)

    odbcConnstr = "DRIVER={};SERVER={};DATABASE={}".format(
        "ODBC Driver 17 for SQL Server", server, database)

    # get bytes from token obtained
    tokenb = bytes(token["accessToken"], "UTF-8")
    exptoken = b''
    for i in tokenb:
        exptoken += bytes({i})
        exptoken += bytes(1)

    tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

    conn = pyodbc.connect(odbcConnstr, attrs_before={1256: tokenstruct})
    return conn


def ExecuteSQLQuery(conn, query):
    # query = 'SELECT TOP 3 name, collation_name FROM sys.databases;'
    result = pd.read_sql(query, conn)
    return result.head()
