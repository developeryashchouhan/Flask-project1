# Databricks notebook source
def useOperator(query, operator, ValueToBeMatch=None):
    if operator == 'regex':
        return query + ' LIKE ' + ValueToBeMatch
    elif operator == 'reference':
        return query
    elif operator == 'notnull':
        return query + ' IS NOT NULL'
    elif operator == 'lessthan':
        return query + ' < ' + ValueToBeMatch
    elif operator == 'greaterthan ':
        return query + ' > ' + ValueToBeMatch
    elif operator == 'lessthanequal':
        return query + ' <= ' + ValueToBeMatch
    elif operator == 'greaterthanequal':
        return query + ' >= ' + ValueToBeMatch
    elif operator == 'equal':
        return query + ' = ' + ValueToBeMatch
    elif operator == 'inequal':
        return query + ' <> ' + ValueToBeMatch
    elif operator == 'contains ':
        return query + ' LIKE %' + ValueToBeMatch
    else:
        return query


def getSQL(dic):
    table = dic["DataObject"]
    column = dic["DataAttribute"]
    dataType = dic["DataType"]
    ValueToBeMatch = ''
    ValOperator = ''
    if "ValidationOperator" in dic:
        ValOperator = dic["ValidationOperator"]
    if "ValueToBeMatch" in dic:
        ValueToBeMatch = dic["ValueToBeMatch"]
    if "refDataObject" in dic:
        refDataObject = dic["refDataObject"]
    if "refDataAttribute" in dic:
        refDataAttribute = dic["refDataAttribute"]

    if (ValOperator != ''):
        query = 'SELECT 1 FROM ' + table + ' where ' + column
    else:
        query = 'SELECT 1 FROM ' + table + ' where ' + column

    if (ValOperator is not None or ValOperator != '') and (ValueToBeMatch != '' or ValueToBeMatch is not None):
        query = useOperator(query, ValOperator, ValueToBeMatch)
    elif ValOperator is not None or ValOperator != '':
        query = useOperator(query, ValOperator)
    return query
