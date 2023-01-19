# Databricks notebook source

def list_contains(parentList, childList):
    list_of_missing_item = []
    for val in childList:
        # if there is a match
        if val not in parentList:
            list_of_missing_item.append(val)
    return list_of_missing_item


def getUniqueValueList(values):
    list_of_unique_value = []
    for i in values:
        if i not in list_of_unique_value:
            list_of_unique_value.append(i)
    return list_of_unique_value
