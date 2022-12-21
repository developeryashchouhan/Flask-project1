# Databricks notebook source
import json


def GetRules(ruleFilePath):
    with open(ruleFilePath) as rf:
        rules = json.load(rf)
        return rules


def GetElementByKeyValue(rules, rKey, rValue):
    for dic in rules:
        for key in dic:
            if key == rKey and dic[key] == rValue:
                return dic


def GetElementByKey(rules, rKey):
    print("-----")
    print(rKey)
    # print(rules)
    for dic in rules:
        # print(dic)
        for key in dic:
            # print(key + "----" + rKey)
            if key == rKey:
                return dic


def GetAllElementByKeyValue(rules, rKey, rValue):
    elementList = []
    for dic in rules:
        for key in dic:
            if key == rKey and dic[key] == rValue:
                elementList.append(dic)
    return elementList


def GetAllValueByKey(rules, rKey):
    elementList = []
    for dic in rules:
        for key in dic:
            if key == rKey:
                elementList.append(dic[key])
    return elementList


def GetValueByKey(rules, rKey):
    elementList = []
    for dic in rules:
        for key in dic:
            if key == rKey:
                elementList.append(dic[key])
    return elementList
