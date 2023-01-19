# Databricks notebook source
from configparser import ConfigParser


def getConfigurationSection(appConfigFilePath, section):
    configur = ConfigParser()
    fileContent = configur.read(appConfigFilePath)
    app_config = fileContent._sections[section]
    return app_config


def getAppConfiguration(self):
    self.RULE_FILE_PATH = self.configur.get('APP', 'RULE_FILE_PATH')
    self.SOURCE_TYPE = self.configur.get('APP', 'SOURCE_TYPE')
    self.OUTPUT_FILE = self.configur.get('APP', 'OUTPUT_FILE')


def getSourceConfiguration(self):
    self.SOURCE_DATA_FILE_PATH = self.configur.get(
        'SOURCE', 'SOURCE_DATA_FILE_PATH')
    self.SKIP_ROWS = self.configur.get('SOURCE', 'SKIP_ROWS')
    self.SQL_SERVER = self.configur.get('SOURCE', 'SQL_SERVER')
    self.DATABASE_NAME = self.configur.get('SOURCE', 'DATABASE_NAME')
    self.TENANT = self.configur.get('SOURCE', 'TENANT')
    self.USER_CLIENT_ID = self.configur.get('SOURCE', 'USER_CLIENT_ID')
    self.PASSWORD_CLIENT_SECRET = self.configur.get(
        'SOURCE', 'PASSWORD_CLIENT_SECRET')
