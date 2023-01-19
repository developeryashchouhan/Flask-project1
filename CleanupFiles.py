# Databricks notebook source
import os

stage_dir = 'dbfs:/mnt/sgre-apps/data_validation_engine/'
error_dir = 'dbfs:/mnt/sgre-error-logger/data_validation_engine/error/'
report_dir = 'dbfs:/mnt/sgre-error-logger/data_validation_engine/report/'
# dbutils.fs.rm(stage_dir, True)
# dbutils.fs.rm(error_dir, True)
# dbutils.fs.rm(report_dir, True)
# dbutils.fs.mkdirs(stage_dir)
# dbutils.fs.mkdirs(error_dir)
# dbutils.fs.mkdirs(report_dir)
#stage_files = os.listdir(stage_dir)
#error_files = os.listdir(error_dir)
#report_dir = os.listdir(report_dir)
#print(stage_files)
#print(error_files)
#print(report_dir)

#for f in stage_files:
#    os.remove(f)
#stage_files = os.listdir(stage_dir)
#print(stage_files)