# Databricks notebook source
import pandas
import spark as spark

repPath = 'dbfs:/mnt/sgre-error-logger/data_validation_engine/report/*.csv'
outPath = '/dbfs/mnt/sgre-internal/validation_engine/Output/analyzed_metrics.csv'

df = spark.read.format("csv"). \
    option("header", "True"). \
    option("inferSchema", "True"). \
    load(repPath)
df.registerTempTable("report")
df1 = spark.sql("select concat('Total_', Result) as Metric, count(*) as counts from report group by Result union "
                "select concat('Failure_Count_Severity ', Severity) as Metric, count(*) as counts from report where "
                "Result='Fail' group by Severity")
df2 = df1.toPandas()
print(df2)
df2.to_csv(outPath, index=False)
