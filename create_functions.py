# -*- coding: utf-8 -*-
"""create_functions.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1PquIXKwf8hBUvSlHszsW7y38vwsolFSr
"""

import pandas as pd

import datetime as dt
from datetime import timedelta

from pyspark.sql.functions import udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id

#from pyspark.sql.types import *

# function from clean_function.py
from clean_functions import group_temperature_data


def retrieve_visa_type_dim(spark, output_data):
    return spark.read.parquet(output_data + "visatype")


def create_immigration_fact_table(spark, df, output_data):
    """
      Creates country dimension from immigration and land temperatures datasets.
    """

    # retrieve visa_type dimension
    dim_df = retrieve_visa_type_dim(spark, output_data)

    # create/replace view for visa type
    dim_df.createOrReplaceTempView("visa_type_view")

    # udf that converts SAS format to datetime
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # rename columns
    df = df.withColumnRenamed('ccid', 'record_id') \
           .withColumnRenamed('i94res', 'residence_code') \
           .withColumnRenamed('i94addr', 'state_code')

    # create/replace view for immigration
    df.createOrReplaceTempView("immigration_view")

    # create visa_type key
    df = spark.sql(
        """
        SELECT 
            immigration_view.*, 
            visa_type_view.visa_type_id
        FROM immigration_view
        LEFT JOIN visa_type_view ON visa_type_view.visatype=immigration_view.visatype
        """
    )

    # converts date into datetime object
    df = df.withColumn("arrdate", get_datetime(df.arrdate))

    # drop visatype key
    df = df.drop(df.visatype)

    # write/overwrite dimension to parquet
    df.write.parquet(output_data + "immigration_fact", mode="overwrite")

    return immigration_df


def create_immigration_arrivals_dimension(df, output_data):
    """
      Creates immigration arrivals table
    """

    # udf that converts SAS format to datetime
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    arrivals_df = df.select(['arrdate']).withColumn("arrdate", get_datetime(df.arrdate)).distinct()

    # compartmentalize datetime data
    arrivals_df = arrivals_df.withColumn('arrival_day', dayofmonth('arrdate'))
    arrivals_df = arrivals_df.withColumn('arrival_week', weekofyear('arrdate'))
    arrivals_df = arrivals_df.withColumn('arrival_month', month('arrdate'))
    arrivals_df = arrivals_df.withColumn('arrival_year', year('arrdate'))
    arrivals_df = arrivals_df.withColumn('arrival_weekday', dayofweek('arrdate'))

    # create id field
    arrivals_df = arrivals_df.withColumn('id', monotonically_increasing_id())

    # write/overwrite dimension to parquet
    part_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    arrivals_df.write.parquet(output_data + "immigration_arrivals", partitionBy=part_columns, mode="overwrite")

    return arrivals_df



def create_demographics_dimension_table(df, output_data):
    """
      Creates demographics dimension table.
    """

    dim_df = df.withColumnRenamed('Median Age', 'median_age') \
                .withColumnRenamed('Male Population', 'male_population') \
                .withColumnRenamed('Female Population', 'female_population') \
                .withColumnRenamed('Total Population', 'total_population') \
                .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
                .withColumnRenamed('Foreign-born', 'foreign_born') \
                .withColumnRenamed('Average Household Size', 'average_household_size') \
                .withColumnRenamed('State Code', 'state_code')

    # add id column
    dim_df = dim_df.withColumn('id', monotonically_increasing_id())

    # write/overwrite dimension to parquet
    dim_df.write.parquet(output_data + "demographics", mode="overwrite")

    return demographics_df



def create_country_dimension_table(spark, df, temp_df, output_data, mapping_file):
    """
      Creates country dimension table.
    """

    # create/replace view for immigration
    df.createOrReplaceTempView("immigration_view")

    # create/replace view for countries codes
    mapping_file.createOrReplaceTempView("country_codes_view")

    # retreive grouped temperature data
    agg_temp = group_temperature_data(temp_df)

    # create/replace view for countries temperature
    agg_temp.createOrReplaceTempView("grouped_temperature_view")

    # create country dimension using SQL
    country_df = spark.sql(
        """
        SELECT 
            i94res as country_code,
            Name as country_name
        FROM immigration_view
        LEFT JOIN country_codes_view
        ON immigration_view.i94res=country_codes_view.code
        """
    ).distinct()

    # create temp country view
    country_df.createOrReplaceTempView("country_view")

    country_df = spark.sql(
        """
        SELECT 
            country_code,
            country_name,
            average_temperature
        FROM country_view
        LEFT JOIN grouped_temperature_view
        ON country_view.country_name=grouped_temperature_view.Country
        """
    ).distinct()

    # write/overwrite dimension to a parquet
    country_df.write.parquet(output_data + "country", mode="overwrite")

    return country_df



def create_visa_dimension_table(df, output_data):
    """
      Creates visa dimension from immigration dataset.
    
    """
    # create visa type df
    visatype_df = df.select(['visatype']).distinct()

    # add id column
    visatype_df = visatype_df.withColumn('visa_type_id', monotonically_increasing_id())

    # write/overwrite dimension to parquet
    visatype_df.write.parquet(output_data + "visatype", mode="overwrite")

    return visatype_df
  

def data_quality_check(df, table_name):
    """
        Quality check on fact and dimension tables.
    """
    
    row_count = df.count()

    if row_count == 0:
        print(f"Quality check Unsuccessful on {table_name} - zero records!")
    else:
        print(f"Quality check Successful on {table_name} - {total_count:,} records.")
    return 0