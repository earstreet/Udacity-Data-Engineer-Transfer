# Import all needed packages
import pandas as pd
import numpy as np
import inspect
import configparser
import os
import glob
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import split, udf, col, lower
from pyspark.sql.types import IntegerType
import logging
import re
from datetime import datetime, timedelta

# configure logging format
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

# Get params from config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def outter_union(df1, df2):
    """
    Function according https://stackoverflow.com/a/56666722
    Keeps all columns in either df1 or df2
    """
    # Add missing columns to df1
    left_df = df1
    for column in set(df2.columns) - set(df1.columns):
        left_df = left_df.withColumn(column, F.lit(None))

    # Add missing columns to df2
    right_df = df2
    for column in set(df1.columns) - set(df2.columns):
        right_df = right_df.withColumn(column, F.lit(None))

    return left_df.unionByName(right_df)

   
def create_spark_session():
    """Create Spark Session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    return spark

def copy_raw_data(spark, input_data, output_data):
    """
    Copy raw data to S3.
    
    Parameters: 
    spark: Spark session
    input_data: path to raw data tables
    output_data: S3 bucket path to store output tables
    """
    # load smaller test table from parquet files
    immigration_data_small = os.path.join(input_data, 'sas_data/')
    immigration_small_df = spark.read.parquet(immigration_data_small)
    
    # load full table from sas files 
    immigration_data_path = os.path.join(input_data, 'data/18-83510-I94-Data-2016/*.sas7bdat')
    immigration_data = [f for f in glob.glob(immigration_data_path)]

    for file in immigration_data:
        if file == immigration_data[0]:
            immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(file)
        else:
            new_df = spark.read.format('com.github.saurfang.sas.spark').load(file)
            immigration_df = outter_union(immigration_df, new_df)
    
    temp_data = os.path.join(input_data, 'GlobalLandTemperaturesByState.csv')
    temp_df = spark.read.csv(temp_data, header=True)
    logging.info('Temperature data successfully loaded')

    cities_data = os.path.join(input_data, 'us-cities-demographics.csv')
    cities_df = spark.read.csv(cities_data, header=True, sep=';')
    logging.info('Cities data successfully loaded')

    airports_data = os.path.join(input_data, 'airport-codes_csv.csv')
    airports_df = spark.read.csv(airports_data, header=True)
    logging.info('Airports data successfully loaded')


    # put all tables into one dictionary
    df_raw_all = {#'immigration_data': immigration_small_df, # activate for using small test dataset
                'immigration_data': immigration_df, # activate for using full dataset
                'temp_data': temp_df, 
                'cities_data': cities_df,
                'airport_data': airports_df
                }

    for file, df in df_raw_all.items():
        # remove spaces and minus in column-names of dataframes
        df = df.select([F.col(col).alias(col.replace(' ', '_').replace('-', '_')) for col in df.columns])
        # write parquet files to raw path
        path = os.path.join(output_data, 'raw', file)
        df.write.parquet(path)
    logging.info(f'All tables copied to {output_data} as parquet')
    
def restore_data_from_s3(spark, s3_bucket):
    """
    Load data tables from S3.
    
    Parameters: 
    spark: Spark session
    s3_bucket: S3 bucket path to store output tables

    Returns:
    Dictionary with all data tables
    """
    raw_path = os.path.join(s3_bucket, 'raw')
    table_names = [y for x, y, z in os.walk(raw_path)][0]
    subdirs = glob.glob(raw_path + '/*/')
    df_raw_all = {}
    for name, path in zip(table_names, subdirs):
        df_raw_all[name] = spark.read.parquet(path)
        logging.info(f'Dataframe <{name}> from parquet-file in <{path}> successfully loaded')
    return df_raw_all

def load_sas_desc_file(input_data):
    """
    Load SAS description file for parsing.
    
    Parameters: 
    input_data: folder path to description file
    """
    # parse description file
    i94_desc_path = os.path.join(input_data, 'I94_SAS_Labels_Descriptions.SAS')
    with open(i94_desc_path) as f:
        lines = f.readlines()
        i94_desc_string = ''.join(lines)
        i94_desc_string = i94_desc_string.replace('\n', '')
        i94_desc_string = i94_desc_string.replace('\t', '')
    logging.info('SAS description file successfully processed')
    return i94_desc_string

def process_fact_table(spark, input_data, output_data, sas_desc_string):
    """
    Process fact table and write table to parquet.
    
    Parameters: 
    spark: Spark session
    input_data: dictionary with raw data tables
    output_data: S3 bucket path to store output tables
    sas_desc_string: Content of SAS description file as one string
    """
    df_raw_all = input_data
    i94_desc_string = sas_desc_string
    
    # Get country codes from SAS description file
    i94_country_codes = dict(re.findall(r'(\d{3})\s+\=\s+\'(.*?)\'', i94_desc_string))
    lol = list(map(list, i94_country_codes.items()))
    countries_table = spark.createDataFrame(lol, ["country_id", "country"])
    countries_table = countries_table.withColumn('country', lower(col('country')))
    # write table to staging folder
    path = os.path.join(output_data, 'staging', 'countries_table')
    countries_table.write.parquet(path)
    logging.info(f'countries_table successfully written in <{path}>')
    
    # Get state codes from SAS description file
    i94_state_codes = dict(re.findall(r"'(\w{2})'\s*=\s*'(.*?)\s*'", i94_desc_string))
    # remove port keys
    port_keys = ['AG', 'NK']
    for key in port_keys:
        del i94_state_codes[key]
    lol = list(map(list, i94_state_codes.items()))
    states_staging = spark.createDataFrame(lol, ["state_id", "state"])
    states_staging = states_staging.withColumn('state', lower(col('state')))
    # write table to staging folder
    path = os.path.join(output_data, 'staging', 'states_table')
    states_staging.write.parquet(path)
    logging.info(f'states_staging successfully written in <{path}>')
    
    # Create Mode and Visa codes from SAS description file
    i94_mode_codes = {
        1 : 'Air',
        2 : 'Sea',
        3 : 'Land',
        9 : 'Not reported'
    }
    i94_visa_codes = {
        1 : 'Business',
        2 : 'Pleasure',
        3 : 'Student'
    }
    lol = list(map(list, i94_mode_codes.items()))
    modes_table = spark.createDataFrame(lol, ["mode_id", "mode"])
    
    # write table to staging folder
    path = os.path.join(output_data, 'staging', 'modes_table')
    modes_table.write.parquet(path)
    logging.info(f'modes_table successfully written in <{path}>')
    
    lol = list(map(list, i94_visa_codes.items()))
    visas_table = spark.createDataFrame(lol, ["visa_id", "visa"])
    
    # write table to staging folder
    path = os.path.join(output_data, 'staging', 'visas_table')
    visas_table.write.parquet(path)
    logging.info(f'visas_table successfully written in <{path}>')
    
    # Create Immigration table
    immigrations_stage = df_raw_all['immigration_data']

    # replace all kind of None strings with Spark Null
    immigrations_stage = immigrations_stage.replace(['NaN', 'NONE', 'Null', 'null', 'None'],[None, None, None, None, None])

    # first day in 2016 as SAS date
    start_date = abs(datetime(2016, 1, 1).date() - datetime(1960, 1, 1).date()).days

    # drop rows with dates before 2016
    immigrations_stage = immigrations_stage.filter(immigrations_stage.arrdate >= start_date).filter(immigrations_stage.depdate >= start_date)
    
    # create timestamp from sas dates
    get_timestamp = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    immigrations_stage = immigrations_stage.withColumn('arrdate_ts', get_timestamp(immigrations_stage.arrdate))
    immigrations_stage = immigrations_stage.withColumn('depdate_ts', get_timestamp(immigrations_stage.depdate))
    
    # add mode columns
    immigrations_stage = immigrations_stage.join(modes_table, modes_table.mode_id == immigrations_stage.i94mode)                      
    
    # add visa columns
    immigrations_stage = immigrations_stage.join(visas_table, visas_table.visa_id == immigrations_stage.i94visa)
    
    # add country names to i94cit and i94res
    immigrations_stage = immigrations_stage.join(countries_table, countries_table.country_id == immigrations_stage.i94cit).withColumnRenamed('country','i94cit_country').drop('country_id')
    immigrations_stage = immigrations_stage.join(countries_table, countries_table.country_id == immigrations_stage.i94res).withColumnRenamed('country','i94res_country').drop('country_id')                                            
    
    # write table to staging folder
    path = os.path.join(output_data, 'staging', 'immigrations_stage')
    immigrations_stage.write.parquet(path)
    logging.info(f'immigrations_stage successfully written in <{path}>')
    
    immigrations_table = immigrations_stage.selectExpr(
        'cast(cicid as int)', 
        'arrdate_ts', 
        'depdate_ts', 
        'i94port', 
        'cast(i94mode as int)',
        'airline',
        'fltno',
        'cast(i94yr as int)',
        'cast(i94mon as int)',
        'visatype',
        'visa AS i94visa',
        'cast(i94bir as int)',
        'i94cit_country AS i94cit',
        'i94res_country AS i94res',
        'i94addr',
        'gender'
    ).dropDuplicates()
    path = os.path.join(output_data, 'ops', 'immigrations_table')
    immigrations_table.write.parquet(path)
    logging.info(f'immigrations_table successfully written in <{path}>')

def process_dim_tables(spark, input_data, output_data, sas_desc_string):
    """
    Process dim tables and write tables to parquet.
    
    Parameters: 
    spark: Spark session
    input_data: dictionary with raw data tables
    output_data: S3 bucket path to store output tables
    sas_desc_string: Content of SAS description file as one string
    """
    df_raw_all = input_data
    i94_desc_string = sas_desc_string

    # Get city codes from SAS description file
    i94_city_codes = dict(re.findall(r"'(\w{3})'\s*=\s*'(.*?)\s*'", i94_desc_string))

    lol = list(map(list, i94_city_codes.items()))
    cities_table = spark.createDataFrame(lol, ["city_id", "city_state"])
    
    # split city_state in two separate columns
    cities_table = cities_table.withColumn("city", split("city_state", ", ").getItem(0)).withColumn("state_code", split("city_state", ", ").getItem(1))
    cities_table = cities_table.drop('city_state')
    # write table to ops folder
    path = os.path.join(output_data, 'ops', 'cities_table')
    cities_table.write.parquet(path)
    logging.info(f'cities_table successfully written in <{path}>')


    # Get states data from Demographic table
    # convert necessary column to integer
    demographics_table = df_raw_all['cities_data'].withColumn("Male_Population", df_raw_all['cities_data']["Male_Population"].cast(IntegerType()))\
                                                    .withColumn("Female_Population", df_raw_all['cities_data']["Female_Population"].cast(IntegerType()))\
                                                    .withColumn("Total_Population", df_raw_all['cities_data']["Total_Population"].cast(IntegerType()))\
                                                    .withColumn("Number_of_Veterans", df_raw_all['cities_data']["Number_of_Veterans"].cast(IntegerType()))\
                                                    .withColumn("Foreign_born", df_raw_all['cities_data']["Foreign_born"].cast(IntegerType()))\
                                                    .withColumn("Count", df_raw_all['cities_data']["Count"].cast(IntegerType()))
    # pivot race column, aggregate by sum for state view
    group_cols = ['State_Code', 'State', 'Male_Population', 'Female_Population', 'Total_Population', 'Number_of_Veterans', 'Foreign_born']
    demographics_table = demographics_table.groupBy(group_cols).pivot('Race').sum('Count')
    demographics_table = demographics_table.groupBy(['State_Code', 'State']).sum()

    # replace blancs, 'sum' and brackets in column names
    demographics_table = demographics_table.select([F.col(col).alias(col.replace(' ', '_').replace('-','_').lower().replace(r'sum(', '').replace(r')', '')) for col in demographics_table.columns])

    # save states temperature table
    states_table = demographics_table.select('*')
    # write table to ops folder
    path = os.path.join(output_data, 'ops', 'states_table')
    states_table.write.parquet(path)
    logging.info(f'states_table successfully written in <{path}>')


    # Create Temperature tables
    temperatures_table = df_raw_all['temp_data']
    # filter table for the last complete year of the dataset which is in 2012
    temperatures_table = temperatures_table.where("dt LIKE '2012%'")
    # split date column to month and year because temperature in 2012 is only recorded monthly
    temperatures_table = temperatures_table.selectExpr(
        'year(dt) AS year',
        'month(dt) AS month',
        'AverageTemperature AS average_temperature',
        'State AS state',
        'Country AS country'
    )
    # save states temperature table
    us_state_temperatures_table = temperatures_table.select('*')
    # write table to ops folder
    path = os.path.join(output_data, 'ops', 'us_state_temperatures_table')
    us_state_temperatures_table.write.parquet(path, partitionBy=['country', 'state', 'year', 'month'])
    logging.info(f'us_state_temperatures_table successfully written in <{path}>')
    # create countries temperature table
    country_temperatures_table = temperatures_table.select('*').groupBy(['Country', 'month', 'year']).agg(F.avg('average_temperature').alias('average_temperature'))
    # lower country names for join
    country_temperatures_table = country_temperatures_table.withColumn('Country', lower(col('Country')))
    # write table to ops folder
    path = os.path.join(output_data, 'ops', 'country_temperatures_table')
    country_temperatures_table.write.parquet(path)
    logging.info(f'country_temperatures_table successfully written in <{path}>')

    # Create Dates table
    # load immigrations_table from parquet
    path = os.path.join(output_data, 'ops', 'immigrations_table')
    immigrations_table = spark.read.parquet(path)
    #collect dates
    arrdate_table = immigrations_table.select('arrdate_ts').distinct()
    depdate_table = immigrations_table.select('depdate_ts').distinct()
    date_table = arrdate_table.union(depdate_table).distinct().orderBy('arrdate_ts')
    # create table
    date_table = date_table.selectExpr(
        'arrdate_ts AS date',
        'day(arrdate_ts) AS day',
        'month(arrdate_ts) AS month',
        'year(arrdate_ts) AS year',
        'weekofyear(arrdate_ts) AS week',
        'weekday(arrdate_ts) AS weekday'
    )
    # write table to ops folder
    path = os.path.join(output_data, 'ops', 'date_table')
    date_table.write.parquet(path)
    logging.info(f'date_table successfully written in <{path}>')

def data_quality_check(spark, input_data):
    """
    Check quality of data tables.
    
    Parameters: 
    spark: Spark session
    input_data: path to data tables
    """

    # check part of star_schema, number of rows and columns of tables in ops folder 
    star_schema_tables = ['immigrations_table', 
                       'date_table', 
                       'cities_table', 
                       'states_table', 
                       'us_state_temperatures_table', 
                       'country_temperatures_table']
    found_tables = []

    path = os.path.join(input_data, 'ops')
    table_names = [y for x, y, z in os.walk(path)][0]
    subdirs = glob.glob(path + '/*/')
    df_check = {}
    for name, path in zip(table_names, subdirs):
        # check part of schema
        if name not in star_schema_tables:
            raise ValueError(f'Table {name} not known from star schema.')
        # load table from parquet
        df_check[name] = spark.read.parquet(path)
        logging.info(f'Dataframe <{name}> successfully loaded.')
        # count rows and columns
        rows_count = df_check[name].count()
        cols_count = len(df_check[name].columns)
        if rows_count < 1:
            raise ValueError(f'Data quality check failed! <{name}> contains 0 rows.')
        if cols_count < 2:
            raise ValueError(f'Data quality check failed! <{name}> contains 0 columns.')
        logging.info(f'Data Quality Checks passed successfully. <{name}> contains <{rows_count}> rows and <{cols_count}> columns.\n')
        found_tables.append(name)

    # sorting both the lists 
    star_schema_tables.sort() 
    found_tables.sort() 
    # check part of star schema 
    if star_schema_tables != found_tables:
        raise ValueError('Tables not identical to star schema.')
    logging.info('All tables from star schema exist.')



def main():
    """
    Main function to call all functions for the etl process.
    """
    spark = create_spark_session()
    logging.info('Spark Session created')
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-emr-project"
    #input_data = './data/'
    #output_data = '/Users/daniel/Desktop/output/'
    logging.info(f'Set input path to {input_data}')
    logging.info(f'Set output path to {output_data}')
    
    copy_raw_data(spark, input_data, output_data)

    s3_data = restore_data_from_s3(spark, output_data)
    
    sas_desc_string = load_sas_desc_file(input_data)
    
    process_fact_table(spark, s3_data, output_data, sas_desc_string)
    
    process_dim_tables(spark, s3_data, output_data, sas_desc_string)

    data_quality_check(spark, output_data)
    
    logging.info('ETL process successfully finished.')


if __name__ == "__main__":
    logging.info('Starting main()')
    main()
