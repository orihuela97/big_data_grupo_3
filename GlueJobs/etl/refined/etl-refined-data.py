
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import boto3
import time
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
REFINED_DATABASE = "pr2-grupo3-rodaan-refined-db"
S3_REFINED_BUCKET = "pr2-grupo3-rodaan-refined-layer"

CATALOG_TABLES = {"countries":"dim_countries","users":"dim_users","facts":"facts_user","horoscopes":"dim_horoscopes"}

S3_CURATED_COUNTRIES = "s3://pr2-grupo3-rodaan-curated-layer/data/dim_countries/"
S3_REFINED_HOROSCOPES = "s3://pr2-grupo3-rodaan-refined-layer/data/dim_horoscopes/"

S3_REFINED_COUNTRIES = "s3://pr2-grupo3-rodaan-refined-layer/data/dim_countries/"
S3_CURATED_HOROSCOPES = "s3://pr2-grupo3-rodaan-curated-layer/data/dim_horoscopes/"

S3_CURATED_USERS = "s3://pr2-grupo3-rodaan-curated-layer/data/dim_users/"
S3_REFINED_USERS = "s3://pr2-grupo3-rodaan-refined-layer/data/dim_users/"

S3_CURATED_FACTS = "s3://pr2-grupo3-rodaan-curated-layer/data/facts_user/"
S3_REFINED_FACTS = "s3://pr2-grupo3-rodaan-refined-layer/data/facts_user/"

PATH_LOG = "pr2-grupo3-rodaan-refined-layer/log/"

inf_VARIABLE_LECTURA_S3_COUNTRIES = " LOG INF: Reading countries data file to dataframe"
inf_VARIABLE_LECTURA_S3_USERS = " LOG INF: Reading users data file to dataframe"
inf_VARIABLE_LECTURA_S3_FACTS = " LOG INF: Reading facts data file to dataframe"
inf_VARIABLE_LECTURA_S3_HOROSCOPES = " LOG INF: Reading horoscopes data file to dataframe"




inf_VARIABLE_WRITING_DF_TO_S3 = " LOG INF: Writing dataframe to s3"
inf_VARIABLE_WRITING_DF_TO_S3_OK = " LOG INF: Writing dataframe to s3 finished OK"


inf_VARIABLE_TRANSFORMING_S3_HOROSCOPES = " LOG INF: Starting horoscopes transformation"

inf_VARIABLE_RENAME_COLUMNS_HOROSCOPES = " LOG INF: horoscopes renaming horoscope column to normalize data"

inf_VARIABLE_TRANSFORMING_S3_USERS = " LOG INF: Starting users transformation"

inf_VARIABLE_RENAME_COLUMNS_USERS = " LOG INF: users renaming id column to normalize data"
inf_VARIABLE_ADDING_HOROSCOPES_JOIN_FIELDS = " LOG INF: users adding fields day register and month register to join with horoscopes"



inf_VARIABLE_TRANSFORM_OK_HOROSCOPES = " LOG INF: horoscopes transform finished OK"

inf_VARIABLE_TRANSFORM_OK_USERS = " LOG INF: users cleaning transform OK"



inf_VARIABLE_LOAD_FACTS = " LOG INF: facts loading to refined stage"
inf_VARIABLE_LOAD_OK_FACTS = " LOG INF: facts loading finished OK"

inf_VARIABLE_LECTURA_S3_OK = " LOG INF: extracting sources finished ok"

inf_VARIABLE_ADDING_HOROSCOPES_USERS = " LOG INF: Adding horoscopes to users"
inf_VARIABLE_ADDING_CONTINENT_USERS = " LOG INF: Adding continent to users"
inf_VARIABLE_LOAD_USERS = " LOG INF: users loading to refined stage"
inf_VARIABLE_LOAD_OK_USERS = " LOG INF: users loading finished OK"

inf_VARIABLE_LOAD_COUNTRIES = " LOG INF: countries loading to refined stage"
inf_VARIABLE_LOAD_OK_COUNTRIES = " LOG INF: countries loading finished OK"

inf_VARIABLE_LOAD_HOROSCOPES = " LOG INF: horoscopes loading to refined stage"
inf_VARIABLE_LOAD_OK_HOROSCOPES = " LOG INF: horoscopes loading finished OK"

inf_VARIABLE_WRITING_DF_TO_S3 = " LOG INF: Starting loading to refined stage"
inf_VARIABLE_WRITING_DF_TO_S3_OK = " LOG INF: loading to refined stage finished OK"

inf_VARIABLE_CLEANING_DIRECTORY_PATH = " LOG INF: Cleaning the directory to write the new files"

DIRECTORY_PATH_COUNTRIES = "data/dim_countries"
DIRECTORY_PATH_USERS = "data/dim_users"
DIRECTORY_PATH_HOROSCOPES = "data/dim_horoscopes"
DIRECTORY_PATH_FACTS = "data/facts_user"

def get_timestamp():
    try:
        timestamp = time.time()
        fecha_hora_iso = time.strftime('%Y-%m-%dT%H:%M:%S%z', time.localtime(timestamp))
        return fecha_hora_iso
    except ValueError:
        return ValueError
def vaciar_carpeta_s3(bucket, carpeta):
    """
    Vacía una carpeta de un bucket de Amazon S3.

    Parámetros:
    - bucket: el nombre del bucket de S3
    - carpeta: el nombre de la carpeta que se desea vaciar (debe estar en el formato "carpeta/subcarpeta/")

    """

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)

    for obj in bucket.objects.filter(Prefix=carpeta):
        obj.delete()
def write_log_in_s3(path, texto, timestamp):
    path = path + timestamp + ".txt"
    prefix = "(" + str(get_timestamp()) + ")"
    texto = prefix + texto + '\n'
    s3 = boto3.client('s3')
    bucket_name, object_key = path.split('/', 1)
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        contenido_actual = obj['Body'].read().decode('utf-8')
        contenido_nuevo = contenido_actual + texto
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=contenido_nuevo)
    except s3.exceptions.NoSuchKey:
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=texto)
    else:
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=contenido_nuevo)
def write_df_to_s3(log_timestamp, directory_path, s3_path, glueContext, spark, catalog_database, catalog_table, df_to_write, partition_keys=[]):
    try:
        write_log_in_s3(PATH_LOG, inf_VARIABLE_WRITING_DF_TO_S3, log_timestamp)
        s3output = glueContext.getSink(
          path=s3_path,
          connection_type="s3",
          updateBehavior="UPDATE_IN_DATABASE",
          partitionKeys= partition_keys,
          compression="snappy",
          enableUpdateCatalog=True,
          transformation_ctx="s3output",
        )
        s3output.setCatalogInfo(
          catalogDatabase= catalog_database , catalogTableName= catalog_table
        )
        dynamic_frame = glueContext.create_dynamic_frame_from_rdd(df_to_write.rdd, "dynamic_frame")
        s3output.setFormat("glueparquet")
        write_log_in_s3(PATH_LOG, inf_VARIABLE_CLEANING_DIRECTORY_PATH, log_timestamp)
        vaciar_carpeta_s3(S3_REFINED_BUCKET, directory_path)
        s3output.writeFrame(dynamic_frame)
        write_log_in_s3(PATH_LOG, inf_VARIABLE_WRITING_DF_TO_S3_OK, log_timestamp)
    except Exception as e:
        error = "LOG: ERROR: " + str(e)
        write_log_in_s3(PATH_LOG, error, log_timestamp)
        raise e 
    
def read_s3_csv_to_df(log_timestamp, spark, s3_path, format = 'csv', header = 'true', delimiter = '\t'):
    try:
        df_to_return = (
            spark.read
            .format(format)
            .option("header", header)
            .option("delimiter",delimiter)
            .load(s3_path) 
        )
        return df_to_return
    except Exception as e:
        error = "LOG: ERROR: " + str(e)
        write_log_in_s3(PATH_LOG, error, log_timestamp)
        raise e 
def extract_data(log_timestamp, spark):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_USERS, log_timestamp)
    df_dim_users = read_s3_csv_to_df(log_timestamp, spark, S3_CURATED_USERS, 'parquet')
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_HOROSCOPES, log_timestamp)
    df_dim_horoscopes = read_s3_csv_to_df(log_timestamp, spark, S3_CURATED_HOROSCOPES, 'parquet')
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_FACTS, log_timestamp)
    df_dim_facts = read_s3_csv_to_df(log_timestamp, spark, S3_CURATED_FACTS, 'parquet')
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_COUNTRIES, log_timestamp)
    df_dim_contries = read_s3_csv_to_df(log_timestamp, spark, S3_CURATED_COUNTRIES, 'parquet')
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_OK, log_timestamp)
    
    return df_dim_users, df_dim_horoscopes, df_dim_facts, df_dim_contries
    
def day(date):
    date_str = str(date)
    date_split = date_str.split("-")
    day = int(date_split[2])
    return day
def transform_users(df_dim_users, df_dim_horoscopes, df_dim_contries, log_timestamp):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_TRANSFORMING_S3_USERS, log_timestamp)
    try:
        write_log_in_s3(PATH_LOG,inf_VARIABLE_RENAME_COLUMNS_USERS, log_timestamp)
        df_dim_users_rename_id = (
            df_dim_users
            .withColumnRenamed("#id","user_id")
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_ADDING_HOROSCOPES_JOIN_FIELDS, log_timestamp)
        dayUDF = udf(lambda date: day(date))

        df_dim_users_month_day_register = (
        df_dim_users_rename_id
            .withColumn("register_month", month("register_date"))
            .withColumn("register_day", dayUDF("register_date"))    
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_ADDING_HOROSCOPES_USERS, log_timestamp)
        df_users_join_horoscopes = (
            df_dim_users_month_day_register
            .join(df_dim_horoscopes, (df_dim_users_month_day_register.register_day == df_dim_horoscopes.Day) & (df_dim_users_month_day_register.register_month == df_dim_horoscopes.Month),"left")
            .drop("Date","Day","Month")
            .withColumnRenamed("Horocope","horoscope")  
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_ADDING_CONTINENT_USERS, log_timestamp)
        df_dim_users_refined = (
            df_users_join_horoscopes
            .join(df_dim_countries, df_users_join_horoscopes.country_name == df_dim_countries.country, "left")
            .drop(df_dim_countries.country)
            .drop("nombre","continente","pais")
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_TRANSFORM_OK_USERS, log_timestamp)
    except Exception as e:
        write_log_in_s3(PATH_LOG, str(e), log_timestamp)
        raise e 
    return df_dim_users_refined
    
def transform_horoscopes(df_dim_horoscopes, log_timestamp):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_TRANSFORMING_S3_HOROSCOPES, log_timestamp)
    try:
        write_log_in_s3(PATH_LOG,inf_VARIABLE_RENAME_COLUMNS_HOROSCOPES, log_timestamp)
        df_dim_horoscopes_refined = df_dim_horoscopes.withColumnRenamed("Horoscopes","horoscopes")
        write_log_in_s3(PATH_LOG,inf_VARIABLE_TRANSFORM_OK_HOROSCOPES, log_timestamp)
    except Exception as e:
        write_log_in_s3(PATH_LOG, str(e), log_timestamp)
        raise e 
    return df_dim_horoscopes_refined
    
def load_refined_data(log_timestamp, glueContext, spark, df_dim_users_refined, df_dim_horoscopes_refined, df_dim_countries_refined, df_dim_facts_refined):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_WRITING_DF_TO_S3, log_timestamp)
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_COUNTRIES, log_timestamp)
    write_df_to_s3(log_timestamp, DIRECTORY_PATH_COUNTRIES, S3_REFINED_COUNTRIES, glueContext, spark, REFINED_DATABASE, CATALOG_TABLES["countries"], df_dim_countries_refined)
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_OK_COUNTRIES, log_timestamp)
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_HOROSCOPES, log_timestamp)
    write_df_to_s3(log_timestamp, DIRECTORY_PATH_HOROSCOPES,S3_REFINED_HOROSCOPES, glueContext, spark, REFINED_DATABASE, CATALOG_TABLES["horoscopes"], df_dim_horoscopes_refined)
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_OK_HOROSCOPES, log_timestamp)
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_USERS, log_timestamp)
    write_df_to_s3(log_timestamp, DIRECTORY_PATH_USERS,S3_REFINED_USERS, glueContext, spark, REFINED_DATABASE, CATALOG_TABLES["users"], df_dim_users_refined)
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_OK_USERS, log_timestamp)
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_FACTS, log_timestamp)
    write_df_to_s3(log_timestamp, DIRECTORY_PATH_FACTS,S3_REFINED_FACTS, glueContext, spark, REFINED_DATABASE, CATALOG_TABLES["facts"], df_dim_facts_refined, partition_keys=["user_id"])
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LOAD_OK_FACTS, log_timestamp)
    
    write_log_in_s3(PATH_LOG,inf_VARIABLE_WRITING_DF_TO_S3_OK, log_timestamp)
    
    
    
    
timestamp = str(get_timestamp())

df_dim_users, df_dim_horoscopes, df_dim_facts, df_dim_countries = extract_data(timestamp, spark)

df_dim_users_refined = transform_users(df_dim_users, df_dim_horoscopes, df_dim_countries, timestamp)
df_dim_horoscopes_refined = transform_horoscopes(df_dim_horoscopes, timestamp)

load_refined_data(timestamp, glueContext, spark, df_dim_users_refined, df_dim_horoscopes_refined, df_dim_countries, df_dim_facts)



job.commit()