
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
CURATED_DATABASE = "pr2-grupo3-rodaan-curated"
S3_CURATED_BUCKET = "pr2-grupo3-rodaan-curated-layer"

CATALOG_TABLES = {"countries":"dim_countries","users":"dim_users","facts":"facts_user","horoscopes":"dim_horoscopes"}
S3_CURATED_COUNTRIES = "s3://pr2-grupo3-rodaan-curated-layer/data/dim_countries/"
S3_CURATED_HOROSCOPES = "s3://pr2-grupo3-rodaan-curated-layer/data/dim_horoscopes/"
S3_RAW_COUNTRIES = "s3://pr2-grupo3-rodaan-raw-layer/data/country/paises.csv"
S3_RAW_HOROSCOPES = "s3://pr2-grupo3-rodaan-raw-layer/data/horoscope/Horoscope.csv"

S3_RAW_USERS = "s3://pr2-grupo3-rodaan-raw-layer/data/user/userid-profile.tsv"
S3_CURATED_USERS = "s3://pr2-grupo3-rodaan-curated-layer/data/dim_users/"

S3_RAW_FACTS = "s3://pr2-grupo3-rodaan-raw-layer/data/user/userid-timestamp-artid-artname-traid-traname.tsv"
S3_CURATED_FACTS = "s3://pr2-grupo3-rodaan-curated-layer/data/facts_user/"

PATH_LOG = "pr2-grupo3-rodaan-curated-layer/log/"
inf_VARIABLE_LECTURA_S3_COUNTRIES = " LOG INF: Reading countries data file to dataframe"
inf_VARIABLE_CAMBIO_NOMBRES = " LOG INF: Changing column names"
inf_VARIABLE_GENERANDO_CONTINENTES_INGLES = " LOG INF: Creating column continent in english"
inf_VARIABLE_GENERANDO_PAISES_NUEVOS = " LOG INF: Adding country rows to dataframe"
inf_VARIABLE_ELIMINAR_PAISES_DUPLICADOS = " LOG INF: Removing congo duplicates from dataframe"
inf_VARIABLE_CAMBIANDO_CONTINENTES = " LOG INF: Updating continent in countries that don't have continent"
inf_VARIABLE_WRITING_DF_TO_S3 = " LOG INF: Writing dataframe to s3"
inf_VARIABLE_WRITING_DF_TO_S3_OK = " LOG INF: Writing dataframe to s3 finished OK"
inf_VARIABLE_CLEANING_OK_COUNTRIES = " LOG INF: Countries cleaning finished OK"

inf_VARIABLE_LECTURA_S3_HOROSCOPÈS = " LOG INF: Reading horoscopes data file to dataframe"
inf_VARIABLE_CLEANING_OK_HOROSCOPES = " LOG INF: Horoscopes cleaning finished OK"

inf_VARIABLE_LECTURA_S3_USERS = " LOG INF: Reading users data file to dataframe"
inf_VARIABLE_CLEANING_OK_USERS = " LOG INF: users cleaning finished OK"
inf_VARIABLE_CAMPO_CRUCE_PAIS_USERS = " LOG INF: users creating a column with the same country values as dim_countries to join"
inf_VARIABLE_FORMAT_DATE_USERS = " LOG INF: users creating a column with the correct date format for register date"
inf_VARIABLE_CLEANING_OK_USERS = " LOG INF: Users cleaning finished OK"

inf_VARIABLE_LECTURA_S3_FACTS = " LOG INF: Reading facts data file to dataframe"
inf_VARIABLE_CLEANING_OK_FACTS = " LOG INF: facts cleaning finished OK"
inf_VARIABLE_COLUMNAS_FACTS = " LOG INF: facts giving the correct name to its columns"
inf_VARIABLE_TIMESTAMPS_FACTS = " LOG INF: facts giving the correct format and column type to the fact timestamp"
inf_VARIABLE_CLEANING_NON_VALUE_ROWS_FACTS = " LOG INF: facts dropping rows without values in all columns"
inf_VARIABLE_CLEANING_OK_FACTS = " LOG INF: facts cleaning finished OK"

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
        vaciar_carpeta_s3(S3_CURATED_BUCKET, directory_path)
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
def clean_countries(spark, s3_path, glueContext, log_timestamp):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_COUNTRIES, log_timestamp)
    df_countries = read_s3_csv_to_df(log_timestamp, spark, S3_RAW_COUNTRIES, 'csv', 'true', delimiter = ',')
    ##limpieza de los nombres originales
    try:
        write_log_in_s3(PATH_LOG,inf_VARIABLE_CAMBIO_NOMBRES, log_timestamp)
        df_countries_refined_names = (
            df_countries
            .withColumnRenamed(' name', "country")
            .withColumnRenamed(' iso2', 'iso2')
            .withColumnRenamed(' iso3', 'iso3')
            .withColumnRenamed(' nom', 'nom')
            .withColumnRenamed(' phone_code', 'phone_code')
        )
        ##incluir campos de continentes en ingles
        ##el dataset original los tenia en español
        write_log_in_s3(PATH_LOG,inf_VARIABLE_GENERANDO_CONTINENTES_INGLES, log_timestamp)
        df_countries_english_names = (
            df_countries_refined_names
            .withColumn("continent",when( expr("continente == 'Europa'"), "Europe"  )
                       .when( expr("continente == 'África'"), "Africa"  )
                       .when( expr("continente == 'Australia y Oceanía'"), "Australia and Oceania"  )
                       .when( expr("continente == 'América'"), "America"  )
                       .when( expr("continente == 'Antártida'"), "Antarctica"  )
                       .otherwise( col("continente") ))
            .withColumnRenamed("nombre","pais")
        )
        ##Incluir costa de marfil, el data set original no lo tenia
        write_log_in_s3(PATH_LOG,inf_VARIABLE_GENERANDO_PAISES_NUEVOS, log_timestamp)
        df_countries_reduce_rows = (
            df_countries_english_names
            .drop("iso2","iso3","phone_code","nom")
        )

        df_new_row = spark.createDataFrame([["Cote D'Ivoire","Cote D'Ivoire","África","Africa"]])

        df_countries_union = df_countries_reduce_rows.union(df_new_row)
        ##El congo viene duplicado en el data set original
        ##Se puede eliminar una de las dos filas ya que no aporta ningun valor añadido extra
        write_log_in_s3(PATH_LOG,inf_VARIABLE_ELIMINAR_PAISES_DUPLICADOS, log_timestamp)
        df_countries_drop_duplicates = (
            df_countries_union
            .dropDuplicates()

        )
        ##Como se vio en la parte de analitycs, hay paises con continente a null
        ##Estos paises son archipielagos de islas que realmente no pertenecen a ningun continente
        ##Para diferenciar el valor null (no hay dato) del valor no tiene continente se cambia el valor de estos registros
        write_log_in_s3(PATH_LOG,inf_VARIABLE_CAMBIANDO_CONTINENTES, log_timestamp)
        df_countries_curated = (
        df_countries_drop_duplicates
            .withColumn("continente",
                        when( expr("continente is NULL"),"Sin continente" )
                        .otherwise(col("continente")) )
            .withColumn("continent",
                        when( expr("continent is NULL"),"Without continent" )
                        .otherwise(col("continent")) )
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_COUNTRIES, log_timestamp)
    except Exception as e:
        write_log_in_s3(PATH_LOG, str(e), log_timestamp)
        raise e 
    return df_countries_curated

def extract_horoscopes(spark, s3_path, glueContext, log_timestamp):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_HOROSCOPÈS, log_timestamp)
    df_horoscopes = read_s3_csv_to_df(log_timestamp, spark, S3_RAW_HOROSCOPES, 'csv', 'true', delimiter = ',')
    return df_horoscopes
def udf_date_convert(date):
    def right(s, amount):
        return s[-amount:]
    date_split = date.split(" ")
    new_date_list = []
    for i,value in enumerate(date_split):
        if i == 1:
            
            row = '0' + value.replace(",","")
            print(row)
            row = right(row,2)
            new_date_list.append(row)
        
        else:
            new_date_list.append(value)  
    if new_date_list[0] == 'Jan':
        value = '01'
    elif new_date_list[0] == 'Feb':
        value = '02'
    elif new_date_list[0] == 'Mar':
        value = '03'
    elif new_date_list[0] == 'Apr':
        value = '04'
    elif new_date_list[0] == 'May':
        value = '05'
    elif new_date_list[0] == 'Jun':
        value = '06'
    elif new_date_list[0] == 'Jul':
        value = '07'
    elif new_date_list[0] == 'Aug':
        value = '08'
    elif new_date_list[0] == 'Sep':
        value = '09'
    elif new_date_list[0] == 'Oct':
       value = '10'
    elif new_date_list[0] == 'Nov':
        value = '11'
    elif new_date_list[0] == 'Dec':
        value = '12'
    else:
        value = '00'
    return new_date_list[1] + '-' + str(value) + '-' + new_date_list[2]
def clean_users(spark, s3_path, glueContext, log_timestamp):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_USERS, log_timestamp)
    df_users = read_s3_csv_to_df(log_timestamp, spark, S3_RAW_USERS, 'csv', 'true', delimiter = "\t")
    try:
        write_log_in_s3(PATH_LOG,inf_VARIABLE_CAMPO_CRUCE_PAIS_USERS, log_timestamp)
        df_users_refined = (
            df_users
            .withColumn("country_name", when( expr("country like 'Congo%'"),"Congo" )
                        .when( expr("country like 'Korea, Democratic%'"),"South Korea" )
                        .when( expr("country like 'Russian%'"),"Russia" )
                        .when( expr("country like 'United States%'"),"United States of America" )
                        .otherwise(col("country")) )
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_FORMAT_DATE_USERS, log_timestamp)
        conversion = udf(lambda z: udf_date_convert(z))      
        df_users_date_conversion = df_users_refined.where(col("registered").isNotNull()).withColumn("register_date",conversion(col("registered")) )
        df_users_curated = df_users_date_conversion.withColumn("registered_date", to_date(col("register_date"),"dd-MM-yyyy")) 
        df_users_curated_final = df_users_curated.drop("register_date").withColumnRenamed("registered_date","register_date")
        write_log_in_s3(PATH_LOG,inf_VARIABLE_CLEANING_OK_USERS, log_timestamp)
    except Exception as e:
        write_log_in_s3(PATH_LOG, str(e), log_timestamp)
        raise e 
    return df_users_curated_final
def generate_list_users_to_clean(df_users_without_values):
    user_list_withtout_values = (
        df_users_without_values
        .select("#id")
        .collect()
    )
    list_users_to_clean = []
    for row in user_list_withtout_values:
       list_users_to_clean.append((row.asDict()["user_id"]))
    return list_users_to_clean
def clean_facts(spark, s3_path, glueContext, log_timestamp, df_users_curated):
    write_log_in_s3(PATH_LOG,inf_VARIABLE_LECTURA_S3_USERS, log_timestamp)
    df_facts = read_s3_csv_to_df(log_timestamp, spark, S3_RAW_FACTS, 'csv', 'false', delimiter = "\t")
    try:
        write_log_in_s3(PATH_LOG,inf_VARIABLE_COLUMNAS_FACTS, log_timestamp)
        df_user_facts_renamed = (
            df_facts
            .withColumnRenamed("_c1","fact_date")
            .withColumnRenamed("_c0","user_id")
            .withColumnRenamed("_c3","group_name")
            .withColumnRenamed("_c2","group_id")
            .withColumnRenamed("_c5","song_name")
            .withColumnRenamed("_c4","song_id")
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_TIMESTAMPS_FACTS, log_timestamp)
        df_user_facts_formatted_date = (
            df_user_facts_renamed
            .select("*", to_timestamp("fact_date").alias("fact_timestamp"))
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_CLEANING_NON_VALUE_ROWS_FACTS, log_timestamp)
        df_users_without_values = df_users_curated.where("registered is null")
        list_users_to_clean = generate_list_users_to_clean(df_users_without_values)
        df_filter_facts_with_users_not_valid = (
            df_user_facts_formatted_date
            .where(~col("user_id").isin(list_users_to_clean))
        )
        write_log_in_s3(PATH_LOG,inf_VARIABLE_CLEANING_OK_FACTS, log_timestamp)
    except Exception as e:
        write_log_in_s3(PATH_LOG, str(e), log_timestamp)
        raise e 
    return df_filter_facts_with_users_not_valid
    
    
timestamp = str(get_timestamp())

df_countries_curated = clean_countries(spark, S3_RAW_COUNTRIES, glueContext,timestamp)
write_df_to_s3(timestamp, DIRECTORY_PATH_COUNTRIES, S3_CURATED_COUNTRIES, glueContext, spark, CURATED_DATABASE, CATALOG_TABLES["countries"], df_countries_curated)


df_horoscopes_curated = extract_horoscopes(spark, S3_RAW_HOROSCOPES, glueContext,timestamp)
write_df_to_s3(timestamp, DIRECTORY_PATH_HOROSCOPES,S3_CURATED_HOROSCOPES, glueContext, spark, CURATED_DATABASE, CATALOG_TABLES["horoscopes"], df_horoscopes_curated)

df_users_curated = clean_users(spark, S3_RAW_USERS, glueContext,timestamp)
write_df_to_s3(timestamp, DIRECTORY_PATH_USERS,S3_CURATED_USERS, glueContext, spark, CURATED_DATABASE, CATALOG_TABLES["users"], df_users_curated)

df_facts_curated = clean_facts(spark, S3_RAW_FACTS, glueContext, timestamp, df_users_curated)
write_df_to_s3(timestamp, DIRECTORY_PATH_FACTS,S3_CURATED_FACTS, glueContext, spark, CURATED_DATABASE, CATALOG_TABLES["facts"], df_facts_curated, partition_keys=["user_id"])

job.commit()