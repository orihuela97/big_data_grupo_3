
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
df_users = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter",'\t')
    .load("s3://pr2-grupo3-rodaan-raw-layer/data/user/userid-profile.tsv")
)
df_users.show()
df_countries = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load("s3://pr2-grupo3-rodaan-raw-layer/data/country/paises.csv")
)
df_countries.show()
df_users_facts = (
    spark.read
    .format("csv")
    .option("delimiter",'\t')
    .load("s3://pr2-grupo3-rodaan-raw-layer/data/user/userid-timestamp-artid-artname-traid-traname.tsv")
)
df_users_facts.show()
df_horoscopes = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter",',')
    .load("s3://pr2-grupo3-rodaan-raw-layer/data/horoscope/Horoscope.csv")
)
df_horoscopes.show()
df_countries.columns
df_countries_refined_names = (
    df_countries
    .withColumnRenamed(' name', "country")
    .withColumnRenamed(' iso2', 'iso2')
    .withColumnRenamed(' iso3', 'iso3')
    .withColumnRenamed(' nom', 'nom')
    .withColumnRenamed(' phone_code', 'phone_code')
)
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
df_antijoin_users_countries = (
    df_users
    .join(df_countries_refined_names,"country","leftanti")
    .select("country")
    .distinct()
    .show(truncate = False)
)
df_countries_english_names.where(col("country") == "Congo").show()
df_users_refined = (
    df_users
    .withColumn("country_name", when( expr("country like 'Congo%'"),"Congo" )
                .when( expr("country like 'Korea, Democratic%'"),"South Korea" )
                .when( expr("country like 'Russian%'"),"Russia" )
                .when( expr("country like 'United States%'"),"United States of America" )
                .otherwise(col("country")) )
)
df_countries_reduce_rows = (
    df_countries_english_names
    .drop("iso2","iso3","phone_code","nom")
)

df_new_row = spark.createDataFrame([["Cote D'Ivoire","Cote D'Ivoire","África","Africa"]])

df_countries_union = df_countries_reduce_rows.union(df_new_row)
df_countries_union.where(col("country") == "Congo").show()
df_countries_drop_duplicates = (
    df_countries_union
    .dropDuplicates()
    
)
df_countries_drop_duplicates.where(col("country") == "Congo").show()
df_countries_drop_duplicates.where(col("continente").isNull()).show(truncate= False)
df_countries_curated = (
    df_countries_drop_duplicates
    .withColumn("continente",
                when( expr("continente is NULL"),"Sin continente" )
                .otherwise(col("continente")) )
    .withColumn("continent",
                when( expr("continent is NULL"),"Without continent" )
                .otherwise(col("continente")) )
)
df_countries_curated.where(col("continente").isNull()).show(truncate= False)
df_antijoin_users_countries = (
    df_users_refined
    .join(df_countries_union,  df_users_refined.country_name==df_countries_union.country,"leftanti")
    .select("country")
    .distinct()
    .show()
)
df_users_refined.show()

count_distinct = df_users_refined.select("#id").distinct().count()
count = df_users_refined.select("#id").count()
print(f"El numero de usuarios distintos es {count_distinct} y el numero total es {count}")
count = df_users_refined.where("registered is null").count()
print(f"El numero de usuarios sin fecha de registro es {count}")
df_users_refined.where("registered is null").show()
df_users_without_values = df_users_refined.where("registered is null")
(
    df_users_refined
    .where("country is null and registered is not null")
    .show(truncate = False)
    
)
total = (
    df_users_refined
    .where("country is null and registered is not null")
    .count() 
)
print(f"El total de usuarios sin pais registrado es: {total}")
df_users_refined.show()
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

conversion = udf(lambda z: udf_date_convert(z))      
df_users_date_conversion = df_users_refined.where(col("registered").isNotNull()).withColumn("register_date",conversion(col("registered")) )
df_users_curated = df_users_date_conversion.withColumn("registered_date", to_date(col("register_date"),"dd-MM-yyyy")) 
df_users_curated_final = df_users_curated.drop("register_date").withColumnRenamed("registered_date","register_date")
df_users_curated_final.show()
df_user_facts_renamed = (
    df_users_facts
    .withColumnRenamed("_c1","fact_date")
    .withColumnRenamed("_c0","user_id")
    .withColumnRenamed("_c3","group_name")
    .withColumnRenamed("_c2","group_id")
    .withColumnRenamed("_c5","song_name")
    .withColumnRenamed("_c4","song_id")
)
df_user_facts_renamed.where(col("group_name")=="Underworld").select("group_id","group_name").show(truncate=False)
df_user_facts_renamed.where(col("group_name")=="Underworld").select("group_id").distinct().show(truncate=False)
df_user_facts_renamed.where(col("song_name")=="Elysian Fields").select("song_id","song_name","group_name").show(truncate=False)
df_user_facts_renamed.show(truncate=False)
(
    df_user_facts_renamed
    .where(col("fact_date").isNull())
    .show(truncate=False)
)
df_user_facts_formatted_date = (
        df_user_facts_renamed
        .select("*", to_timestamp("fact_date").alias("fact_timestamp"))
)
df_user_facts_formatted_date.show()
(
    df_user_facts_formatted_date
    .where(col("fact_timestamp").isNull())
    .show(truncate=False)
)
(
    df_user_facts_formatted_date
    .groupBy("user_id")
    .agg(count("user_id").alias("total_facts"))
    .select("user_id","total_facts")
    .show()
)
(
    df_user_facts_formatted_date
    .where(col("song_name").isNull())
    .show(truncate=False)
)
(
    df_user_facts_formatted_date
    .where(col("group_name").isNull())
    .show(truncate=False)
)
user_list_withtout_values = (
    df_users_without_values
    .select("#id")
    .collect()
)
list_users_to_clean = []
for row in user_list_withtout_values:
   list_users_to_clean.append((row.asDict()["#id"]))
list_users_to_clean
df_filter_users_not_valid = (
    df_user_facts_formatted_date
    .where(~col("user_id").isin(list_users_to_clean))
)

df_facts_user_curated = (
    df_filter_users_not_valid
    .withColumn("year",year(col("fact_timestamp")))
    .withColumn("month",month(col("fact_timestamp")))
)
count_bef_filter = df_user_facts_formatted_date.count()
count_aft_filter = df_filter_users_not_valid.count()
print(f"Antes del filtro {count_bef_filter} Despues del filtro {count_aft_filter}")
job.commit()