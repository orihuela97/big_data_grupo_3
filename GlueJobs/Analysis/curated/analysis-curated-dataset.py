
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
df_dim_users = (
    spark.read
    .format("parquet")
    .load("s3://pr2-grupo3-rodaan-curated-layer/data/dim_users/")
)
df_dim_users.show()
df_dim_countries = (
    spark.read
    .format("parquet")
    .load("s3://pr2-grupo3-rodaan-curated-layer/data/dim_countries/")
)
df_dim_countries.show()
df_dim_horoscopes = (
    spark.read
    .format("parquet")
    .load("s3://pr2-grupo3-rodaan-curated-layer/data/dim_horoscopes/")
)
df_dim_horoscopes.show()
df_dim_facts = (
    spark.read
    .format("parquet")
    .load("s3://pr2-grupo3-rodaan-curated-layer/data/facts_user/")
)
df_dim_facts.show(truncate=False)
df_dim_users_rename_id = (
    df_dim_users
    .withColumnRenamed("#id","user_id")
)
def day(date):
    date_str = str(date)
    date_split = date_str.split("-")
    day = int(date_split[2])
    return day

dayUDF = udf(lambda date: day(date))

df_dim_users_month_day_register = (
    df_dim_users_rename_id
    .withColumn("register_month", month("register_date"))
    .withColumn("register_day", dayUDF("register_date"))    
)
df_dim_users_month_day_register.show()
df_users_join_horoscopes = (
    df_dim_users_month_day_register
    .join(df_dim_horoscopes, (df_dim_users_month_day_register.register_day == df_dim_horoscopes.Day) & (df_dim_users_month_day_register.register_month == df_dim_horoscopes.Month),"left")
    .drop("Date","Day","Month")
    .withColumnRenamed("Horocope","horoscope")  
)
(
    df_dim_users_month_day_register
    .join(df_dim_horoscopes, (df_dim_users_month_day_register.register_day == df_dim_horoscopes.Day) & (df_dim_users_month_day_register.register_month == df_dim_horoscopes.Month),"left")
    .drop("Date","Day","Month")
    .withColumnRenamed("Horocope","horoscope")
    .where("horoscope is null")
    .show()  
)
df_dim_users_with_continent = (
    df_dim_users_month_day_register
    .join(df_dim_countries, df_dim_users_month_day_register.country_name == df_dim_countries.country, "left")
    .drop(df_dim_countries.country)
    
)
df_dim_users_with_continent.show()
df_dim_user_refined = (
    df_dim_users_with_continent
    .drop("nombre")
)
df_dim_groups = (
    df_dim_facts
    .select("group_name","group_id")
    .distinct()
    
    
)
count = (
    df_dim_facts
    .select("group_name")
    
    .distinct()
    .count()
    
)
print(f"Cuenta por group name {count} y cuante por los dos campos {df_dim_groups.count()}")
df_dim_facts.where( col("group_id").isNull()).show(truncate=False)

(
    df_dim_facts
    .groupBy("group_name","song_name")
    .agg(countDistinct("group_name","song_name").alias("cuenta"))
    .where(col("cuenta")>1)

    .show(truncate=False)
)
(
    df_dim_facts
    .select("group_name","song_name","group_id","song_id")
    .distinct()
    .count()
)
(
    df_dim_facts
    .where(col("user_id").isNull())
    .show(truncate=False)
)
job.commit()