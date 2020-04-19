#!/usr/bin/env python3
# coding: utf-8

# In[1]:
from pyspark.sql.functions import desc, col, lag, date_sub, to_date
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# In[2]:
from config import datalake_features_path, datalake_staged_path, daily_feature_countries


# In[3]:
spark = SparkSession.builder.appName('covid_daily_feature').getOrCreate()

# In[4]:
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .load(datalake_staged_path+"/full"))

# In[5]:
df = (df
      .filter((col("country_region").isin(
          daily_feature_countries)))
      .filter(col("province_state") == "n/a")
      .orderBy(desc("date"))
      # Two days for 2 countries
      .limit(2*2)
      )

# In[6]:
window = Window.partitionBy("country_region").orderBy("date")

df_lag = (
    df.withColumn('prev_confirmed', lag(col('confirmed')).over(window))
    .withColumn('prev_deaths', lag(col('deaths')).over(window))
    .withColumn('prev_recovered', lag(col('recovered')).over(window))
)

result = (df_lag.withColumn('new_confirmed', df_lag['confirmed'] - df_lag['prev_confirmed'])
          .withColumn('new_deaths', df_lag['deaths'] - df_lag['prev_deaths'])
          .withColumn('new_recovered', df_lag['recovered'] - df_lag['prev_recovered'])
          .orderBy(desc("date"))
          .limit(2)
          .select("date", "country_region", "new_confirmed", "confirmed", "deaths", "new_deaths", "recovered", "new_recovered"))


# In[7]:
(result.repartition(col("country_region"))
 .write
 .partitionBy("country_region", "date")
 .mode('append')
 .json(datalake_features_path+"/daily/"))
