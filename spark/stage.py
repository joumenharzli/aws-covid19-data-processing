#!/usr/bin/env python3
# coding: utf-8

# In[1]:
from pyspark.sql.functions import array, col, explode, lit, struct, regexp_replace, to_date, desc
from pyspark.sql import DataFrame, SparkSession
from pyspark.context import SparkContext


# In[2]:
from config import datalake_raw_path, datalake_staged_path, staged_countries


# In[3]:
spark = SparkSession.builder.appName('covid_staged').getOrCreate()

# In[4]:


def melt(
        df,
        id_vars, value_vars,
        var_name, value_name):
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
        col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


# In[5]:
def read_and_clean_df(table_name, values_column_name):
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(datalake_raw_path+"/202004140200/jhu_csse_covid_19_time_series_"+values_column_name+"_global__202004140200__202004140200.csv")
          .withColumnRenamed("Country/Region", "country_region")
          .withColumnRenamed("Lat", "latitude")
          .withColumnRenamed("Long", "longitude")
          .withColumnRenamed("Province/State", "province_state")
          )

    cols = df.schema.names
    base_cols = cols[:4]
    date_cols = cols[4:]

    df = df.filter(col("country_region").isin(staged_countries))

    df = melt(df, base_cols, date_cols,
              "date_not_formatted", values_column_name)

    return (df.withColumn("date",
                          to_date(regexp_replace("date_not_formatted", "_", "/"), "M/dd/yy"))
            .drop("date_not_formatted")
            .fillna({'province_state': 'n/a'})
            )


# In[6]:
confirmed_df = read_and_clean_df("raw_confirmed", "confirmed")
deaths_df = read_and_clean_df("raw_deaths", "deaths")
recovered_df = read_and_clean_df("raw_recovered", "recovered")

# In[7]:
join_cols = ["province_state",  "country_region",
             "latitude", "longitude", "date"]

full_df = (
    confirmed_df
    .join(deaths_df, join_cols, "outer")
    .join(recovered_df, join_cols, "outer")
)

# In[8]:
(full_df.repartition(4, col("country_region"))
 .write
 .partitionBy("country_region")
 .mode("Overwrite")
 .parquet(datalake_staged_path+"/full/"))
