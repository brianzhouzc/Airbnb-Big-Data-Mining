import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import monotonically_increasing_id, last, regexp_replace, col


spark = SparkSession.builder.appName("cmhc_average_rent").getOrCreate()
df = spark.read.option("header", "true").csv("average_rent_oct-2022.csv")


def reliability(df):
    r_map = {"a": "Excellent", "b": "Very good", "c": "Good", "d": "Fair"}
    for column in df.columns:
        for key, value in r_map.items():
            df = df.withColumn(column, when(col(column) == key, value).otherwise(col(column)))
    return df

df = reliability(df)
df = df.filter(~col(df.columns[0]).startswith("Notes"))
df = df.replace("**", "Data Suppressed")

for column in df.columns:
    if df.select(column).distinct().count() == 1 and df.select(column).distinct().collect()[0][0] in (None, "", " "):
        df = df.drop(column)

df.write.option("header", "true").csv("cleaned_data.csv")