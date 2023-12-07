import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from datetime import datetime
from glob import glob

def main(average_f, median_f, reference, output):
    
    schema = types.StructType([
        types.StructField("Bachelor", types.DoubleType(), True),
        types.StructField("1 Bedroom", types.DoubleType(), True),
        types.StructField("2 Bedroom", types.DoubleType(), True),
        types.StructField("3 Bedroom +", types.DoubleType(), True),
        types.StructField("Total", types.DoubleType(), True)
    ])

    average_df = spark.read.csv(average_f, header = True)
    average_df = average_df.withColumnRenamed('_c0', 'neighbourhood_average')
    #drop columns containing reliability index
    columns_to_drop = ['_c2', '_c4', '_c6', '_c8', '_c10'] 
    average_df = average_df.drop(*columns_to_drop)
    #drop last blank columns
    if average_df.select(average_df.columns[-1]).filter(col(average_df.columns[-1]).isNull()).count() == average_df.count(): 
        average_df = average_df.drop(average_df.columns[-1])
    #replace all ** to -1
    for col_name in average_df.columns:
        average_df = average_df.withColumn(col_name, when(col(col_name) == "**", None).otherwise(col(col_name)))
    
    average_df = average_df.drop(*['Bachelor', '1 Bedroom', '2 Bedroom', '3 Bedroom +'])
    average_df = average_df.withColumnRenamed('Total', 'average_rent')

    median_df = spark.read.csv(median_f, header = True)
    median_df = median_df.withColumnRenamed('_c0', 'neighbourhood_median')
    #drop columns containing reliability index
    columns_to_drop = ['_c2', '_c4', '_c6', '_c8', '_c10'] 
    median_df = median_df.drop(*columns_to_drop)
    #drop last blank columns
    if median_df.select(median_df.columns[-1]).filter(col(median_df.columns[-1]).isNull()).count() == median_df.count(): 
        median_df = median_df.drop(median_df.columns[-1])
    #replace all ** to -1
    for col_name in median_df.columns:
        median_df = median_df.withColumn(col_name, when(col(col_name) == "**", None).otherwise(col(col_name)))

    median_df = median_df.drop(*['Bachelor', '1 Bedroom', '2 Bedroom', '3 Bedroom +'])
    median_df = median_df.withColumnRenamed('Total', 'median_rent')
    

    source_df = average_df.join(median_df, average_df.neighbourhood_average == median_df.neighbourhood_median)
    source_df = source_df.drop(*['neighbourhood_average']).withColumnRenamed('neighbourhood_median', 'neighbourhood')

    reference_df = spark.read.csv(reference, header = True, inferSchema=True)
    reference_df = reference_df.withColumn('cmhc_array', split(col("cmhc"),";"))

    joined_df = source_df.join(reference_df, expr("array_contains(cmhc_array, neighbourhood)"))

    joined_df = joined_df.drop(*['cmhc_array', 'cmhc'])

    joined_df = joined_df.withColumn('median_rent', regexp_replace('median_rent', ',', '')) \
                         .withColumn('average_rent', regexp_replace('average_rent', ',', '')) \
                         .withColumn('median_rent', col('median_rent').cast(types.DoubleType())) \
                         .withColumn('average_rent', col('average_rent').cast(types.DoubleType()))

    joined_df = joined_df.groupBy('geojson').agg(avg('median_rent').alias('median_rent'), avg('average_rent').alias('average_rent'))

    joined_df.coalesce(1).write.option("header",True).csv(output)

if __name__ == '__main__':
    average_f = sys.argv[1]
    median_f = sys.argv[2]
    reference = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('Correlate Logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(average_f, median_f, reference, output)