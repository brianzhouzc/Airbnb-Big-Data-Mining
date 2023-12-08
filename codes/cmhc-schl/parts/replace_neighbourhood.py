import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from datetime import datetime
from glob import glob

def main(source, reference, output):
    
    source_df = spark.read.csv(source, header = True, inferSchema=True)


    reference_df = spark.read.csv(reference, header = True, inferSchema=True)
    reference_df = reference_df.withColumn('cmhc_array', split(col("cmhc"),";"))

    joined_df = source_df.join(reference_df, expr("array_contains(cmhc_array, Area)"))

    joined_df = joined_df.drop(*['Area', 'cmhc_array', 'cmhc'])
    joined_df = joined_df.withColumnRenamed('geojson', 'neighbourhood')

    joined_df = joined_df.groupBy('neighbourhood').agg(avg('Bachelor').alias('bachelor'), \
                                                       avg('1 Bedroom').alias('1_bedroom'), \
                                                       avg('2 Bedroom').alias('2_bedroom'), \
                                                       avg('3 Bedroom +').alias('3_bedroom+'), \
                                                       avg('Total').alias('total'))
    #print(joined_df.show(100, False))
    joined_df.coalesce(1).write.option("header",True).csv(output)
    #print("Done. Output saved at %s", output)

if __name__ == '__main__':
    source = sys.argv[1]
    reference = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('Correlate Logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(source, reference, output)