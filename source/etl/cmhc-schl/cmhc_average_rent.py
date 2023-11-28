import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import pandas as pd


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import monotonically_increasing_id, last, regexp_replace, col, when


def main(input, output):

    schema = types.StructType([
        types.StructField("Area", types.StringType(), True),
        types.StructField("Bachelor", types.StringType(), True),
        types.StructField("1 Bedroom", types.StringType(), True),
        types.StructField("2 Bedroom", types.StringType(), True),
        types.StructField("3 Bedroom +", types.StringType(), True),
        types.StructField("Total", types.StringType(), True)
    ])


    #read csv file
    df = spark.read.csv(input, header = True, inferSchema = True)
    #drop columns containing reliability index
    columns_to_drop = ['_c2', '_c4', '_c6', '_c8', '_c10']  # Replace with the actual columns
    df = df.drop(*columns_to_drop)
    #drop last blank columns
    if df.select(df.columns[-1]).filter(col(df.columns[-1]).isNull()).count() == df.count(): df = df.drop(df.columns[-1])


    df.show()
    


    #output to a csv file
    df.coalesce(1).write.option("header", True).csv(output)


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Correlate Logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)