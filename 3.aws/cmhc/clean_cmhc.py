import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, when


def main(input, output):

    schema = types.StructType([
        types.StructField("Bachelor", types.DoubleType(), True),
        types.StructField("1 Bedroom", types.DoubleType(), True),
        types.StructField("2 Bedroom", types.DoubleType(), True),
        types.StructField("3 Bedroom +", types.DoubleType(), True),
        types.StructField("Total", types.DoubleType(), True)
    ])

    #read csv file
    df = spark.read.csv(input, header = True)
    df = df.withColumnRenamed('_c0', 'Area')
    #drop columns containing reliability index
    columns_to_drop = ['_c2', '_c4', '_c6', '_c8', '_c10'] 
    df = df.drop(*columns_to_drop)
    #drop last blank columns
    if df.select(df.columns[-1]).filter(col(df.columns[-1]).isNull()).count() == df.count(): 
        df = df.drop(df.columns[-1])
    #replace all ** to -1
    for col_name in df.columns:
        df = df.withColumn(col_name, when(col(col_name) == "**", "-1").otherwise(col(col_name)))

    # df.show()
    
    #output to a csv file
    df.coalesce(1).write.option("header", True).csv(output)


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('cmhc').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)