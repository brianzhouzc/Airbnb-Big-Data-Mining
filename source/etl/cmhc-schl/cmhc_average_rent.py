import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

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

    #df = spark.read \
        #.option("header", "true") \
        #.option("encoding", "ISO-8859-1") \
        #.option("quote", "\"") \
        #.csv(input)

    df = spark.read.csv(
        input,
        encoding = 'ISO-8859-1',
        schema = schema,
        quote = '"',
        sep = ',',
        header = False,
        #mode = "DROPMALFORMED"  # This mode drops lines that do not match the schema
    )


    df.show()



    df.coalesce(1).write.option("header", True).csv(output)


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Correlate Logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)