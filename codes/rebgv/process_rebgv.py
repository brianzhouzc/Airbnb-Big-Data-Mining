from PyPDF2 import PdfWriter, PdfReader
from nanonets import NANONETSOCR
import glob
import os
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import monotonically_increasing_id, last, regexp_replace, col, round
from pyspark.sql.window import Window
from datetime import datetime

def main(input, output):

    pdfs = glob.glob("%s/pdf/raw/*" % input)

    for raw_pdf in pdfs:
        print("Processing %s" % raw_pdf.split('/')[-1])
        
        output_name = raw_pdf.split('/')[-1].split('.')[0]
        output_name = "rebgv-%s%s" % (output_name.split('-')[3], output_name.split('-')[4])
        inputpdf = PdfReader(open(raw_pdf, "rb"))

        output = PdfWriter()
        output.add_page(inputpdf.pages[len(inputpdf.pages) - 7])
        output.add_page(inputpdf.pages[len(inputpdf.pages) - 6])
        if not os.path.exists("%s/pdf/trimed/" % input):
            os.makedirs("%s/pdf/trimed/" % input)
        with open("%s/pdf/trimed/%s.pdf" % (input, output_name), "wb") as outputStream:
            output.write(outputStream)

    model = NANONETSOCR()
    model.set_token('6dbb97df-896c-11ee-a76a-12e83ea2166a') # https://app.nanonets.com/#/keys

    pdfs = glob.glob("%s/pdf/trimed/*" % input)

    for raw_pdf in pdfs:
        print("Processing %s" % raw_pdf.split('/')[-1])
        
        output_name = raw_pdf.split('/')[-1].split('.')[0]
        model.convert_to_csv(raw_pdf, output_file_name = '%s/csv/raw/%s.csv' % (input, output_name))

    data_schema = types.StructType([
        types.StructField('property_type', types.StringType()),
        types.StructField('area', types.StringType()),
        types.StructField('benchmark_price', types.IntegerType()),
        types.StructField('price_index', types.DoubleType()),
        types.StructField('1_month_change_percentage', types.DoubleType()),
        types.StructField('3_month_change_percentage', types.DoubleType()),
        types.StructField('6_month_change_percentage', types.DoubleType()),
        types.StructField('1_year_change_percentage', types.DoubleType()),
        types.StructField('3_year_change_percentage', types.DoubleType()),
        types.StructField('5_year_change_percentage', types.DoubleType()),
        types.StructField('10_year_change_percentage', types.DoubleType()),
        types.StructField('time', types.DateType())
    ])

    f_df = spark.createDataFrame(sc.emptyRDD(), data_schema)

    csv_files = glob.glob("%s/csv/raw/*" % input)

    for csv in csv_files:
        print("Processing %s ..." % csv)

        df = spark.read.csv(csv)
        #remove any empty rows
        df = df.na.drop(subset=['_c10']) 

        # fill null with previously avaliable housing type
        df = df.withColumn('index', monotonically_increasing_id()) #add index to allow ordering
        windowSpec = Window.orderBy('index').rowsBetween(Window.unboundedPreceding, 0)
        df = df.withColumn('_c0', last('_c0', ignorenulls=True).over(windowSpec))
        df = df.drop('index') #drop index

        df = df.withColumnRenamed('_c0', 'property_type') \
            .withColumnRenamed('_c1', 'area') \
            .withColumnRenamed('_c2', 'benchmark_price') \
            .withColumnRenamed('_c3', 'price_index') \
            .withColumnRenamed('_c4', '1_month_change_percentage') \
            .withColumnRenamed('_c5', '3_month_change_percentage') \
            .withColumnRenamed('_c6', '6_month_change_percentage') \
            .withColumnRenamed('_c7', '1_year_change_percentage') \
            .withColumnRenamed('_c8', '3_year_change_percentage') \
            .withColumnRenamed('_c9', '5_year_change_percentage') \
            .withColumnRenamed('_c10', '10_year_change_percentage')
        
        df = df.na.replace('Residential / Composite', 'Composite', 'property_type')
        df = df.na.replace('Single Family Detached', 'House', 'property_type')

        df = df.withColumn('benchmark_price', regexp_replace('benchmark_price', '\$*\,*\s*', '')) \
            .withColumn('benchmark_price', col('benchmark_price').cast(types.IntegerType()))
        
        df = df.withColumn('price_index', col('price_index').cast(types.DoubleType()))

        df = df.withColumn('1_month_change_percentage', regexp_replace('1_month_change_percentage', '\%*\s*', '')) \
            .withColumn('1_month_change_percentage', col('1_month_change_percentage').cast(types.DoubleType())) \
            .withColumn('1_month_change_percentage', round(col('1_month_change_percentage') / 100, 3))
        
        df = df.withColumn('3_month_change_percentage', regexp_replace('3_month_change_percentage', '\%*\s*', '')) \
            .withColumn('3_month_change_percentage', col('3_month_change_percentage').cast(types.DoubleType())) \
            .withColumn('3_month_change_percentage', round(col('3_month_change_percentage') / 100, 3))
        
        df = df.withColumn('6_month_change_percentage', regexp_replace('6_month_change_percentage', '\%*\s*', '')) \
            .withColumn('6_month_change_percentage', col('6_month_change_percentage').cast(types.DoubleType())) \
            .withColumn('6_month_change_percentage', round(col('6_month_change_percentage') / 100, 3))
        
        df = df.withColumn('1_year_change_percentage', regexp_replace('1_year_change_percentage', '\%*\s*', '')) \
            .withColumn('1_year_change_percentage', col('1_year_change_percentage').cast(types.DoubleType())) \
            .withColumn('1_year_change_percentage', round(col('1_year_change_percentage') / 100, 3))
        
        df = df.withColumn('3_year_change_percentage', regexp_replace('3_year_change_percentage', '\%*\s*', '')) \
            .withColumn('3_year_change_percentage', col('3_year_change_percentage').cast(types.DoubleType())) \
            .withColumn('3_year_change_percentage', round(col('3_year_change_percentage') / 100, 3))
        
        df = df.withColumn('5_year_change_percentage', regexp_replace('5_year_change_percentage', '\%*\s*', '')) \
            .withColumn('5_year_change_percentage', col('5_year_change_percentage').cast(types.DoubleType())) \
            .withColumn('5_year_change_percentage', round(col('5_year_change_percentage') / 100, 3))
        
        df = df.withColumn('10_year_change_percentage', regexp_replace('10_year_change_percentage', '\%*\s*', '')) \
            .withColumn('10_year_change_percentage', col('10_year_change_percentage').cast(types.DoubleType())) \
            .withColumn('10_year_change_percentage', round(col('10_year_change_percentage') / 100, 3))
        
        df = df.withColumn('time', functions.lit(datetime.strptime(csv.split('.')[-2].split('-')[1], '%b%Y'))) \
            .withColumn('time', col('time').cast(types.DateType()))
        
        f_df = f_df.union(df)

    print(f_df.show(100))
    f_df.coalesce(1).write.option("header",True).csv(output)
    print("Done. Output saved at %s", output)
    
if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Correlate Logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)