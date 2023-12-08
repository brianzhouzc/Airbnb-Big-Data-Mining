"""
Terminal command:
    ${SPARK_HOME}/bin/spark-submit listing_etl.py listing output
"""

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import regexp_replace, col, when
from pyspark import SparkConf, SparkContext


def main(inputs, output):

    listing_schema = types.StructType([
        types.StructField("id",types.StringType()), 
        # types.StructField("listing_url",types.StringType()), 
        types.StructField("scrape_id",types.LongType()), 
        types.StructField("last_scraped",types.DateType()), 
        types.StructField("source",types.StringType()), 
        types.StructField("name",types.StringType()), 
        types.StructField("description",types.StringType()),
        types.StructField("neighborhood_overview",types.StringType()), 
        # types.StructField("picture_url",types.StringType()), 
        types.StructField("host_id",types.IntegerType()), 
        # types.StructField("host_url",types.StringType()), 
        types.StructField("host_name",types.StringType()), 
        types.StructField("host_since",types.DateType()), 
        types.StructField("host_location",types.StringType()), 
        types.StructField("host_about",types.StringType()), 
        types.StructField("host_response_time",types.StringType()), 
        types.StructField("host_response_rate",types.StringType()), 
        types.StructField("host_acceptance_rate",types.StringType()), 
        types.StructField("host_is_superhost",types.StringType()), 
        # types.StructField("host_thumbnail_url",types.StringType()), 
        # types.StructField("host_picture_url",types.StringType()), 
        types.StructField("host_neighbourhood",types.StringType()), 
        types.StructField("host_listings_count",types.StringType()), 
        types.StructField("host_total_listings_count",types.StringType()), 
        types.StructField("host_verifications",types.StringType()), 
        # types.StructField("host_has_profile_pic",types.StringType()), 
        types.StructField("host_identity_verified",types.StringType()), 
        types.StructField("neighbourhood",types.StringType()), 
        types.StructField("neighbourhood_cleansed",types.StringType()), 
        types.StructField("neighbourhood_group_cleansed",types.StringType()), 
        types.StructField("latitude",types.DoubleType()), 
        types.StructField("longitude",types.DoubleType()), 
        types.StructField("property_type",types.StringType()), 
        types.StructField("room_type",types.StringType()), 
        types.StructField("accommodates",types.IntegerType()), 
        types.StructField("bathrooms",types.DoubleType()), 
        types.StructField("bathrooms_text",types.StringType()), 
        types.StructField("bedrooms",types.IntegerType()), 
        types.StructField("beds",types.IntegerType()), 
        # types.StructField("amenities",types.StringType()), 
        types.StructField("price",types.StringType()), 
        types.StructField("minimum_nights",types.IntegerType()), 
        types.StructField("maximum_nights",types.IntegerType()), 
        types.StructField("minimum_minimum_nights",types.IntegerType()), 
        types.StructField("maximum_minimum_nights",types.IntegerType()), 
        types.StructField("minimum_maximum_nights",types.IntegerType()), 
        types.StructField("maximum_maximum_nights",types.IntegerType()), 
        types.StructField("minimum_nights_avg_ntm",types.DoubleType()), 
        types.StructField("maximum_nights_avg_ntm",types.DoubleType()), 
        types.StructField("calendar_updated",types.DateType()), 
        types.StructField("has_availability",types.StringType()), 
        types.StructField("availability_30",types.IntegerType()), 
        types.StructField("availability_60",types.IntegerType()), 
        types.StructField("availability_90",types.IntegerType()), 
        types.StructField("availability_365",types.IntegerType()), 
        types.StructField("calendar_last_scraped",types.DateType()), 
        types.StructField("number_of_reviews",types.IntegerType()), 
        types.StructField("number_of_reviews_ltm",types.IntegerType()), 
        types.StructField("number_of_reviews_l30d",types.IntegerType()), 
        types.StructField("first_review",types.DateType()), 
        types.StructField("last_review",types.DateType()), 
        types.StructField("review_scores_rating",types.StringType()), 
        types.StructField("review_scores_accuracy",types.StringType()), 
        types.StructField("review_scores_cleanliness",types.StringType()), 
        types.StructField("review_scores_checkin",types.StringType()), 
        types.StructField("review_scores_communication",types.StringType()), 
        types.StructField("review_scores_location",types.StringType()), 
        types.StructField("review_scores_value",types.StringType()), 
        types.StructField("license",types.StringType()), 
        types.StructField("instant_bookable",types.StringType()), 
        types.StructField("calculated_host_listings_count",types.IntegerType()), 
        types.StructField("calculated_host_listings_count_entire_homes",types.IntegerType()), 
        types.StructField("calculated_host_listings_count_private_rooms",types.IntegerType()), 
        types.StructField("calculated_host_listings_count_shared_rooms",types.IntegerType()), 
        types.StructField("reviews_per_month",types.DoubleType())        
        ])    

    listings_df = spark.read.option('multiLine', 'True').option('escape', '\"').\
        option("mode", "PERMISSIVE").csv(inputs, inferSchema=True , header=True)
    
    ## Leave out columns of url or pic
    drop_columns = [col for col in listings_df.columns if ('url' in col or 'pic' in col)]
    listings_df = listings_df.drop(*drop_columns)
    
    ## Drop text columns
    listings_df = listings_df.drop(*['description', 'neighborhood_overview','amenities', 'host_about'])
    
    ## Drop all null of copied columns
    noc_cols = ['neighbourhood_group_cleansed', 'bathrooms', 'calendar_updated','calendar_last_scraped' ]
    listings_df = listings_df.drop(*noc_cols).na.fill(value=-1).na.fill(value='Null')
    
    ## Change data type of 'price'
    listings_df = listings_df.withColumn('price', regexp_replace('price', '\$*\,*\s*', '')) \
            .withColumn('price', col('price').cast(types.IntegerType()))

    listings_use = listings_df.cache()
    listings_use.coalesce(1).write.mode('overwrite')\
        .option("header",True).csv(output)
    # print(listings_use.columns)
    # print(listings_use.schema)
    
    ## Check input and output shapes
    num_rows = listings_use.count()
    num_columns = len(listings_use.columns)
    print('Output Shape:', num_rows, num_columns)
    
    check = spark.read.csv(output, header=True)   
    num_rows = check.count()
    num_columns = len(check.columns)
    print('Check Shape:', num_rows, num_columns)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('airbnb listing ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)
    sc = spark.sparkContext
    main(inputs, output)
