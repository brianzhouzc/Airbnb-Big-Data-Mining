"""
Terminal command:
    ${SPARK_HOME}/bin/spark-submit listing_ml.py output
"""

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
spark = SparkSession.builder.appName('listing review train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0' # make sure we have Spark 3.0+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def main(inputs):
    
    df = spark.read.json(inputs,
                            mode="PERMISSIVE",
                            columnNameOfCorruptRecord="_corrupt_record",
                            dateFormat="yyyy-MM-dd",)

    # choose data
    dt_cols = ['review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness', 
            'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 	
            'review_scores_value', 'price', 'room_type']
    data = df.filter((df['last_scraped'] == '2023-09-06') & (df['number_of_reviews'] > 0)).select(dt_cols)
    
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    stringIndexer = StringIndexer(inputCol="room_type", outputCol="room_type_i", stringOrderType="alphabetDesc")
    featureAssembler = VectorAssembler(inputCols=["room_type_i", 'review_scores_accuracy', 'review_scores_cleanliness', 
                                                'review_scores_checkin', 'review_scores_communication', 
                                                'review_scores_location', 'review_scores_value',
                                                'price'],
                                       outputCol="features")
    gbt_regressor = GBTRegressor(featuresCol="features", labelCol="review_scores_rating", maxIter=10)
    pipeline = Pipeline(stages=[stringIndexer, featureAssembler, gbt_regressor])
    model = pipeline.fit(train)    
    predictions = model.transform(validation)

    evaluator_r2 = RegressionEvaluator(labelCol="review_scores_rating", predictionCol="prediction", metricName="r2")
    score_r2 = evaluator_r2.evaluate(predictions)
    print('Validation score for DecisionTree model: (R square)%g' % (score_r2)) #(R square)0.719047
    
    # match featureImportances with feature names
    importances = model.stages[-1].featureImportances
    original_feature_columns = featureAssembler.getInputCols()
    feature_importance_dict = dict(zip(original_feature_columns, importances.toArray()))

    for feature, importance in feature_importance_dict.items():
       print(f"Feature: {feature}, Importance: {importance}")
    
    fi_schema = types.StructType([
                types.StructField('feature', types.StringType()),
                types.StructField('importance', types.DoubleType()),
            ])
    rows = [types.Row(feature=feature, importance=float(importance)) for feature, importance in feature_importance_dict.items()]
    df = spark.createDataFrame(rows, schema=fi_schema)
    df.coalesce(1).write.csv('listing_importance', header=True, mode="overwrite")
      
    data.write.csv('listing_ml', header=True, mode="overwrite")

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
