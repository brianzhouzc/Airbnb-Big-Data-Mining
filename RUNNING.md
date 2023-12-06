# Data storage on AWS S3 budget
(Publicly accessible)
- S3 ARN: arn:aws:s3:::project-lzy
- AWS Region: US West (Oregon) us-west-2



# ETL on EMR

## 1.Airbnb listing

### clean data
- Input file URI
    s3://project-lzy/ab_listings/raw/

- Argument
    spark-submit --deploy-mode client s3://project-lzy/listing_etl.py s3://project-lzy/ab_listings/raw s3://project-lzy/ab_listings/cleaned

### ml result
- Input file URI
    s3://project-lzy/ab_listings/cleaned/

- Argument
    spark-submit --deploy-mode client s3://project-lzy/ab_listings/listing_ml.py s3://project-lzy/ab_listings/cleaned/ s3://project-lzy/ab_listings/feature_importance/ s3://project-lzy/ab_listings/review_scores


## 2.cmhc
- Input file URI
    s3://project-lzy/cmhc/raw/

- Argument    
    spark-submit --deploy-mode client s3://project-lzy/cmhc/clean_cmhc.py s3://project-lzy/cmhc/raw s3://project-lzy/cmhc/cleaned


## 3.rebgv (optional)
- Input file URI
    s3://project-lzy/rebgv/raw/

- Argument
    spark-submit --deploy-mode client s3://project-lzy/rebgv/load_csv.py s3://project-lzy/rebgv/raw s3://project-lzy/rebgv/cleaned



# Visualization on Flourish - TBD
- Input file URI
    s3://project-lzy/ab_listings/cleaned/
    s3://project-lzy/cmhc/cleaned/

- Geographical input


# Data analysis on Jupyter notebook

## price_line_chart.ipynb
- Input file URI
    s3://project-lzy/ab_listings/cleaned/

## listing_analysis.ipynb
- Input file URI
    - Correlation: s3://project-lzy/ab_listings/review_scores/
    - Bar chart: s3://project-lzy/ab_listings/feature_importance/
    
