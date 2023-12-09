# Running ETL locally
All commands should be executed on the root of the repository.

All required python packages are listed in `requirements.txt`. Use `pip -r requirements.txt` to install the neccessary requirements

## 1.Airbnb listing
`spark-submit ./codes/ab_listing/listing_etl.py ./datasets/inside_airbnb/listing_ud/raw ./output_airbnb` 

This should output the processed dataset in the folder `output_airbnb`

## 2.CMHC
`spark-submit ./codes/cmhc-schl/process_cmhc.py ./datasets/cmhc-schl/by_neighbourhood/raw/average_rent_oct-2022.csv ./datasets/cmhc-schl/by_neighbourhood/raw/median_rent_oct-2022.csv ./datasets/neighbourhood/cmhc_neighbourhood_reference.csv ./output-cmhc`

This should output the processed dataset in the folder `output_cmhc`

# 3.REVBV
`spark-submit ./codes/rebgv/process_rebgv.py ./datasets/rebgv ./output-rebgv`

This should output the processed dataset in the folder `output-rebgv`

This process will take a while since it's using OCR to extract data from PDFs.

# Data storage on AWS S3 bucket
(Publicly accessible)
- S3 ARN: arn:aws:s3:::project-lzy
- AWS Region: US West (Oregon) us-west-2



# ETL on EMR

## 1.Airbnb listing

### clean data
- Input file URI

    `s3://project-lzy/ab_listings/raw/`

- Argument

    `spark-submit --deploy-mode client s3://project-lzy/listing_etl.py s3://project-lzy/ab_listings/raw s3://project-lzy/ab_listings/cleaned`

### ml result
- Input file URI

    `s3://project-lzy/ab_listings/cleaned/`

- Argument

    `spark-submit --deploy-mode client s3://project-lzy/ab_listings/listing_ml.py s3://project-lzy/ab_listings/cleaned/ s3://project-lzy/ab_listings/feature_importance/ s3://project-lzy/ab_listings/review_scores`


## 2.cmhc
- Input file URI

    `s3://project-lzy/cmhc/raw/`

- Argument

    `spark-submit --deploy-mode client s3://project-lzy/cmhc/clean_cmhc.py s3://project-lzy/cmhc/raw s3://project-lzy/cmhc/cleaned`


## 3.rebgv (optional)
- Input file URI

    `s3://project-lzy/rebgv/raw/`

- Argument

    `spark-submit --deploy-mode client s3://project-lzy/rebgv/load_csv.py s3://project-lzy/rebgv/raw s3://project-lzy/rebgv/cleaned`



# Visualization on Flourish
Please use the links below to access all the raw Flourish visualizations. An account might be required for you to copy the project to see the data.

- https://public.flourish.studio/visualisation/16072157/
- https://public.flourish.studio/visualisation/16072161/
- https://public.flourish.studio/visualisation/16072163/

Most of the data are generated using the commands we mentiond above. Some are sepecific to Flourish:

- `python ./codes/ab_listing/geojson_points_to_region.py`: This generates a JSON file which contains the amount of Airbnb listings per neighbourhood.
  - Output file: `datasets/inside_airbnb/listing_used/cleaned/airbnb_neighbourhoods_counter.json`
- Metro Vancouver GeoJson: `datasets/neighbourhood/vancouver_district_and_metro_vancouver_boundaries.geojson`

# Data analysis on Jupyter notebook

## price_line_chart.ipynb
- Input file URI

    `s3://project-lzy/ab_listings/cleaned/`

## listing_analysis.ipynb
- Input file URI
    - Correlation: `s3://project-lzy/ab_listings/review_scores/`
    - Bar chart: `s3://project-lzy/ab_listings/feature_importance/`
    
