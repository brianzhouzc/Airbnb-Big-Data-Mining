# Run with following command:
# python make_schema.py listing/listings.csv.gz /Users/hongyingyue/Desktop/CMPT732_Lab1/Project/inside_airbnb/vancouver/Inside_Airbnb_Data_Dictionary.xlsx

import gzip
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import pandas as pd


gzip_file_path = sys.argv[1] # path of listings.csv.gz file folder
col_dict_path = sys.argv[2]  # path of Inside_Airbnb_Data_Dictionary.xlsx file

# Get the column name lines
with gzip.open(gzip_file_path, 'rt', encoding='utf-8') as file:
    col_names = file.readline()
    
# print("First row:", first_line)
# print(type(col_names))  # type: str

colNames = col_names.split(',')
print(len(colNames))


col_dict = pd.read_excel(col_dict_path, sheet_name='listings.csv detail v4.3', skiprows=7).iloc[:-4]


# Define a function to match Spark column types
def define_type(col):
    type_dict= {
        'integer': 'types.IntegerType()',
        'text': 'types.StringType()',   
        'numeric': 'types.DoubleType()',
        'date': 'types.DateType()',
        'boolean [t=true; f=false]': 'types.BooleanType()',
        'boolean':'types.BooleanType()',
        'bigint': 'types.LongType()',
        'datetime':'types.TimestampType()',
        'string':'types.StringType()',
        'json':'types.StringType()',
        'currency': 'types.StringType()',
    }
    return type_dict.get(col, 'types.StringType()')
col_dict['Type_spark'] = col_dict['Type'].apply(define_type)

col_dict['Spark_schema'] = 'types.StructField("' + col_dict['Field']+'",'+ col_dict['Type_spark']+')'
# col_dict['Spark_schema'].to_csv('spark_schema.csv', index=False)
print(', '.join(col_dict['Spark_schema'].tolist()))

# col_dict output is used to insert into the schema defination block as below
# observation_schema = types.StructType([
#     types.StructField('station', types.StringType()),
#     types.StructField('date', types.StringType()),
# ])
