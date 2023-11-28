import csv

input_csv_file = 'average_rent_oct-2022.csv'  
output_csv_file = 'average_rent_oct-2022_dropped.csv' 

with open(input_csv_file, 'r', newline='', encoding='latin-1') as file:
    reader = csv.reader(file)
    rows = list(reader)


modified_rows = rows[2:-9] if len(rows) > 11 else []


with open(output_csv_file, 'w', newline='', encoding='latin-1') as file:
    writer = csv.writer(file)
    writer.writerows(modified_rows)
