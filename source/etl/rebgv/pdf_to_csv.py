from nanonets import NANONETSOCR
import glob

model = NANONETSOCR()
model.set_token('6dbb97df-896c-11ee-a76a-12e83ea2166a') # https://app.nanonets.com/#/keys

directory = "../../../datasets"
pdfs = glob.glob("%s/rebgv/pdf/trimed/*" % directory)

for raw_pdf in pdfs:
    print("Processing %s" % raw_pdf.split('/')[-1])
    
    output_name = raw_pdf.split('/')[-1].split('.')[0]
    model.convert_to_csv(raw_pdf, output_file_name = '%s/rebgv/csv/raw/%s.csv' % (directory, output_name))