from PyPDF2 import PdfWriter, PdfReader
import glob

directory = "../../../datasets"

pdfs = glob.glob("%s/rebgv/pdf/raw/*" % directory)

for raw_pdf in pdfs:
    print("Processing %s", raw_pdf.split('/')[-1])
    
    output_name = raw_pdf.split('/')[-1].split('.')[0]
    output_name = "rebgv-%s%s" % (output_name.split('-')[3], output_name.split('-')[4])
    inputpdf = PdfReader(open(raw_pdf, "rb"))

    output = PdfWriter()
    output.add_page(inputpdf.pages[len(inputpdf.pages) - 6])
    output.add_page(inputpdf.pages[len(inputpdf.pages) - 5])
    with open("%s/trimed/%s.pdf" % (directory, output_name), "wb") as outputStream:
        output.write(outputStream)