from tabula import read_pdf
from tabula import read_pdf_with_template
from tabula import convert_into
#df = read_pdf("Roll_Call_5.pdf", pages="all",stream=True)
'''
convert_into("Sample_input_table_data/Roll_Call_5.pdf", "Sample_output_table_data/Roll_Call_5_nostream.csv", output_format="csv", pages='all',lattice = True,stream=False)
'''
df = read_pdf_with_template("Sample_input_table_data/Roll_Call_5.pdf","Sample_input_table_data/template_5.csv", pages='all',lattice = True)

df.read()
#1->2-5
#3->2-6
