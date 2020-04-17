# Parse Opticial character recognition based pdfs

This project involves identifying tables in OCR pdfs and extracting them. The documents used in this project are from 1920-1930's and OCR converted data are not quite accurate same words are often recognized with different words.
So a lot of approximations have been made and manual preparation of data have been carried out. 
Extracting tables and merging them from inaccurate OCR data using Fuzzy String search algorithm to best approximate words.

### Libraries required:
```
pdfquery_utils
PyPDF2
fuzzywuzzy
pandas
tabula
logging
```

## How to run it?

### Step 1:

Run
```
Step1_Identify_Tables.ipynb
```
 to identify and verify the table locations present in the pdfs. 

### Step 2:

Once the verification is carried out, Run 
```
Step2_Split_PDFS_with_tables.ipynb
```
 to extract all the tables identified in step 1.

### Step 3:

Manually prepare unstructured csv files by copying them from pdfs into the csv files. Then with the csv files run the following file
```
Step3_Create_table.ipynb
```
This creates table for each file in original structured pdf format.

### Step 4:
Run
```
Step3_Create_table.ipynb
```
to merge all the tables together to create the final output.


## Other Files

Following code will help in creating 
```
Cleanup_votes.ipynb
```
