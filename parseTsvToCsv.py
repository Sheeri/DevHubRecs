import os, re, csv
from dotenv import load_dotenv 
import pandas as pd

# parse a TSV file downloaded from GA query explorer demo/tools
#             https://ga-dev-tools.appspot.com/query-explorer/


# Load config from a .env file:
load_dotenv()
MONGODB_URI = os.environ['MONGODB_URI']
GA_DATA_FILE = os.environ['GA_DATA_FILE']
DB = os.environ['DB']
PAGES_BY_ID = os.environ['PAGES_BY_ID']

with open(GA_DATA_FILE + '.tsv', 'r',  encoding='iso-8859-1') as myfile:
  with open(GA_DATA_FILE + '.csv', 'w', encoding='iso-8859-1') as csv_file:
    for line in myfile:
      fileContent = re.sub("'",'', re.sub('\000', '', re.sub('\r', '', line) ))
      if re.match(r'/article', fileContent) or re.match(r'/quickstart', fileContent) or re.match(r'/how', fileContent):
          csv_file.write(re.sub('\t',',', fileContent))

