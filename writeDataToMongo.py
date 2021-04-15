import os, csv
from collections import defaultdict
from dotenv import load_dotenv 
from pymongo import MongoClient
from pymongo.server_api import ServerApi
# use with python3

#TODO - change data file from env variable to commandline parameter (required)
#TODO - make this an upsert so it updates existing ids if they exist in the collection
#TODO - make the database have the proper unique keys idempotent so duplicate entries are not made
#TODO - add timing, so we know order (e.g. someone reads a beginner article then an advanced one),
#       we want to recommend the advanced article after the beginner but not vice versa.
#TODO - Is there any way to create the documents to insert at the same time as reading the file to make the defaultdict?
#       Currently we loop through the file to make the defaultdict, then loop through the dict to make the insert docs.

# Load config from a .env file:
load_dotenv()
MONGODB_URI = os.environ['MONGODB_URI']
GA_DATA_FILE = os.environ['GA_DATA_FILE'] + '.csv'
DB = os.environ['DB']
PAGES_BY_ID = os.environ['PAGES_BY_ID']

### make a dictionary with a key of userID and array of pages
this_dict = defaultdict(list)
with open(GA_DATA_FILE, 'r+') as f:
  read_me = csv.reader(f, delimiter=",")
  for row in read_me:
    if row[0] not in this_dict[row[1]]:
      this_dict[row[1]].append(row[0])

### save to MongoDB:
# Connect to your MongoDB cluster:
server_api = ServerApi('1')
client = MongoClient(MONGODB_URI,server_api=server_api)

# database
db = client[DB]
# collection
pageviews = db[PAGES_BY_ID]

res = pageviews.find({})
for rec in res:
  for page in rec['pages']:
    if page not in this_dict[rec['_id']]:
      this_dict[rec['_id']].append(page)


insert_list=[]
# Insert a document for each key:
for key in this_dict:
  doc={}
  doc['_id']=key
  doc['pages']=this_dict[key]
  insert_list+=[doc]

pageviews.drop()
pageviews.insert_many(insert_list, ordered=False)
