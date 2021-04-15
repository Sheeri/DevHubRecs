import os, csv, re
from collections import defaultdict
from dotenv import load_dotenv 
from pymongo import MongoClient
from pymongo.server_api import ServerApi
# use with python3

# Load config from a .env file:
load_dotenv()
MONGODB = os.environ['MONGODB_URI']
DB = os.environ['DB']
RELATED = os.environ['RELATED']
MANUAL = os.environ['MANUAL']

### get associations from MongoDB:
# Connect to your MongoDB cluster:
client = MongoClient(MONGODB)
#server_api = ServerApi('1')
#client = MongoClient(MONGODB,server_api=server_api)

# database
db = client[DB]
# collection
manual = db['manual']
pipeline = [ { '$lookup': {
            'from': 'related', 
            'localField': 'url', 
            'foreignField': '_id', 
            'as': 'other' }
    }, { '$unwind': { 'path': '$other' }
    }, { '$addFields': {
            'diff': { '$setDifference': [ '$other.related', '$series_related_links' ] } }
    }, { '$match': { 'diff': { '$ne': [] } }
    }, { '$project': { 'url': 1, 'diff': 1, '_id': 0 }
    } ]

print("<HTML><BODY><TABLE BORDER=1><TH>Folks who read:</TH><TH>Also read:</TH>")
results = manual.aggregate(pipeline)
for record in results:
  print('<TR>')
  print('<TD>' + record['url'] + '</TD><TD>')
  diff=record['diff']
  for newUrl in diff:
    print(newUrl + '<BR>')
  print('</TD></TR>')

print("</TABLE></BODY></HTML>")
