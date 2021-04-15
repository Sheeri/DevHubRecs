import os, csv, re
from collections import defaultdict
from dotenv import load_dotenv 
from pymongo import MongoClient
from pymongo.server_api import ServerApi
# use with python3

# Load config from a .env file:
load_dotenv()
SOURCE_MONGODB = os.environ['MONGODB_URI']
SOURCE_DB = os.environ['DB']
TARGET_MONGODB = os.environ['MONGODB_URI2']
TARGET_DB = os.environ['DB2']
GA_DATA_FILE = os.environ['GA_DATA_FILE']
ASSOCIATIONS = os.environ['ASSOCIATIONS']
RELATED = os.environ['RELATED']

#TODO - get rid of match clause here, we aren't even saving / and /learn in pageviews
#TODO - make sure we get rid of ?page= in the parsing, then remove that from the match clause

### get associations from MongoDB:
# Connect to your MongoDB cluster:
server_api = ServerApi('1')
client = MongoClient(SOURCE_MONGODB,server_api=server_api)

# database
db = client[SOURCE_DB]
# collection
assoc = db[ASSOCIATIONS]

pipeline = [
    {
        '$match': {
            '$and': [
                { 'antecedents': { '$not': re.compile(r"^.$") }
                }, { 'consequents': { '$not': re.compile(r"^.$") }
                }, { 'antecedents': { '$not': re.compile(r"^.learn") }
                }, { 'consequents': { '$not': re.compile(r"^.learn") }
                }, { 'antecedents': { '$not': re.compile(r"\\?page=") }
                }, { 'consequents': { '$not': re.compile(r"\\?page=") }
                } ]
        }
    }, {
        '$group': {
            '_id': '$antecedents', 
            'related': { '$addToSet': '$consequents' }
        }
    }
]

results = assoc.aggregate(pipeline)
insert_list=[]
for record in results:
   insert_list+=[record]

client = MongoClient(TARGET_MONGODB)
#client = MongoClient(TARGET_MONGODB,server_api=server_api)

# database
db = client[TARGET_DB]
# collection
related = db['skunkworks2']

related.drop()
related.insert_many(insert_list)

