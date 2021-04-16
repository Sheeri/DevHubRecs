import os, re
from dotenv import load_dotenv 
from pymongo import MongoClient
#from pymongo.server_api import ServerApi
# use with python3

# probably you don't have a different source and target but skunkworks did
# so you probably want to change this
# Load config from a .env file:
load_dotenv()
SOURCE_MONGODB = os.environ['MONGODB_URI']
SOURCE_DB = os.environ['DB']
TARGET_MONGODB = os.environ['MONGODB_URI2']
TARGET_DB = os.environ['DB2']
ASSOCIATIONS = os.environ['ASSOCIATIONS']
RELATED = os.environ['RELATED']

#TODO - get rid of match clause here, we aren't even saving / and /learn in pageviews
#TODO - make sure we get rid of ?page= in the parsing, then remove that from the match clause

### get associations from MongoDB:
client = MongoClient(MONGODB)
# server_api is only needed for MongoDB version 4.9+
#server_api = ServerApi('1')
#client = MongoClient(MONGODB,server_api=server_api)

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
related = db[RELATED]

related.drop()
related.insert_many(insert_list)

