import os
import pandas as pd, numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

from dotenv import load_dotenv 
from pymongo import MongoClient
#from pymongo.server_api import ServerApi

#TODO - add a date of when the associations were created
#TODO - move the ' replacement from the pipeline to the tsv parsing script
#TODO - be able to save the output from the association_rules function directly
#       without having to loop through the set again
#TODO - retrieve and use views as opposed to putting a literal value of 1 in the retrieval pipeline
#TODO - support retrieving dates for ordered associations
#TODO - save old associations? is this wanted?

min_support=0.001

# Load config from a .env file:
load_dotenv()
MONGODB = os.environ['MONGODB_URI']
DB = os.environ['DB']
PAGES_BY_ID = os.environ['PAGES_BY_ID']
ASSOCIATIONS = os.environ['ASSOCIATIONS']

client = MongoClient(MONGODB)
# server_api is only needed for MongoDB version 4.9+
#server_api = ServerApi('1')
#client = MongoClient(MONGODB,server_api=server_api)

# database
db = client[DB]
# collection
pageviews = db[PAGES_BY_ID]

pipeline = [ { '$unwind': { 'path': '$pages' } }, 
             { '$project': { 'pages': { '$replaceAll': { 'input': '$pages', 'find': "'", 'replacement': '' } }, 
                             'values': { '$literal': 1 } }
             }
           ]

df = pd.DataFrame(list(pageviews.aggregate(pipeline)))

# Make a pivot table - the format needed for the apriori function later on
pvt = df.pivot_table(index='_id', columns='pages', values='values',
                     fill_value=0, aggfunc=np.sum)

# print(pvt)
def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1

my_data = pvt.applymap(encode_units)

## verify column names
# for col in my_data.columns:
#    print(col)

frequent_itemsets = apriori(my_data, min_support, use_colnames=True)

rules = association_rules(frequent_itemsets, metric="lift", min_threshold=.1)

#pd.set_option("display.max_rows", None, "display.max_columns", None)
#print(rules)
#rules.reset_index(inplace=True)
#print(rules)

data_dict = rules.to_dict('records')

# bson.errors.InvalidDocument: cannot encode object: frozenset({"'/'"}), of type: <class 'frozenset'>
# couldn't find a way to directly save this, got frozenset issues, so here's a super ugly way:
insert_list=[]
# Insert a document for each key:
for index in range(len(data_dict)):
    for key in data_dict[index]:
        if type(data_dict[index][key]) == frozenset:
          data_dict[index][key] = (list(data_dict[index][key])[0])

for index in range(len(data_dict)):
  insert_list+=[data_dict[index]]

associations = db[ASSOCIATIONS]
associations.drop()
associations.insert_many(insert_list)
