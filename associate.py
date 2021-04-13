import os
import pandas as pd, numpy as np
from collections import defaultdict
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

from dotenv import load_dotenv 
from pymongo import MongoClient
from pymongo.server_api import ServerApi

#TODO - make the collection have unique indexes and use insert_many with ordered false so this script is idempotent

min_support=0.001

# Load config from a .env file:
load_dotenv()
MONGODB_URI = os.environ['MONGODB_URI']
DB = os.environ['DB']
PAGES_BY_ID = os.environ['PAGES_BY_ID']
ASSOCIATIONS = os.environ['ASSOCIATIONS']

# Connect to your MongoDB cluster:
server_api = ServerApi('1')
client = MongoClient(MONGODB_URI,server_api=server_api)

# database
db = client[DB]
# collection
pageviews = db[PAGES_BY_ID]

# pipeline = [ { '$unwind': { 'path': '$pages' } }, { '$match': { '_id': '1056937' } } ]
pipeline = [ { '$unwind': { 'path': '$pages' } }, { '$addFields': { 'values': 1 } } ]

## verification
#results = pageviews.aggregate(pipeline)
#for row in results:
#   print("{userID} {pages}".format(
#         userID=row["_id"],
#         pages=row["pages"],
#   ))

df = pd.DataFrame(list(pageviews.aggregate(pipeline)))
# restructure dataframe via pivot_table
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
# print(frequent_itemsets)

rules = association_rules(frequent_itemsets, metric="lift", min_threshold=.1)
#pd.set_option("display.max_rows", None, "display.max_columns", None)

#print(rules)
#rules.reset_index(inplace=True)
#print(rules)

data_dict = rules.to_dict('records')
#print(data_dict)

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
associations.insert_many(insert_list)
