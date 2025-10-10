
'''
Dataset Citation: 

@article{hou2024bridging,
  title={Bridging Language and Items for Retrieval and Recommendation},
  author={Hou, Yupeng and Li, Jiacheng and He, Zhankui and Yan, An and Chen, Xiusi and McAuley, Julian},
  journal={arXiv preprint arXiv:2403.03952},
  year={2024}
}
'''

'''
For Using MongoDB with brew
brew services start mongodb-community - starts db
brew services stop mongodb-community - stops db
'''
from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017/")

db_name = "sample_db"

db = client[db_name]

items = db["items"]

with open("sample.jsonl") as file: 
    for l in file:  # line of data in file 
        line_of_data = json.loads(l) # read data line by line  
        items.insert_one(line_of_data) #inserts line of data into db 


all_items = items.count_documents({}) 

#test if all sample documents are stored in db
if all_items == 6000:
    print("Test 1 passed")
else:
    raise ImportError ("sample.json did not import successfully")

#drop the videos and images feilds
items.update_many({}, {"$unset" : {"videos" : ""}}) 
items.update_many({}, {"$unset" : {"images" : ""}})


client.drop_database(db_name) # leave this while testing so duplicates do not get added 