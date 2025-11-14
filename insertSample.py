
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

from pymongo import MongoClient,UpdateOne #updateOne used to help increase performance when updating things in batches
import json

client = MongoClient("mongodb://localhost:27017/")

db_name = "sample_db"

db = client[db_name]

items = db["items"]

JSON_FILE_NAME = "sample.jsonl" # this will be replaced with the LARGE DATA "meta_Amazon_Fashion.jsonl" 

# we are going to insert in batches to increase performance making it scale better when we do it with our large dataset
size_of_batch = 2000
batch = []
counter = 0

with open(JSON_FILE_NAME) as file: 
    for l in file:  # line of data in file 
        line_of_data = json.loads(l) # read data line by line  
        batch.append(line_of_data) #add to batch
        counter += 1

        if counter == size_of_batch:
            items.insert_many(batch) #inserts batch of data into db
            batch = [] #empty the batch and reset counter
            counter = 0 

if len(batch) > 0: # if the batch had less items than the size of batch and were not able to get inserted 
    items.insert_many(batch)



all_items = items.count_documents({}) 

#test if all sample documents are stored in db
if all_items == 6000:
    print("Test 1 passed")
else:
    raise ImportError ("sample.json did not import successfully")

#drop the videos and images feilds
items.update_many({}, {"$unset" : {"videos" : ""}}) 
items.update_many({}, {"$unset" : {"images" : ""}})
items.update_many({}, {"$unset" : {"details" : ""}})


#We need to assign a unique item_id to each item in an efficent way
update_batch = []
counter = 1

for doc in items.find({}, {"_id": 1}): # only get the id instead of the entire item to increase perfomance

    #here we create a batch of updates to add a unique id to each of the items starting at 1 - total num of items 
    update_batch.append(UpdateOne({"_id" : doc["_id"]} , {"$set"  :{"item_id" : counter}} )) #we access each document by id 
    counter += 1 

    if len(update_batch) == size_of_batch: # if there is a full update batch
        items.bulk_write(update_batch) #write all the updates
        update_batch = [] #reset the batch of updates 



if len(update_batch) > 0:  # if some updates not on the db because not at the size mark
    items.bulk_write(update_batch) # write the last updates 


with open("Spark_Sample.jsonl", "w") as file:
    for doc in items.find():
        doc.pop("_id", None)# makes it so it does not write the id given by mongodb otherwise we get "TypeError: Object of type ObjectId is not JSON serializable"
        file.write(json.dumps(doc) + "\n")


client.drop_database(db_name) # leave this while testing so duplicates do not get added 
