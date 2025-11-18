from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.master("local").appName("Spark_Recommender").getOrCreate()

df= spark.read.json("Spark_Sample.jsonl").repartition(20) 
df.printSchema()

total_items = df.count() 

#gets purchased items from the user
purchased_items = [] 
for i in range(5):

    num = int(input(f"Enter a number of the item you want to use for the recommender between 1-{total_items}: "))

    while num in purchased_items or num < 1 or num > total_items:
         num = int(input(f"Enter a different number between 1-{total_items}: "))
    
    purchased_items.append(num) 

   
def store_recommendation(purchased_item,dataframe):
    print("Finding Store Recommendations: \n")
    #dict to track the stores score
    store = set()

    for item_id in purchased_item:

        purchased_item = dataframe.filter(dataframe.item_id == purchased_item).first()#access each purchased item

        if not purchased_item:
            continue

        #print current purchased item along with the store of the item
        title = purchased_item["title"]
        print(f"Item: {title}")
        store_name = purchased_item["store"]
        print (f"Store:{store_name}")
        category = purchased_item["main_category"]
        print(f"Category: {category}")

        store.add(store_name)

        print("recommended stores based on your purchase history: ")

        for s in store:
            print(s)

store_recommendation(purchased_items,df)


# output items to console 
spark.stop()
