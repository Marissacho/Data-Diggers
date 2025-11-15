from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Spark_Recommender").getOrCreate()

df= spark.read.json("Spark_Sample.jsonl").repartition(20) # read in jsonl where it is partions data so it can run parallel thus decreasing time it take to read data

        
df.printSchema()

total_items = df.count() # gets the total for the range of valid values to find items in

purchased_items = [] # this is where the purchased items are in 
for i in range(5):
    #get user input for the items they have in their purchase history
    num = int(input(f"Enter a number of the item you want to use for the recommender between 1-{total_items}: "))

    while num in purchased_items or num < 1 or num > total_items:
         num = int(input(f"Enter a different number between 1-{total_items}: "))
    
    purchased_items.append(num) # purchased item list gets item added

   
def price_recommendation(purchased_item_lst,dataframe):
    recommendation_list = []
    print("Finding Recommendations For: \n")
    for i in range(5):

        purchased_item = dataframe.filter(dataframe.item_id == purchased_item_lst[i]).first() #access each purchased item

        #print current purchased item along with price of that item
        print(purchased_item["title"])
        price = purchased_item["price"]
        if purchased_item["price"] != None: 
            print(f"Price: {price}\n\n")
        else: 
            print("Price: N/A\n\n")
        
        #find item withing bounds and add to recommendation list 
        if price != None:
            upperBound_of_price = purchased_item["price"] + 10

            if purchased_item["price"] >= 10:
                lowerBound_of_price = purchased_item["price"] - 10
            else:
                lowerBound_of_price= 0
            #filter for an item within the range of upper and lower bound
            recommendation_item = dataframe.filter((dataframe.price <= upperBound_of_price) & (dataframe.price >= lowerBound_of_price)).first()
            recommendation_list.append(recommendation_item)

    #if some items were added to recommend
    if len(recommendation_list) > 0:
        print("\nSome recommened items within 10$ of previous purchases are: \n")
        for recommended_item in recommendation_list: 
        
            print(recommended_item["title"])
            recommeneded_item_price = recommended_item["price"]
            print(f"Price: {recommeneded_item_price}" +"\n\n")
    else: 
        print("Sorry no items were found within 10$ to any of your items")



price_recommendation(purchased_items,df)


# output items to console 


spark.stop()

