from logging import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dearpygui import dearpygui as dpg # our UI framework

spark = SparkSession.builder.master("local").appName("Spark_Recommender").getOrCreate()

df= spark.read.json("Spark_Sample.jsonl").repartition(20) 
df.printSchema()

total_items = df.count() 

purchased_items = [] # this is where the purchased items are in 
for i in range(5):
    #get user input for the items they have in their purchase history
    num = int(input(f"Enter a number of the item you want to use for the recommender between 1-{total_items}: "))

    while num in purchased_items or num < 1 or num > total_items:
         num = int(input(f"Enter a different number between 1-{total_items}: "))
    
    purchased_items.append(num) # purchased item list gets item added

   
def store_recommendation(purchased_item_lst,dataframe):
    print("Finding Store Recommendations: \n")
    #dict to track the stores score
    store = set()
    recommendation_list = []
    for i in range(5):

        purchased_item = dataframe.filter(dataframe.item_id == purchased_item_lst[i]).first()

        #print current purchased item along with the store of the item
        title = purchased_item["title"]
        print(f"Item: {title}\n")
        store_name = purchased_item["store"]
        print (f"Store:{store_name}\n\n")
        purchased_item_id = purchased_item["item_id"]

       

        if store_name != None: 
            recommended_item= dataframe.filter((dataframe.store == store_name)&(dataframe.item_id != purchased_item_id)).first()
            if recommended_item != None:
                recommendation_list.append(recommended_item)
            
    
    #if some items were added to recommend
    if len(recommendation_list) > 0:
        print("\nSome recommended items from stores you have shopped at are: \n")
        for ri in recommendation_list: 
        
            print(ri["title"])
            recommeneded_item_store = ri["store"]
            print(f"Store: {recommeneded_item_store}" +"\n\n")
    else: 
        print("Sorry no recommended items from stores you have shopped at were found")


def price_recommendation(purchased_item_lst,dataframe):
    recommendation_list = []
    print("Finding Recommendations For: \n")
    for i in range(5):

        purchased_item = dataframe.filter(dataframe.item_id == purchased_item_lst[i]).first() #access each purchased item

        #print current purchased item along with price of that item
        print(purchased_item["title"])
        price = purchased_item["price"]
        purchased_item_id = purchased_item["item_id"]

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
            recommendation_item = dataframe.filter((dataframe.price <= upperBound_of_price) & (dataframe.price >= lowerBound_of_price)&(dataframe.item_id != purchased_item_id)).first()
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




def Recommender(purchased_items_lst,df):
    print("Choose your recommendation type:")
    print("1 = Price Recommendation")
    print("2 = Frequently Bought Together Recommendation")
    print("3 = Store Recommendation")

    choice = int(input("Enter a number 1-3:"))

    while choice not in [1,2,3]:
        print("Please enter a valid input")

    if choice == 1:
        price_recommendation(purchased_items_lst,df)

    #elif choice == 2:
    #buy_together_recommender(purchased_items,df)

    elif choice == 3:
        store_recommendation(purchased_items_lst,df)



Recommender(purchased_items,df)

spark.stop()
