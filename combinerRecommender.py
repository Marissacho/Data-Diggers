from logging import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, lit, array, size
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.linalg import DenseVector, SparseVector
from dearpygui import dearpygui as dpg # our UI framework

spark = SparkSession.builder.master("local").appName("Spark_Recommender").getOrCreate()

# df needs to be accessed within def functions to get it to work 
df= spark.read.json("Spark_Sample.jsonl").repartition(20) 
df.printSchema()

total_items = df.count()

# Preprocess data for buy-together recommender (TF-IDF)
print("Preprocessing data for buy-together recommender...")
def preprocess_data(dataframe):
    df_processed = dataframe.withColumn(
        "features",
        when(col("features").isNull(), array()).otherwise(col("features"))
    ).withColumn(
        "description",
        when(col("description").isNull(), array()).otherwise(col("description"))
    ).withColumn(
        "title",
        when(col("title").isNull(), lit("")).otherwise(col("title"))
    ).withColumn(
        "main_category",
        when(col("main_category").isNull(), lit("")).otherwise(col("main_category"))
    ).withColumn(
        "store",
        when(col("store").isNull(), lit("")).otherwise(col("store"))
    ).withColumn(
        "average_rating",
        when(col("average_rating").isNull(), lit(0.0)).otherwise(col("average_rating"))
    )
    df_processed = df_processed.withColumn(
        "features_str",
        when(size(col("features")) > 0, concat_ws(" ", col("features"))).otherwise(lit(""))
    ).withColumn(
        "description_str",
        when(size(col("description")) > 0, concat_ws(" ", col("description"))).otherwise(lit(""))
    )
    df_processed = df_processed.withColumn(
        "combined_text",
        concat_ws(" ", 
            col("title"),
            col("main_category"),
            col("features_str"),
            col("description_str")
        )
    )
    return df_processed

df_processed = preprocess_data(df)
tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
df_tokenized = tokenizer.transform(df_processed)
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_filtered = remover.transform(df_tokenized)
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
df_tf = hashingTF.transform(df_filtered)
idf = IDF(inputCol="rawFeatures", outputCol="tfidf_features")
idfModel = idf.fit(df_tf)
df_tfidf = idfModel.transform(df_tf)
df_tfidf.cache()
print("Data preprocessing completed!") 
   
def store_recommendation(purchased_item_lst,dataframe):
    with dpg.window(label="Items and recommendations",width= 1100,height=800):
        dpg.add_text("Finding items from the same stores as these: \n")
       
        recommendation_list = []
        for i in range(5):

            purchased_item = dataframe.filter(dataframe.item_id == purchased_item_lst[i]).first()

            #print current purchased item along with the store of the item
            title = purchased_item["title"]
            dpg.add_text(f"Item: {title}\n")
            store_name = purchased_item["store"]
            dpg.add_text(f"Store:{store_name}\n\n")
            purchased_item_id = purchased_item["item_id"]

        

            if store_name != None: 
                #filter for items from the same store as purchased items and get it from a random sample of 15% of the data and limit till you find 1 that matches 
                recommended_item= dataframe.filter((dataframe.store == store_name)&(dataframe.item_id != purchased_item_id)).sample(False,0.15).limit(1).first()
                if recommended_item != None:
                    recommendation_list.append(recommended_item)
                
        
        #if some items were added to recommend
        if len(recommendation_list) > 0:
            dpg.add_text("\nSome recommended items from stores you have shopped at are: \n")
            for ri in recommendation_list: 
            
                dpg.add_text(ri["title"])
                recommeneded_item_store = ri["store"]
                dpg.add_text(f"Store: {recommeneded_item_store}" +"\n\n")
        else: 
            dpg.add_text("Sorry no recommended items from stores you have shopped at were found")


def price_recommendation(purchased_item_lst,dataframe):
    with dpg.window(label="Items and recommendations",width= 1100,height=800):

        
        recommendation_list = []
        dpg.add_text("Finding items within $10 as these: \n")
        for i in range(5):

            purchased_item = dataframe.filter(dataframe.item_id == purchased_item_lst[i]).first() #access each purchased item

            #print current purchased item along with price of that item
            dpg.add_text(purchased_item["title"])
            price = purchased_item["price"]
            purchased_item_id = purchased_item["item_id"]

            if purchased_item["price"] != None: 
                dpg.add_text(f"Price: {price}\n\n")
            else: 
                dpg.add_text("Price: N/A\n\n")
            
            #find item withing bounds and add to recommendation list 
            if price != None:
                upperBound_of_price = purchased_item["price"] + 10

                if purchased_item["price"] >= 10:
                    lowerBound_of_price = purchased_item["price"] - 10
                else:
                    lowerBound_of_price= 0
                #filter for an item within the range of upper and lower bound and get it from a random sample of 15% of the data and limit till you find 1 that matches
                recommendation_item = dataframe.filter((dataframe.price <= upperBound_of_price) & (dataframe.price >= lowerBound_of_price)&(dataframe.item_id != purchased_item_id)).sample(False,0.15).limit(1).first()
                recommendation_list.append(recommendation_item)

        #if some items were added to recommend
        if len(recommendation_list) > 0:
            dpg.add_text("\nSome recommened items within 10$ of previous purchases are: \n")
            for recommended_item in recommendation_list: 
            
                dpg.add_text(recommended_item["title"])
                recommeneded_item_price = recommended_item["price"]
                dpg.add_text(f"Price: {recommeneded_item_price}" +"\n\n")
        else: 
            dpg.add_text("Sorry no items were found within 10$ to any of your items or some items did not have their price listed")


def cosine_similarity(vec1, vec2):
    """Calculate cosine similarity between two vectors"""
    if vec1 is None or vec2 is None:
        return 0.0
    try:
        if isinstance(vec1, SparseVector):
            vec1 = DenseVector(vec1.toArray())
        if isinstance(vec2, SparseVector):
            vec2 = DenseVector(vec2.toArray())
        if isinstance(vec1, DenseVector) and isinstance(vec2, DenseVector):
            dot_product = float(vec1.dot(vec2))
            norm1 = float(vec1.norm(2))
            norm2 = float(vec2.norm(2))
            if norm1 == 0.0 or norm2 == 0.0:
                return 0.0
            return dot_product / (norm1 * norm2)
        return 0.0
    except Exception as e:
        return 0.0

def calculate_similarity_score(item1, item2):
    """Calculate weighted similarity score between two items"""
    score = 0.0
    text_sim = cosine_similarity(item1["tfidf_features"], item2["tfidf_features"])
    score += text_sim * 0.4
    if item1["main_category"] == item2["main_category"] and item1["main_category"] != "":
        score += 0.2
    if item1["store"] == item2["store"] and item1["store"] != "":
        score += 0.1
    rating1 = item1["average_rating"] if item1["average_rating"] is not None else 0.0
    rating2 = item2["average_rating"] if item2["average_rating"] is not None else 0.0
    rating_diff = abs(rating1 - rating2)
    rating_sim = 1.0 - (rating_diff / 5.0)
    score += rating_sim * 0.3
    return score

def buy_together_recommender(purchased_item_lst, dataframe_tfidf):
    with dpg.window(label="Items and recommendations",width= 1100,height=800):
        dpg.add_text("Finding items frequently bought together: \n")
        
        # Use the first item from purchased list for buy-together recommendation
        target_item_id = purchased_item_lst[0]
        target_item = dataframe_tfidf.filter(dataframe_tfidf.item_id == target_item_id).first()
        
        if target_item is None:
            dpg.add_text(f"Item ID {target_item_id} not found.")
            return
        
        dpg.add_text(f"Target Item: {target_item['title']}\n")
        dpg.add_text(f"Category: {target_item['main_category']}\n")
        dpg.add_text(f"Store: {target_item['store']}\n")
        dpg.add_text(f"Rating: {target_item['average_rating']}\n\n")
        
        all_items = dataframe_tfidf.filter(dataframe_tfidf.item_id != target_item_id).collect()
        
        recommendations = []
        for item in all_items:
            similarity_score = calculate_similarity_score(target_item, item)
            recommendations.append({
                'item_id': item['item_id'],
                'title': item['title'],
                'main_category': item['main_category'],
                'store': item['store'],
                'average_rating': item['average_rating'],
                'price': item['price'],
                'similarity_score': similarity_score
            })
        
        recommendations.sort(key=lambda x: x['similarity_score'], reverse=True)
        top_recommendations = recommendations[:5]
        
        if len(top_recommendations) > 0:
            dpg.add_text("\nTop 5 Recommended Items to Buy Together: \n")
            for i, rec in enumerate(top_recommendations, 1):
                dpg.add_text(f"{i}. {rec['title']}")
                dpg.add_text(f"   Similarity Score: {rec['similarity_score']:.4f}")
                dpg.add_text(f"   Category: {rec['main_category']}")
                dpg.add_text(f"   Store: {rec['store']}")
                dpg.add_text(f"   Rating: {rec['average_rating']}")
                if rec['price'] is not None:
                    dpg.add_text(f"   Price: ${rec['price']:.2f}\n")
                else:
                    dpg.add_text(f"   Price: N/A\n")
        else:
            dpg.add_text("No recommendations found.")


# NOTE - df needs to not be passed in as arg due to the fact that recommender cannot have args because of how dpg handles args in callback
def Recommender():
    
    rec_type = int(dpg.get_value("recommendation_type"))

    purchased_items = [] # this is where the purchased items are in 
    i = 1
    while i < 6:
        # get user input for the items they have in their purchase history
        
        num = int(dpg.get_value(f"item_{i}")) # get each item from the UI to create the purchased list
      
        purchased_items.append(num) # purchased item list gets item added

        i = i+1
    
    

    #here is where df is needed to be read from outside the function when deciding which recommender to follow
    if rec_type == 1:
        price_recommendation(purchased_items,df)

    elif rec_type == 2:
        buy_together_recommender(purchased_items,df_tfidf)

    elif rec_type == 3:
        store_recommendation(purchased_items,df)




dpg.create_context()
with dpg.window(label="Purchased Items",width= 1100,height=800):
    #Starting window asking for user input 


    dpg.add_text(f"Enter a number between 1-{total_items}")
    dpg.add_input_int(label="item 1" ,tag="item_1",min_value=1,max_value=total_items)
    dpg.add_input_int(label="item 2" ,tag="item_2",min_value=1,max_value=total_items)
    dpg.add_input_int(label="item 3" ,tag="item_3",min_value=1,max_value=total_items)
    dpg.add_input_int(label="item 4" ,tag="item_4",min_value=1,max_value=total_items)
    dpg.add_input_int(label="item 5" ,tag="item_5",min_value=1,max_value=total_items)
    dpg.add_text("Recommendation Types:")
    dpg.add_text("1. Similar Priced items within $10")
    dpg.add_text("2. Frequently Bought Together Items")
    dpg.add_text("3. Items from stores you've shopped from")
    dpg.add_slider_int(label="Recommendation Type",tag="recommendation_type",min_value=1,max_value=3,default_value=1)

    

    dpg.add_button(label="Run Recommender",callback=Recommender)#runs recommender 

#Start and stop GUI along stopping spark
dpg.create_viewport(title="Amazon Fashion Recommender", width= 1100,height=800)
dpg.setup_dearpygui()
dpg.show_viewport()
dpg.start_dearpygui()

spark.stop()
dpg.destroy_context()
dpg.stop_dearpygui()
