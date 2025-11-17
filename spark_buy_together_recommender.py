from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, lit, array, size
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.linalg import DenseVector, SparseVector
import sys

# Create SparkSession
spark = SparkSession.builder.master("local").appName("Buy_Together_Recommender").getOrCreate()

df = spark.read.json("Spark_Sample.jsonl").repartition(20)  

df.printSchema()
total_items = df.count()
print(f"Total items in dataset: {total_items}")

# Data Preprocessing: Combine text fields and handle nulls
def preprocess_data(dataframe):
    """
    Preprocess the dataframe by combining text fields and handling null values
    """
    # Handle null values in arrays and strings
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
    
    # Combine text fields: title + features + description
    # Convert arrays to strings by joining with space
    df_processed = df_processed.withColumn(
        "features_str",
        when(size(col("features")) > 0, concat_ws(" ", col("features"))).otherwise(lit(""))
    ).withColumn(
        "description_str",
        when(size(col("description")) > 0, concat_ws(" ", col("description"))).otherwise(lit(""))
    )
    
    # Combine all text into one field
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

# Preprocess the dataframe
df_processed = preprocess_data(df)

# Calculate TF-IDF for text similarity
print("Calculating TF-IDF vectors for text similarity...")
tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
df_tokenized = tokenizer.transform(df_processed)

# Remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_filtered = remover.transform(df_tokenized)

# Calculate TF (Term Frequency) using HashingTF
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
df_tf = hashingTF.transform(df_filtered)

# Calculate IDF
idf = IDF(inputCol="rawFeatures", outputCol="tfidf_features")
idfModel = idf.fit(df_tf)
df_tfidf = idfModel.transform(df_tf)

# Cache the processed dataframe for efficient access
df_tfidf.cache()

print("Data preprocessing completed!")

# Cosine similarity function
def cosine_similarity(vec1, vec2):
    """Calculate cosine similarity between two vectors"""
    if vec1 is None or vec2 is None:
        return 0.0
    try:
        # Convert to DenseVector if needed for easier calculation
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
    """
    Calculate weighted similarity score between two items
    Components:
    1. Text similarity (TF-IDF cosine similarity) - weight: 0.4
    2. Category match - weight: 0.2
    3. Store match - weight: 0.1
    4. Rating similarity - weight: 0.3
    """
    score = 0.0
    
    # 1. Text similarity (TF-IDF cosine similarity) - 40% weight
    text_sim = cosine_similarity(item1["tfidf_features"], item2["tfidf_features"])
    score += text_sim * 0.4
    
    # 2. Category match - 20% weight
    if item1["main_category"] == item2["main_category"] and item1["main_category"] != "":
        score += 0.2
    
    # 3. Store match - 10% weight
    if item1["store"] == item2["store"] and item1["store"] != "":
        score += 0.1
    
    # 4. Rating similarity - 30% weight
    # Normalize rating difference (ratings are 0-5, so max difference is 5)
    rating1 = item1["average_rating"] if item1["average_rating"] is not None else 0.0
    rating2 = item2["average_rating"] if item2["average_rating"] is not None else 0.0
    rating_diff = abs(rating1 - rating2)
    rating_sim = 1.0 - (rating_diff / 5.0)  # Normalize to 0-1
    score += rating_sim * 0.3
    
    return score

def get_recommendations(item_id, dataframe, top_n=5):
    """
    Get top N similar items for a given item_id
    """
    # Get the target item
    target_item = dataframe.filter(dataframe.item_id == item_id).first()
    
    if target_item is None:
        return []
    
    print(f"\nFinding recommendations for item: {target_item['title']}")
    print(f"Category: {target_item['main_category']}")
    print(f"Store: {target_item['store']}")
    print(f"Rating: {target_item['average_rating']}\n")
    
    # Collect all items (excluding the target item) for comparison
    all_items = dataframe.filter(dataframe.item_id != item_id).collect()
    
    # Calculate similarity scores
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
    
    # Sort by similarity score (descending) and return top N
    recommendations.sort(key=lambda x: x['similarity_score'], reverse=True)
    top_recommendations = recommendations[:top_n]
    
    # Ensure we return exactly top_n items if available
    if len(top_recommendations) < top_n and len(recommendations) > 0:
        print(f"Warning: Only {len(top_recommendations)} recommendations found (requested {top_n})")
    
    return top_recommendations

def display_recommendations(recommendations):
    """
    Display recommendations in a formatted way - always shows top 5 if available
    """
    if len(recommendations) == 0:
        print("No recommendations found.")
        return
    
    num_recommendations = len(recommendations)
    print(f"\n{'='*80}")
    print(f"TOP {num_recommendations} RECOMMENDED ITEMS TO BUY TOGETHER:")
    print(f"{'='*80}\n")
    
    # Display all recommendations (should be 5, but display whatever we have)
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec['title']}")
        print(f"   Similarity Score: {rec['similarity_score']:.4f}")
        print(f"   Category: {rec['main_category']}")
        print(f"   Store: {rec['store']}")
        print(f"   Rating: {rec['average_rating']}")
        if rec['price'] is not None:
            print(f"   Price: ${rec['price']:.2f}")
        else:
            print(f"   Price: N/A")
        print()
    
    # Confirm we got the expected number
    if num_recommendations < 5:
        print(f"Note: Only {num_recommendations} recommendations available (requested 5)")

# User interface
print("\n" + "="*80)
print("BUY TOGETHER ITEM RECOMMENDER")
print("="*80)
print(f"Enter an item_id between 1 and {total_items} to get recommendations")
print("="*80 + "\n")

# Check if item_id was provided as command-line argument
if len(sys.argv) > 1:
    try:
        item_id = int(sys.argv[1])
        if item_id < 1 or item_id > total_items:
            print(f"Error: item_id must be between 1 and {total_items}")
            sys.exit(1)
        print(f"Using item_id from command line: {item_id}\n")
    except ValueError:
        print("Error: item_id must be a number")
        sys.exit(1)
else:
    item_id = int(input(f"Enter item_id (1-{total_items}): "))
    while item_id < 1 or item_id > total_items:
        item_id = int(input(f"Please enter a valid item_id between 1 and {total_items}: "))

# Get recommendations
recommendations = get_recommendations(item_id, df_tfidf, top_n=5)

# Display recommendations
display_recommendations(recommendations)

# Stop Spark session
spark.stop()

