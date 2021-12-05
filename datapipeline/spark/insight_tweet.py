from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, sum, to_date, date_format, countDistinct

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_insight_tweet")\
        .getOrCreate()
    
    tweet = spark.read.json(
        "/home/kauai/datapipeline/"
        "datalake/silver/twitter_chapecoense/tweet"
    )

    chapecoense = tweet\
        .where("author_id = '287690539'")\
        .select("author_id", "conversation_id")

    tweet = tweet.alias("tweet")\
        .join(
            chapecoense.alias("chapecoense"),
            [
                chapecoense.author_id != tweet.author_id,
                chapecoense.conversation_id == tweet.conversation_id
            ],
            'left')\
        .withColumn(
            "chapecoense_conversation",
            when(col("chapecoense.conversation_id").isNotNull(), 1).otherwise(0)
        ).withColumn(
            "reply_chapecoense",
            when(col("tweet.in_reply_to_user_id") == '287690539', 1).otherwise(0)
        ).groupBy(to_date("created_at").alias("created_date"))\
        .agg(
            countDistinct("id").alias("n_tweets"),
            countDistinct("tweet.conversation_id").alias("n_conversation"),
            sum("chapecoense_conversation").alias("chapecoense_conversation"),
            sum("reply_chapecoense").alias("reply_chapecoense")
        ).withColumn("weekday", date_format("created_date", "E"))\
    
    tweet.coalesce(1)\
        .write\
        .mode("overwrite")\
        .json(
        "/home/kauai/datapipeline/"
        "datalake/gold/twitter_insight_tweet"
    )
