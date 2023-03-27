from pyspark.sql.functions import *
from pyspark.sql.types import *

class SocialMediaDataCleaner:
    
    def __init__(self, language="en"):
        self.language = language
    
    def clean_data(self, input_file_path, output_file_path):
        
        # Load social media data from a file
        data = spark.read.json(input_file_path)

        # Select relevant columns
        data = data.select("id", "text", "created_at")

        # Convert created_at to timestamp
        data = data.withColumn("created_at", unix_timestamp("created_at", "EEE MMM dd HH:mm:ss Z yyyy").cast(TimestampType()))

        # Remove URLs
        data = data.withColumn("text", regexp_replace("text", r'http\S+', ''))

        # Remove cross-posted social mentions
        data = data.withColumn("text", regexp_replace("text", r'@[A-Za-z0-9_]+', ''))

        # Remove RTs
        data = data.filter(lower(col("text")).rlike('^((?!rt).)*$'))

        # Remove usernames and hashtags
        data = data.withColumn("text", regexp_replace("text", r'#([^\s]+)', ''))
        data = data.withColumn("text", regexp_replace("text", r'@[A-Za-z0-9_]+', ''))

        # Remove posts containing only hashtags and no other text
        data = data.filter(length(col("text")) > length(regexp_replace(col("text"), r'#', '')))

        # Filter by language
        data = data.filter(detect_language(col("text")) == self.language)

        # Remove spam
        data = data.filter(~lower(col("text")).rlike('(free|discount|buy|sale|click|subscribe|download)'))

        # Convert text to lowercase
        data = data.withColumn("text", lower(col("text")))

        # Remove non-alphabetic characters
        data = data.withColumn("text", regexp_replace("text", r'[^a-zA-Z\s]', ''))

        # Remove leading/trailing spaces and extra spaces
        data = data.withColumn("text", trim(regexp_replace("text", r'\s+', ' ')))

        # Drop rows with empty text
        data = data.filter(length(col("text")) > 0)

        # Save cleaned data to a file
        data.write.mode("overwrite").json(output_file_path)
