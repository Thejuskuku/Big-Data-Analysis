from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, expr

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaningWithTryCast").getOrCreate()

data = [
    (1, " Alice ", "29", "New York"),
    (2, "Bob", None, "  Los Angeles "),
    (3, None, "25", None),
    (4, "Charlie", "35", "San Francisco"),
    (5, "David", "not_a_number", "Seattle"),
    (6, "Eve", "40", "  ")
]

columns = ["id", "name", "age", "city"]

df = spark.createDataFrame(data, schema=columns)

print("Original Data:")
df.show()

# Trim whitespace
df_clean = df.withColumn("name", trim(col("name"))) \
    .withColumn("city", trim(col("city")))

# Drop rows with null name or age
df_clean = df_clean.dropna(subset=["name", "age"])

# Use try_cast to safely convert age to int, invalid casts become NULL
df_clean = df_clean.withColumn("age", expr("try_cast(age as int)"))

# Drop rows where age is NULL after conversion
df_clean = df_clean.dropna(subset=["age"])

print("Cleaned Data:")
df_clean.show()

spark.stop()
