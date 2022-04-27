from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create SparkSession
spark = SparkSession.builder.master("local[*]").appName("SF crime data stream") \
    .config("spark.sql.shuffle.partitions", 4)\
    .config("spark.streaming.stopGracefullyOnShutdown", "true")\
    .getOrCreate()

# Schema of crime data sample file
schema1 = StructType([
    StructField("IncidntNum", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Descript", StringType(), True),
    StructField("DayOfWeek", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("PdDistrict", StringType(), True),
    StructField("Resolution", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("X", StringType(), True),
    StructField("Y", StringType(), True),
    StructField("PdId", StringType(), True)
]
)


#Read Stream
df_opt1 = spark.readStream.format("csv").schema(schema1).option("header", "True") \
    .option("maxFilesPerTrigger", 1) \
    .load("/home/dhaval/Documents/crimeDataAnalysis/sampleData/sampleDataSplit")

df_opt1.createOrReplaceTempView("sf_crime")

######## Number of crimes in each Category ########

# Spark SQL Approach
crimeCategory = spark.sql("SELECT Category, COUNT(*) AS Count FROM sf_crime GROUP BY category ORDER BY Count DESC")

# DataFrame Approach
#crimeCategoryDF = df_opt1.groupBy('category').count().orderBy('count', ascending=False)


crimeCategoryStream = crimeCategory.writeStream \
    .format('console') \
    .outputMode('complete') \
    .option("checkpointLocation", "checkpoint-location1") \
    .start()

crimeCategoryStream.awaitTermination()

