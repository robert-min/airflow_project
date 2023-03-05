from pyspark.sql import SparkSession
import os

# spark set
MAX_MEMORY="5g"
spark = SparkSession.builder.appName("taxi_fare_prediction")\
                    .config("spark.executor.memory", MAX_MEMORY)\
                    .config("spark.driver.memory", MAX_MEMORY)\
                    .getOrCreate()

# data load
src_path = os.path.abspath(os.path.join(__file__, os.path.pardir))
data_dir = os.path.abspath(os.path.join(src_path, "data"))
trip_dir = os.path.abspath(os.path.join(data_dir, "trips"))

trips_file = os.path.abspath(os.path.join(trip_dir, "*"))
trips_df = spark.read.csv(f"file:///{trips_file}", inferSchema=True, header=True)

# data preprocessing
trips_df.createOrReplaceTempView("trips")

query = """
SELECT 
    passenger_count,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    trip_distance,
    HOUR(tpep_pickup_datetime) as pickup_time,
    DATE_FORMAT(TO_DATE(tpep_pickup_datetime), 'EEEE') AS day_of_week,
    total_amount
FROM
    trips
WHERE
    total_amount < 5000
    AND total_amount > 0
    AND trip_distance > 0
    AND trip_distance < 500
    AND passenger_count < 4
    AND TO_DATE(tpep_pickup_datetime) >= '2021-01-01'
    AND TO_DATE(tpep_pickup_datetime) < '2021-08-01'
"""

data_df = spark.sql(query)
train_df, test_df = data_df.randomSplit([0.8, 0.2], seed=1)

# data save
train_df.write.format("parquet").mode("overwrite").save(f"{data_dir}/train/")
test_df.write.format("parquet").mode("overwrite").save(f"{data_dir}/test/")


