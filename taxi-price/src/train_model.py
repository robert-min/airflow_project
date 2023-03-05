from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression

import pandas as pd
import os

path = os.getcwd()

MAX_MEMORY="5g"
spark = SparkSession.builder.appName("taxi_fare_prediction")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()

src_path = os.path.abspath(os.path.join(__file__, os.path.pardir))
data_dir = os.path.abspath(os.path.join(src_path, "data"))
train_dir = os.path.abspath(os.path.join(data_dir, "train"))
test_dir = os.path.abspath(os.path.join(data_dir, "test"))

train_df = spark.read.parquet(train_dir)
test_df = spark.read.parquet(test_dir)


hyper_df = pd.read_csv(f"{data_dir}/hyperparameter.csv")
alpha = float(hyper_df.iloc[0]['alpha'])
reg_param = float(hyper_df.iloc[0]['reg_param'])


cat_features = [
    "pickup_location_id",
    "dropoff_location_id",
    "day_of_week"
]

stages = []

for c in cat_features:
    cat_indexer = StringIndexer(inputCol=c, outputCol= c + "_idx").setHandleInvalid("keep")
    onehot_encoder = OneHotEncoder(inputCols=[cat_indexer.getOutputCol()], outputCols=[c + "_onehot"])
    stages += [cat_indexer, onehot_encoder]

num_features = [
    "passenger_count",
    "trip_distance",
    "pickup_time"
]

for n in num_features:
    num_assembler = VectorAssembler(inputCols=[n], outputCol= n + "_vecotr")
    num_scaler = StandardScaler(inputCol=num_assembler.getOutputCol(), outputCol= n + "_scaled")
    stages += [num_assembler, num_scaler]

assembler_inputs = [c + "_onehot" for c in cat_features] + [n + "_scaled" for n in num_features]
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="feature_vector")
stages += [assembler]


# Training
transform_stages = stages
pipeline = Pipeline(stages=transform_stages)
fitted_transformer = pipeline.fit(train_df)

vtrain_df = fitted_transformer.transform(train_df)
lr = LinearRegression(
    maxIter=50,
    solver="normal",
    labelCol="total_amount",
    featuresCol="feature_vector",
    elasticNetParam=alpha,
    regParam=reg_param,
)

model = lr.fit(vtrain_df)
vtest_df = fitted_transformer.transform(test_df)
predictions = model.transform(vtest_df)
predictions.cache()
predictions.select(["trip_distance", "day_of_week", "total_amount", "prediction"]).show()

model_dir = data_dir + "/model"
model.write().overwrite().save(model_dir)