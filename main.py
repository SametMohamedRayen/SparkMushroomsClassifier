from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString
from pyspark.sql import SparkSession


def prediction_to_string(pred_double):
    if pred_double == 1.0:
        return "poisonous"
    return "edible"


# Create a SparkSession
spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()

# Load the dataset as a DataFrame
data = spark.read.csv("mushrooms.csv", header=True, inferSchema=True)

# Convert string values to numeric values using StringIndexer
indexers = [StringIndexer(inputCol=col, outputCol=col+"_index").fit(data)
            for col in data.columns]
pipeline = Pipeline(stages=indexers)
data = pipeline.fit(data).transform(data)
print(data.schema)

# Define the feature columns to use
feature_cols = data.columns[24:]
print(feature_cols)

# Assemble the feature columns into a single vector column
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Split the dataset into training and test sets
(training_data, test_data) = data.randomSplit([1.0, 0.0])
# Train a Random Forest classifier
rf = RandomForestClassifier(labelCol="class_index", featuresCol="features", numTrees=10)
pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(training_data)

# Load the new CSV input data as a DataFrame
new_data = spark.read.csv("input.csv", header=True, inferSchema=True)
pipeline = Pipeline(stages=indexers)

# Convert string values to numeric values using StringIndexer
new_data = pipeline.fit(new_data).transform(new_data)

# Make predictions on the new input data
predictions = model.transform(new_data)

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=["Edible", "Poisonous"])
# Apply the transformation to the predictions DataFrame to convert the predicted labels from numerical values to string values
predictions = labelConverter.transform(predictions)
# Print the predicted target values for the new data
predictions.select("predictedLabel").show()
