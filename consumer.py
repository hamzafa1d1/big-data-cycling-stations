from pyspark.sql.functions import from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from os.path import abspath

def index_to_elasticsearch(batch_df, batch_id):
    # Assuming 'batch_df' contains the data you want to index to Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

    # Get the schema of the DataFrame
    schema = batch_df.schema
    flattened_df = batch_df.select(
        "number",
        "contractName",
        "name",
        "address",
        "last_update",
        "position.latitude",
        "position.longitude",
        "bikes",
        "stands",
        "capacity"
    )
    # Extract actions from the DataFrame using the schema
    actions = [
        {
            "_index": "velib-stations",
            "_source": {
                field.name: row[field.name] if row[field.name] is not None else None
                for field in schema.fields
            }
        }
        for row in batch_df.collect()
    ]

    print(f"Number of actions: {len(actions)}")

    # Index the documents into Elasticsearch
    success, failed = bulk(es, actions, raise_on_error=False)

    if failed:
        print(f"Failed to index {len(failed)} documents.")
        for failure in failed:
            print(f"Failed document: {failure}")
    else:
        print(f"Indexed {success} documents successfully.")
    flattened_df = flattened_df.withColumn("last_update", to_timestamp(col("last_update"), "yyyy-MM-dd HH:mm:ss"))

    flattened_df.write \
        .mode("append") \
        .insertInto("cycling_stations", overwrite=True)
    


# Create an Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
index_name = "velib-stations"


# Check if the Elasticsearch index exists
if es.indices.exists(index=index_name):
    # Delete the index
    es.indices.delete(index=index_name)
    print(f"Index '{index_name}' has been deleted.")
else:
    print(f"Index '{index_name}' does not exist.")

mapping = {
    "mappings": {
        "properties": {
            "number": {"type": "integer"},
            "contractName": {"type": "keyword"},
            "name": {"type": "text"},
            "address": {"type": "text"},
            "last_update": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||epoch_millis"},
            "position": {
                "type": "geo_point",
                "fields": {
                    "latitude": {"type": "text"},
                    "longitude": {"type": "text"}
                }
            },
            "bikes": {"type": "integer"},
            "stands": {"type": "integer"},
            "capacity": {"type": "integer"}
            # Add more fields as needed
        }
    }
}

# Create the Elasticsearch index with the specified mapping
es.indices.create(index=index_name, body=mapping)



# Define the schema for your Kafka messages
schema = StructType([
    StructField("number", IntegerType(), True),
    StructField("contractName", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("last_update", StringType(), True),
    StructField("position", StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True)
    ]), True),
    StructField("totalStands", StructType([
        StructField("availabilities", StructType([
            StructField("bikes", IntegerType(), True),
            StructField("stands", IntegerType(), True)
        ]), True),
        StructField("capacity", IntegerType(), True)
    ]), True),
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("VelibStationConsumer") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Create the Hive database and use it
spark.sql("CREATE DATABASE IF NOT EXISTS cycling_stations")
spark.sql("USE cycling_stations")

# Define the Hive table schema
hive_table_schema = """
    CREATE TABLE IF NOT EXISTS cycling_stations (
        number INT,
        contractName STRING,
        name STRING,
        address STRING,
        last_update TIMESTAMP,
        latitude STRING,
        longitude STRING,
        bikes INT,
        stands INT,
        capacity INT
    )
    STORED AS PARQUET
"""

# Create the Hive table
spark.sql(hive_table_schema)

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "velib-stations") \
    .load()

# Deserialize JSON data from Kafka and select relevant columns
velib_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as value") \
    .select(from_json("value", schema).alias("data")) \
    .select(
        col("data.number"),
        col("data.contractName"),
        col("data.name"),
        col("data.address"),
        col("data.last_update"),
        struct(
            col("data.position.latitude").cast(FloatType()).alias("latitude"),
            col("data.position.longitude").cast(FloatType()).alias("longitude")
        ).alias("position"),
        col("data.totalStands.availabilities.bikes").cast(IntegerType()).alias("bikes"),
        col("data.totalStands.availabilities.stands").cast(IntegerType()).alias("stands"),
        col("data.totalStands.capacity").cast(IntegerType()).alias("capacity")
    )

# Define conditions for an empty station
empty_station_condition = (col("bikes") == 0) & (col("capacity") > 0)

# Filter stations that become empty
empty_stations_df = velib_df \
    .filter(empty_station_condition) \
    .select("address", "contractName", "number")

# Display empty stations in the console
console_query = empty_stations_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Index the result data into Elasticsearch
es_query = velib_df \
    .writeStream \
    .foreachBatch(index_to_elasticsearch) \
    .start()

es_query.awaitTermination()

console_query.awaitTermination()

# Stop the Spark session
spark.stop()