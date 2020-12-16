from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import SparkConf, SparkContext
import os,ast, time
from pyspark.sql.functions import from_json, window, expr
import pyspark.sql.functions as func
from pyspark.sql.types import MapType, IntegerType, DoubleType, StringType, StructType, StructField, TimestampType
import pandas as pd


class Consume():

    def __init__(self):

        self.spark = SparkSession.builder \
                .master('local') \
                .config("es.index.auto.create", "true") \
                .appName('self.appName') \
                .getOrCreate()

    def read_data(self):

        userSchema = StructType([
                StructField('medallion', StringType()),
                StructField('pickup_time', TimestampType()),
                StructField('total_amount', DoubleType()),
                ])

        self.fare = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "nycfare1") \
            .option("startingOffsets", "earliest") \
            .option('failOnDataLoss','false') \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        self.df_fare = self.fare.selectExpr("CAST(value as STRING) as json") \
                   .select(from_json("json", userSchema).alias('data'))\
                   .selectExpr(
                        "data.medallion as medallion_fare",
                        "cast (data.pickup_time as timestamp) as pickup_time_fare",
                        "cast (data.total_amount as float)",
                    )

        userSchema = StructType([
            StructField('medallion', StringType()),
            StructField('pickup_time', TimestampType()),
            StructField('dropoff_time', TimestampType()),
            StructField('passenger_count', IntegerType()),
            StructField('trip_time', IntegerType()),
            StructField('trip_distance', DoubleType()),
            StructField('pickup_loc', MapType(StringType(), DoubleType())),
            StructField('dropoff_loc', MapType(StringType(), DoubleType()))
        ])

        self.trip = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "nycspeed9") \
            .option("startingOffsets", "earliest") \
            .option('failOnDataLoss', 'false') \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        self.df_trip = self.trip.selectExpr("CAST(value as STRING) as json") \
            .select(from_json("json", userSchema).alias('data')) \
            .selectExpr(
            "data.medallion as medallion_trip",
            "cast (data.pickup_time as timestamp) as pickup_time_trip",
            "cast (data.dropoff_time as timestamp)",
            "cast (data.passenger_count as integer)",
            "cast (data.trip_time as integer)",
            "cast (data.trip_distance as float)",
            "cast (data.pickup_loc.lat as float) as pickup_loc_lat",
            # "cast data.pickup_loc.lat as pickup_loc_lat"
            "cast (data.pickup_loc.lon as float) as pickup_loc_lon",
            # "cast data.pickup_loc.lon as pickup_loc_lon",
            "cast (data.dropoff_loc.lat as float) as dropoff_loc_lat",
            # "cast data.dropoff_loc.lat as dropoff_loc_lat",
            "cast (data.dropoff_loc.lon as float) as dropoff_loc_lon",
            # "cast data.dropoff_loc.lon as dropoff_loc_lon"
        )

        print(self.df_trip.printSchema())

        self.df = self.df_trip.join(
            self.df_fare,
            expr("""
            medallion_trip = medallion_fare AND
            pickup_time_trip >= pickup_time_fare - interval 1 hour AND
            pickup_time_trip <= pickup_time_fare + interval 1 hour
            """)
        )

        print((self.df \
              .writeStream \
              .outputMode("append") \
              .format("console") \
              .option('truncate','false')
              .option('numRows', 20)
              .start()
              .awaitTermination()
              ))

        query = self.windowedCounts.writeStream \
            .outputMode("append") \
            .queryName("writing_to_es") \
            .format("org.elasticsearch.spark.sql") \
            .option("checkpointLocation", "/tmp/1") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "nycfare2/_doc") \

        query.start().awaitTermination()


if __name__ == '__main__':
    consume_obj = Consume()
    consume_obj.read_data()
