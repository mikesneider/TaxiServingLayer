from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, to_json, struct
import pyspark.sql.functions as func
from pyspark.sql.types import MapType, IntegerType, DoubleType, StringType, StructType, StructField, TimestampType


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
                StructField('dropoff_time', TimestampType()),
                StructField('passenger_count', IntegerType()),
                StructField('trip_time', IntegerType()),
                StructField('trip_distance', DoubleType()),
                StructField('pickup_loc', MapType(StringType(), DoubleType())),
                StructField('dropoff_loc', MapType(StringType(), DoubleType()))
                ])

        self.df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "nycspeed12") \
            .option("startingOffsets", "earliest") \
            .option('failOnDataLoss','false') \
            .option('enable.auto.commit','false') \
            .option('group.id','nyc6') \
            .option('auto.offset.reset','earliest') \
            .option("kafka.client.id", "nycid6") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        self.dff = self.df.selectExpr("CAST(value as STRING) as json") \
                   .select(from_json("json", userSchema).alias('data'))\
                   .selectExpr(
                        "data.medallion as medallion",
                        "cast (data.pickup_time as timestamp)",
                        "cast (data.dropoff_time as timestamp)",
                        "cast (data.passenger_count as integer)",
                        "cast (data.trip_time as integer)",
                        "cast (data.trip_distance as float)",
                        "cast (data.pickup_loc.lat as float) as pickup_loc_lat",
                        "cast (data.pickup_loc.lon as float) as pickup_loc_lon",
                        "cast (data.dropoff_loc.lat as float) as dropoff_loc_lat",
                        "cast (data.dropoff_loc.lon as float) as dropoff_loc_lon",
                    )

        print(self.dff.printSchema())

        self.windowedCounts = self.dff \
            .filter('trip_time > 0') \
            .withWatermark("pickup_time", "30 days") \
            .groupBy("medallion",
            window("pickup_time", "24 hours")) \
            .agg(func.sum('trip_distance').alias('sum_trip_distance'),
                 func.avg('trip_distance').alias('avg_trip_distance'),
                 func.sum('trip_time').alias('sum_trip_time'),
                 func.avg('trip_time').alias('avg_trip_time'),
                 func.sum('passenger_count').alias('sum_passenger_count'),
                 func.avg('passenger_count').alias('avg_passenger_count')
                 )

        print((self.windowedCounts \
              .writeStream \
              .outputMode("complete") \
              .format("console") \
              .option('truncate','false')
              .option('numRows', 20)
              .start()
              .awaitTermination()
              ))

        query = self.windowedCounts \
              .select(to_json(struct("medallion",'window','sum_trip_distance',
                              'avg_trip_distance','sum_trip_time','avg_trip_time',
                              'sum_passenger_count','avg_passenger_count')).alias('value')) \
              .writeStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", "localhost:9092") \
              .option("topic", "es3") \
              .option("checkpointLocation", "/tmp/kafkachkpnt/")\
              .outputMode('update') \
              .start()
        query.awaitTermination()


if __name__ == '__main__':
    consume_obj = Consume()
    consume_obj.read_data()
