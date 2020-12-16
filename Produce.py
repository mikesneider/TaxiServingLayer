from confluent_kafka import Producer
import pandas as pd

pd.options.display.width = 0

class Produce_Data():

    def __init__(self):
        self.p = Producer({'bootstrap.servers': 'localhost:9092'})
        self.topic = 'nycspeed12'
        self.counter = 0

    def produce_messages(self):
        self.path = ("/media/sid/0EFA13150EFA1315/NYCTaxiData/trip_data_1.csv")
        for chunk in pd.read_csv(self.path,chunksize=1000000):
            try:
                chunk.columns = chunk.columns.str.replace(' ', '')
            except Exception as e:
                print("no columns to be renames, " + str(e))
            for i, row in chunk.iterrows():
                self.p.poll(0)
                self.p.produce(self.topic, str(
                    {
                        'medallion': row['medallion'],
                        'pickup_time': row['pickup_datetime'],
                        'dropoff_time': row['dropoff_datetime'],
                        'trip_time': row['trip_time_in_secs'],
                        'passenger_count': row['passenger_count'],
                        'trip_distance': row['trip_distance'],
                        'pickup_loc': {
                            'lat':row['pickup_latitude'],
                            'lon':row['pickup_longitude']
                            },
                        'dropoff_loc': {
                            'lat': row['dropoff_latitude'],
                            'lon': row['dropoff_longitude']
                            }
                    }
                ).encode('utf-8'))
                self.p.poll(0)
                # print("Produced following row")
                # print(row)
                # print('----')
                self.counter += 1
            self.p.flush()

if __name__ == '__main__':
    kafka_obj = Produce_Data()
    kafka_obj.produce_messages()


