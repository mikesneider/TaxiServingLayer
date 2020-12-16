from confluent_kafka import Consumer
import pandas as pd
from ast import literal_eval
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time, json

pd.options.display.width = 0
pd.set_option('display.float_format', lambda x: '%.3f' % x)


class Consume_Data():

    def __init__(self):
        self.topic = 'es2'
        self.df_main = pd.DataFrame()
        self.es = Elasticsearch()

    def ConsumeMessages(self):
        c = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'mygroup3',
            'auto.offset.reset': 'earliest'
        })

        c.subscribe([self.topic])
        self.counter = 0

        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            try:
                self.data = (msg.value().decode('utf-8'))
            except:
                print('--')
            with open("data.json",'a') as file:
                file.write(self.data + '\n')

    def write_data_to_kafka(self):
        self.df_main = pd.DataFrame()
        for chunk in pd.read_json('data.json',chunksize=500000, lines=True, nrows=5000000):
            self.df = pd.DataFrame()
            self.df = self.df.append(chunk,ignore_index=True)
            self.dff = self.df['window'].apply(pd.Series)
            self.df = self.df.drop(['window'],axis=1)
            self.df_main = self.df_main.append(pd.concat([self.df, self.dff],axis=1))
            self.df_main['start'] = self.df_main['start'].apply(lambda x:pd.to_datetime(str(x).split('+')[0]))
            self.df_main['end'] = self.df_main['end'].apply(lambda x: pd.to_datetime(str(x).split('+')[0]))
            self.data = []

            for i, row in self.df_main.iterrows():
                # print(row)
                self.data.append({
                    "_index": 'nycserve2',
                    "type": 'geodata',
                    "_id" : str(row['medallion']) + "_" + str(row['start']),
                    "_source": {
                        'medallion': row['medallion'],
                        'start': row['start'],
                        'end': row['end'],
                        'sum_trip_distance': row['sum_trip_distance'],
                        'avg_trip_distance': row['avg_trip_distance'],
                        'sum_trip_time': row['sum_trip_time'],
                        'avg_trip_time': row['avg_trip_time'],
                        'sum_passenger_count': row['sum_passenger_count'],
                        'avg_passenger_count': row['avg_passenger_count'],
                    }
                })
            print(self.data)
            helpers.bulk(self.es, self.data)
            print("data inserted")

        print(self.df_main.shape)
        self.df_main.drop_duplicates(keep='first')
        print(self.df_main.shape)
        self.df_main.to_csv('processed.csv',index=False,sep=',')



if __name__ == '__main__':
    consume_obj = Consume_Data()
    consume_obj.ConsumeMessages()
    consume_obj.write_data_to_kafka()