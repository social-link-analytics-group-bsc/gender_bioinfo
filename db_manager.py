
from pymongo import MongoClient
from utils import get_config

import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


class DBManager:
    __db = None
    __host = None
    __collection = ''

    def __init__(self, collection, db_name=''):
        current_dir = pathlib.Path(__file__).parents[0]
        config_fn = current_dir.joinpath('config.json')
        config = get_config(config_fn)
        self.__host = config['mongo']['host']
        self.__port = config['mongo']['port']
        client = MongoClient(self.__host + ':' + self.__port)

        if not db_name:
            self.__db = client[config['mongo']['db_name']]
        else:
            self.__db = client[db_name]
        self.__collection = collection

    def save_record(self, record_to_save):
        self.__db[self.__collection].insert(record_to_save)

    def find_record(self, query):
        return self.__db[self.__collection].find_one(query)

    def update_record(self, filter_query, new_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$set': new_values},
                                                       upsert=create_if_doesnt_exist)

    def remove_field_from_record(self, filter_query, fields_to_remove):
        return self.__db[self.__collection].update_one(filter_query, {'$unset': fields_to_remove})

    def search(self, query):
        return self.__db[self.__collection].find(query, no_cursor_timeout=True)

    def store_record(self, record_to_store):
        num_results = 0
        if 'DOI' in record_to_store:
            record_identifier = record_to_store['DOI']
            num_results = self.search({'DOI': record_identifier}).count()
        else:
            record_identifier = record_to_store['name']
        if num_results == 0:
            self.save_record(record_to_store)
            logging.info(f"Inserted record identified by: {record_identifier}")
            return True
        else:
            logging.info(f"Found record duplicated. Identifier: {record_identifier}")
            return False

    def aggregate(self, pipeline):
        return [doc for doc in self.__db[self.__collection].aggregate(pipeline, allowDiskUse=True)]
