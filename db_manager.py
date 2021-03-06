
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

    def num_records(self, query):
        return self.__db[self.__collection].count_documents(query)

    def save_record(self, record_to_save):
        self.__db[self.__collection].insert(record_to_save)

    def find_record(self, query):
        return self.__db[self.__collection].find_one(query)

    def update_record(self, filter_query, new_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$set': new_values},
                                                       upsert=create_if_doesnt_exist)

    def update_records(self, filter_query, new_values):
        return self.__db[self.__collection].update_many(filter_query, {'$set': new_values})

    def update_all_records(self, new_values):
        return self.__db[self.__collection].update_many({}, {'$set': new_values})

    def remove_field_from_record(self, filter_query, fields_to_remove):
        return self.__db[self.__collection].update_one(filter_query, {'$unset': fields_to_remove})

    def remove_field_from_all_records(self, fields_to_remove):
        return self.__db[self.__collection].update_many({}, {'$unset': fields_to_remove})

    def remove_record(self, filter_query):
        return self.__db[self.__collection].remove(filter_query)

    def search(self, query, return_fields=None):
        if not return_fields:
            return self.__db[self.__collection].find(query, no_cursor_timeout=True)
        else:
            return self.__db[self.__collection].find(query, return_fields, no_cursor_timeout=True)

    def store_record(self, record_to_store):
        if 'DOI' in record_to_store:
            record_identifier = record_to_store['DOI']
            query = {'DOI': record_identifier}
        elif 'name' in record_to_store:
            record_identifier = record_to_store['name']
            query = {'name': record_identifier}
        else:
            record_identifier = record_to_store['id']
            query = {'id': record_identifier}
        num_results = self.search(query).count()
        if num_results == 0:
            self.save_record(record_to_store)
            logging.info(f"Inserted record identified by: {record_identifier}")
            return True
        else:
            logging.info(f"Found record duplicated. Identifier: {record_identifier}")
            return False

    def get_papers_by_year(self):
        group = {
            '_id': '$year',
            'num_papers': {
                '$sum': 1
            }
        }
        project = {
            'year': '$_id',
            'count': '$num_papers',
            '_id': 0
        }
        sort = {
            'year': 1
        }
        pipeline = [
            {'$group': group},
            {'$project': project},
            {'$sort': sort}
        ]
        result_docs = self.aggregate(pipeline)
        return result_docs

    def get_average_citations_by_year(self):
        group = {
            '_id': '$year',
            'avg_citations': {
                '$avg': {
                    '$toInt': '$citations'
                }
            }
        }
        project = {
            'year': '$_id',
            'avg_citations': '$avg_citations',
            '_id': 0
        }
        sort = {
            'year': 1
        }
        pipeline = [
            {'$group': group},
            {'$project': project},
            {'$sort': sort}
        ]
        result_docs = self.aggregate(pipeline)
        return result_docs

    def get_name_authors_without_del_flag(self):
        match = {
            'delete': {
                '$exists': 0
            }
        }
        project = {
            'name': '$name',
            '_id': 0
        }
        pipeline = [
            {'$match': match},
            {'$project': project}
        ]
        result_docs = self.aggregate(pipeline)
        return result_docs

    def aggregate(self, pipeline):
        return [doc for doc in self.__db[self.__collection].aggregate(pipeline, allowDiskUse=True)]
