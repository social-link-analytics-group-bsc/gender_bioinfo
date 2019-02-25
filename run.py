
from data_extractor import extra_data_untrackable_journals
from db_manager import DBManager

import csv
import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


def load_data_from_file_into_db(db):
    current_dir = pathlib.Path(__file__).parents[0]
    bio_file_name = current_dir.joinpath('data', 'biolitmap_data.csv')
    with open(str(bio_file_name), 'r', encoding='ISO-8859-1') as f:
        file = csv.DictReader(f, delimiter='\t')
        for line in file:
            line['source'] = line['source'].lower()
            db.store_record(line)


if __name__ == '__main__':
    db = DBManager('gender_authors')
    # load_data_from_file_into_db(db)
    extra_data_untrackable_journals(db)
