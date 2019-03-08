from db_manager import DBManager

import ast
import csv
import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


def load_data_from_file_into_db(filename):
    db = DBManager('bioinfo_papers')
    current_dir = pathlib.Path(__file__).parents[0]
    bio_file_name = current_dir.joinpath('data', filename)
    with open(str(bio_file_name), 'r', encoding='ISO-8859-1') as f:
        file = csv.DictReader(f, delimiter='\t')
        for line in file:
            line['source'] = line['source'].lower()
            db.store_record(line)


def update_data_from_file(filename):
    db = DBManager('bioinfo_papers')
    current_dir = pathlib.Path(__file__).parents[0]
    bio_file_name = current_dir.joinpath('data', filename)
    with open(str(bio_file_name), 'r', encoding='ISO-8859-1') as f:
        file = csv.DictReader(f, delimiter=',')
        for line in file:
            paper = db.find_record({'DOI': line['DOI']})
            if not paper or 'authors' not in paper.keys():
                logging.info(f"Updating the paper {line['DOI']}")
                line['source'] = line['source'].lower()
                try:
                    line['authors'] = ast.literal_eval(line['authors_fullname'])
                    del line['authors_fullname']
                except:
                    logging.error(f"Could not update the paper {line['DOI']}")
                db.update_record({'DOI': line['DOI']}, line, create_if_doesnt_exist=True)

