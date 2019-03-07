import csv
import pathlib


def load_data_from_file_into_db(db, filename):
    current_dir = pathlib.Path(__file__).parents[0]
    bio_file_name = current_dir.joinpath('data', filename)
    with open(str(bio_file_name), 'r', encoding='ISO-8859-1') as f:
        file = csv.DictReader(f, delimiter='\t')
        for line in file:
            line['source'] = line['source'].lower()
            db.store_record(line)
