
import csv
import logging
import pathlib


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


def export_db_into_file(filename_to_export, db, fields_to_export):
    records = db.search({})
    current_dir = pathlib.Path(__file__).parents[0]
    fn = current_dir.joinpath('data', filename_to_export)
    logging.info('Exporting data, please wait...')
    with open(str(fn), 'w', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields_to_export)
        writer.writeheader()
        for record in records:
            record_to_save = dict()
            for key, value in record.items():
                if key in fields_to_export:
                    if key == 'countries':
                        countries = '-'.join(value)
                        record_to_save[key] = countries
                    else:
                        record_to_save[key] = value
            writer.writerow(record_to_save)
