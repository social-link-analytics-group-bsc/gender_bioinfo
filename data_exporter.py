
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
        headers = ['id']
        headers.extend(fields_to_export)
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        record_counter = 0
        for record in records:
            record_counter += 1
            record_to_save = dict()
            for key, value in record.items():
                if key in fields_to_export:
                    if key == 'countries':
                        countries = '-'.join(value)
                        record_to_save[key] = countries
                    else:
                        record_to_save[key] = value
            record_to_save['id'] = record_counter
            writer.writerow(record_to_save)
