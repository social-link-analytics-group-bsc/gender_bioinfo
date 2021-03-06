
import csv
import logging
import pathlib

from db_manager import DBManager

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
            if 'e_id' in record:
                record_id = record['e_id']
            else:
                if record['id'] == '[No author name available]':
                    # skip authors without name
                    continue
                record_id = record['id']
            for key, value in record.items():
                if key in fields_to_export:
                    if key == 'countries':
                        countries = '-'.join(value)
                        record_to_save[key] = countries
                    elif key == 'authors':
                        num_authors = 0
                        # Sum up only male or female authors
                        for idx in range(0, len(value)):
                            if record['authors_gender'][idx] == 'male' or \
                               record['authors_gender'][idx] == 'female' or \
                               record['authors_gender'][idx] == 'mostly_male' or \
                               record['authors_gender'][idx] == 'mostly_female':
                                num_authors += 1
                        record_to_save[key] = num_authors
                    else:
                        record_to_save[key] = value
                else:
                    if key == 'authors_gender':
                        if 'gender_last_author' in fields_to_export:
                            if len(record['authors_gender']) > 0:
                                # Return the last gender, which should be male or female
                                record['authors_gender'].reverse()
                                for author_gender in record['authors_gender']:
                                    if author_gender == 'male' or author_gender == 'female':
                                        record_to_save['gender_last_author'] = author_gender
                                        break
                            else:
                                record_to_save['gender_last_author'] = '-'
            record_to_save['id'] = record_id
            writer.writerow(record_to_save)
    logging.info(f"It was exported {record_counter} records")


def export_author_papers(filename):
    db_papers = DBManager('bioinfo_papers')
    papers = db_papers.search({})
    current_dir = pathlib.Path(__file__).parents[0]
    fn = current_dir.joinpath('data', filename)
    logging.info('Exporting data of papers and authors, please wait...')
    with open(str(fn), 'w', encoding='utf-8') as f:
        headers = ['id', 'title', 'doi', 'year', 'category', 'author_id', 'author', 'author_gender', 'author_position']
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        record_counter, papers_counter, papers_without_authors = 0, 0, 0
        for paper in papers:
            paper_authors = paper.get('authors')
            authors_gender = paper.get('authors_gender')
            authors_id = paper.get('authors_id')
            if paper_authors:
                papers_counter += 1
                for idx in range(0, len(paper_authors)):
                    if authors_id[idx] == '[No author name available]':
                        # skip authors without name
                        continue
                    author_name = paper_authors[idx]
                    record_counter += 1
                    record_to_save = {
                        'id': paper['e_id'],
                        'title': paper['title'],
                        'doi': paper['DOI'],
                        'year': paper['year'],
                        'category': paper['edamCategory'],
                        'author_id': authors_id[idx],
                        'author': author_name,
                        'author_gender': authors_gender[idx],
                        'author_position': idx+1
                    }
                    writer.writerow(record_to_save)
            else:
                papers_without_authors += 1
    logging.info(f"It was exported {papers_counter} papers")
    logging.info(f"Found {papers_without_authors} papers without authors")


def export_unknown_gender(filename):
    db_authors = DBManager('bioinfo_authors')
    u_authors = db_authors.search({'gender': 'unknown'})
    current_dir = pathlib.Path(__file__).parents[0]
    fn = current_dir.joinpath('data', filename)
    logging.info('Exporting data of authors with unknown gender, please wait...')
    with open(str(fn), 'w', encoding='utf-8') as f:
        headers = ['id', 'name', 'gender', 'affiliation']
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        record_counter = 0
        for u_author in u_authors:
            record_to_save = {
                'id': record_counter,
                'name': u_author['name'],
                'gender': u_author['gender']
            }
            if u_author.get('affiliations'):
                record_to_save['affiliation'] = u_author['affiliations'][len(u_author['affiliations'])-1]
            writer.writerow(record_to_save)
            record_counter += 1
