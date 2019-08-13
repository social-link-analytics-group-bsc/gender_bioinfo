
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
            # Do not include records with the delete flag
            if 'delete' in record.keys():
                logging.info(f"The record {record['_id']} contains the delete flag so it won't be exported")
                continue
            record_counter += 1
            record_to_save = dict()
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
            record_to_save['id'] = record_counter
            writer.writerow(record_to_save)


def export_author_papers(filename):
    db_papers = DBManager('bioinfo_papers')
    db_authors = DBManager('bioinfo_authors')
    papers = db_papers.search({})
    authors_without_del_flag = [author['name'] for author in db_authors.get_name_authors_without_del_flag()]
    current_dir = pathlib.Path(__file__).parents[0]
    fn = current_dir.joinpath('data', filename)
    logging.info('Exporting data of papers and authors, please wait...')
    with open(str(fn), 'w', encoding='utf-8') as f:
        headers = ['id', 'title', 'doi', 'year', 'category', 'author', 'author_gender', 'author_position']
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        record_counter, papers_counter = 0, 0
        for paper in papers:
            # Only include papers without the delete flag
            if 'delete' in paper.keys():
                logging.info(f"The paper {paper['_id']} contains the delete flag so it won't be exported")
                continue
            papers_counter += 1
            paper_authors = paper.get('authors')
            authors_gender = paper.get('authors_gender')
            if paper_authors:
                for idx in range(0, len(paper_authors)):
                    author_name = paper_authors[idx]
                    include_record = False
                    if paper_authors[idx] not in authors_without_del_flag:
                        author_db = db_authors.find_record({'other_names': {'$in': [paper_authors[idx]]}})
                        if author_db is None:
                            logging.info(f"The author {paper_authors[idx]} contains the delete flag so it won't be "
                                         f"exported")
                        else:
                            if 'delete' not in author_db:
                                author_name = author_db['name']
                                include_record = True
                    else:
                        include_record = True
                    if include_record:
                        # Only include authors without the delete flag
                        record_counter += 1
                        record_to_save = {
                            'id': record_counter,
                            'title': paper['title'],
                            'doi': paper['DOI'],
                            'year': paper['year'],
                            'category': paper['edamCategory'],
                            'author': author_name,
                            'author_gender': authors_gender[idx],
                            'author_position': idx+1
                        }
                        writer.writerow(record_to_save)
            else:
                print(f"Paper: {paper['title']} does not have authors")
    logging.info(f"It was exported {papers_counter} papers")


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
