from data_wrangler import create_author_record, update_author_record
from db_manager import DBManager
from doiorg_client import DoiClient

import ast
import csv
import logging
import pathlib
import os

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


def __process_paper_authors(paper_summary, paper_full, db_authors, author_names, authors_gender):
    author_ids = paper_summary['Author(s) ID'].split(';').strip()
    author_affiliations = paper_full['Authors with affiliations'].split(';').strip()
    paper_doi = paper_summary['DOI']
    if len(author_names) > 0:
        author_index = 0
        for full_author in zip(author_names, author_ids, author_affiliations):
            author_name = full_author[0]
            author_id = full_author[1]
            author_affiliation = full_author[2]
            author_db_new = db_authors.find_record({'id': author_id})
            if author_db_new:
                update_author_record(
                    author_in_db=author_db_new,
                    author_name=author_db_new['name'],
                    author_index=author_index,
                    author_gender=author_db_new['gender'],
                    article={'DOI': paper_doi, 'citations': paper_summary['Cited by']},
                    db_authors=db_authors
                )
                author_affs = author_db_new['affiliations']
                if author_affiliation.lower() not in author_affs:
                    author_affs.append(author_affiliation)
            else:
                author_name = author_name
                author_gender = authors_gender[author_index]
                create_author_record(
                    author_name=author_name,
                    author_gender=author_gender,
                    author_index=author_index,
                    article={'DOI': paper_doi, 'citations': paper_summary['Cited by']},
                    db_authors=db_authors
                )
                author_affs = [author_affiliation]
            db_authors.update_record({'id': author_id}, {'affiliations': author_affs})
            author_index += 1
    else:
        for full_author in zip(author_ids, author_affiliations):
            author_id = full_author[0]
            author_affiliation = full_author[1]
            author_db_new = db_authors.find_record({'id': author_id})
            if author_db_new:
                author_affs = author_db_new['affiliations']
                if author_affiliation.lower() not in author_affs:
                    author_affs.append(author_affiliation)
                db_authors.update_record({'id': author_id}, {'affiliations': author_affs})
            else:
                author_affs = [author_affiliation]
                db_authors.save_record({'id': author_id, 'affiliations': author_affs})


def __obtain_paper_abstract(file_name, paper_doi):
    dir_full = pathlib.Path('data', 'processed')
    journal_file_name = dir_full.joinpath('data', file_name)
    with open(str(journal_file_name), 'r', encoding='ISO-8859-1') as f:
        file = csv.DictReader(f, delimiter='\t')
        for line in file:
            if line['DOI'] == paper_doi:
                return line['abstract']
    return None


def load_data_from_files_into_db():
    dc = DoiClient()
    db_papers_new = DBManager('bioinfo_papers', db_name='bioinfo')
    db_authors_new = DBManager('bioinfo_authors', db_name='bioinfo')
    db_papers_old = DBManager('bioinfo_papers', db_name='bio4women')
    dir_summary = pathlib.Path('data', 'raw', 'summary')
    file_names = sorted(os.listdir(dir_summary))
    for file_name in file_names:
        logging.info(f"\nProcessing: {file_name}")
        journal_file_name = dir_summary.joinpath('data', file_name)
        with open(str(journal_file_name), 'r', encoding='ISO-8859-1') as f:
            file = csv.DictReader(f, delimiter='\t')
            for line in file:
                paper_db = db_papers_old.find_record({'DOI': line['DOI']})
                if paper_db:
                    paper_categories = paper_db['edamCategory']
                    link = paper_db['link']
                    authors = paper_db['authors']
                    authors_gender = paper_db['authors_gender']
                    pubmed_id = paper_db['pubmed_id']
                else:
                    paper_categories = ''
                    link = dc.get_paper_link_from_doi(line['DOI'])
                    pubmed_id = None
                    authors = []
                    authors_gender = []
                record_to_save = {
                    'title': line['Title'],
                    'year': line['Year'],
                    'DOI': line['DOI'],
                    'source': line['Source title'],
                    'volume': line['Volumen'],
                    'issue': line['Issue'],
                    'scopus_id': line['Art. No.'],
                    'link': link,
                    'e_id': line['EID'],
                    'citations': line['Cited by'],
                    'edamCategory': paper_categories,
                    'authors': authors,
                    'authors_gender': authors_gender,
                    'pubmed_id': pubmed_id,
                    'abstract': __obtain_paper_abstract(file_name, line['DOI'])
                }
                db_papers_new.store_record(record_to_save)
                __process_paper_authors(line, '', db_authors_new, authors, authors_gender)


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
