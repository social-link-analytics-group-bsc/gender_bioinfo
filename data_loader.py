from data_wrangler import create_author_record, update_author_record
from db_manager import DBManager
from doiorg_client import DoiClient
from similarity.jarowinkler import JaroWinkler
from utils import get_db_name, normalize_text, obtain_paper_abstract_and_pubmedid

import ast
import csv
import ctypes
import logging
import pathlib
import os

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


def load_data_from_file_into_db(filename):
    db = DBManager('bioinfo_papers')
    current_dir = pathlib.Path(__file__).parents[0]
    bio_file_name = current_dir.joinpath('data', filename)
    with open(str(bio_file_name), 'r', encoding='ISO-8859-1') as f:
        file = csv.DictReader(f, delimiter='\t')
        for line in file:
            line['source'] = line['source'].lower()
            db.store_record(line)


def __affiliations_to_save(affiliations, new_affiliations):
    jarowinkler = JaroWinkler()
    similarity_threshold = 0.95
    affiliations_to_save = []
    for new_affiliation in new_affiliations:
        exist_affiliation = False
        for affiliation in affiliations:
            # normalize text before comparison
            affiliation_nor = normalize_text(affiliation)
            new_affiliation_nor = normalize_text(new_affiliation)
            similarity_score = jarowinkler.similarity(affiliation_nor.lower(), new_affiliation_nor.lower())
            if similarity_score >= similarity_threshold:
                exist_affiliation = True
        if not exist_affiliation:
            affiliations_to_save.append(new_affiliation)
    return affiliations_to_save


def __get_actual_affiliations(affiliations, author_affiliation):
    actual_affiliations = []
    author_affiliation = author_affiliation.strip()
    for affiliation in affiliations:
        affiliation = affiliation.strip()
        # normalize text before comparison
        affiliation_nor = normalize_text(affiliation)
        author_affiliation_nor = normalize_text(author_affiliation)
        if affiliation_nor.lower() in author_affiliation_nor.lower():
            if affiliation:
                actual_affiliations.append(affiliation.strip())
    return actual_affiliations


def __process_paper_authors(paper_summary, paper_full, db_authors, author_names, authors_gender):
    author_ids = paper_summary['Author(s) ID'].split(';')
    author_affiliations = paper_full['Authors with affiliations'].split(';')
    affiliations = paper_full['Affiliations'].split(';')
    paper_doi = paper_summary['DOI']
    paper_citations = paper_summary['Cited by'] if paper_summary['Cited by'] else paper_full['Cited by']
    if author_names:
        it_authors = zip(author_names, author_ids, author_affiliations)
    else:
        it_authors = zip(author_ids, author_affiliations)
    for author_index, full_author in enumerate(it_authors):
        author_name, author_last_name = '', ''
        if len(full_author) > 2:
            author_name = full_author[0]
            author_id = full_author[1].strip()
            author_affiliation = ','.join(full_author[2].split(',')[2:]).strip()
        else:
            author_id = full_author[0].strip()
            author_affiliation = ','.join(full_author[1].split(',')[2:]).strip()
            author_last_name = full_author[1].split(',')[0].strip().title()
        author_db_new = db_authors.find_record({'id': author_id})
        actual_affiliations = __get_actual_affiliations(affiliations, author_affiliation)
        if author_db_new:
            logging.info(f"Author with id {author_id} already exist")
            update_author_record(
                author_in_db=author_db_new,
                author_name=author_db_new['name'],
                author_index=author_index,
                author_gender=author_db_new['gender'],
                article={'DOI': paper_doi, 'citations': paper_citations},
                db_authors=db_authors
            )
            author_affs = author_db_new['affiliations']
            affiliations_to_save = __affiliations_to_save(author_affs, actual_affiliations)
            if len(affiliations_to_save) > 0:
                author_affs.extend(affiliations_to_save)
                db_authors.update_record({'id': author_id}, {'affiliations': author_affs})
        else:
            if authors_gender:
                author_gender = authors_gender[author_index]
            else:
                author_gender = ''
            create_author_record(
                author_name=author_name,
                author_gender=author_gender,
                author_index=author_index,
                article={'DOI': paper_doi, 'citations': paper_citations},
                db_authors=db_authors,
                author_id=author_id
            )
            db_authors.update_record({'id': author_id}, {'affiliations': actual_affiliations,
                                                         'last_name': author_last_name})


def load_data_from_files_into_db(exist_old_db=False, name_old_db=''):
    dc = DoiClient()
    db_name = get_db_name()
    db_papers_old = None
    db_papers_new = DBManager('bioinfo_papers', db_name=db_name)
    db_authors_new = DBManager('bioinfo_authors', db_name=db_name)
    if exist_old_db:
        db_papers_old = DBManager('bioinfo_papers', db_name=name_old_db)
    dir_summary = pathlib.Path('data', 'raw', 'summary')
    file_names = sorted(os.listdir(dir_summary))
    num_insertions = 0
    for file_name in file_names:
        logging.info(f"\nProcessing: {file_name}")
        journal_file_name = dir_summary.joinpath(file_name)
        with open(str(journal_file_name), 'r', encoding='utf-8') as f:
            file = csv.DictReader(f, delimiter=',')
            for line in file:
                logging.info(f"Processing the paper {line['DOI']}")
                if not line['DOI']:
                    continue
                paper_new_db = db_papers_new.find_record({'DOI': line['DOI']})
                if not paper_new_db:
                    paper_old_db = None
                    if db_papers_old:
                        paper_old_db = db_papers_old.find_record({'DOI': line['DOI']})
                    pubmed_id = None
                    if paper_old_db:
                        paper_categories = paper_old_db['edamCategory']
                        link = paper_old_db['link']
                        authors = paper_old_db.get('authors')
                        authors_gender = paper_old_db.get('authors_gender')
                        pubmed_id = paper_old_db.get('pubmed_id')
                    else:
                        paper_categories = ''
                        logging.info(f"Obtaining the link of the paper {line['DOI']}")
                        link = dc.get_paper_link_from_doi(line['DOI'])
                        authors = []
                        authors_gender = []
                    abstract, _pubmed_id, paper_full = obtain_paper_abstract_and_pubmedid(file_name, line['EID'])
                    if not pubmed_id:
                        pubmed_id = _pubmed_id
                    record_to_save = {
                        'title': line['Title'],
                        'year': line['Year'],
                        'DOI': line['DOI'],
                        'source': line['Source title'].title(),
                        'volume': line['Volume'],
                        'issue': line['Issue'],
                        'scopus_id': line['Art. No.'],
                        'link': link,
                        'e_id': line['EID'],
                        'citations': line['Cited by'],
                        'edamCategory': paper_categories,
                        'pubmed_id': pubmed_id,
                        'abstract': abstract
                    }
                    db_papers_new.store_record(record_to_save)
                    num_insertions += 1
                    if paper_full:
                        __process_paper_authors(line, paper_full, db_authors_new, authors, authors_gender)
                    else:
                        logging.error(f"Could not find the full details of the paper {line['DOI']}")
                else:
                    logging.info(f"Paper {line['DOI']} already in the database!")
    logging.info(f"\n{num_insertions} new papers were inserted!")


def load_author_data_from_scopus_files():
    db_name = get_db_name()
    db_authors = DBManager('bioinfo_authors', db_name=db_name)
    db_papers = DBManager('bioinfo_papers', db_name=db_name)
    dir_summary = pathlib.Path('data', 'raw', 'summary')
    file_names = sorted(os.listdir(dir_summary))
    for file_name in file_names:
        logging.info(f"\nProcessing: {file_name}")
        journal_file_name = dir_summary.joinpath(file_name)
        with open(str(journal_file_name), 'r') as f:
            file = csv.DictReader(f, delimiter=',')
            for line in file:
                paper_db = db_papers.find_record({'DOI': line['DOI']})
                if paper_db:
                    logging.info(f"Processing the authors of the paper: {line['DOI']}")
                    abstract, _pubmed_id, paper_full = obtain_paper_abstract_and_pubmedid(file_name, line['EID'])
                    __process_paper_authors(line, paper_full, db_authors, [], [])


def check_data_to_insert():
    dir_summary = pathlib.Path('data', 'raw', 'summary')
    file_names = sorted(os.listdir(dir_summary))
    papers_to_insert = 0
    journal = []
    for file_name in file_names:
        papers_without_doi, num_duplicates, num_papers = 0, 0, 0
        logging.info(f"\nProcessing: {file_name}")
        journal_file_name = dir_summary.joinpath(file_name)
        with open(str(journal_file_name), 'r', encoding='ISO-8859-1') as f:
            file = csv.DictReader(f, delimiter=',')
            for line in file:
                num_papers += 1
                if line['DOI']:
                    if line['DOI'] not in journal:
                        papers_to_insert += 1
                        journal.append(line['DOI'])
                    else:
                        num_duplicates += 1
                else:
                    papers_without_doi += 1
            logging.info(f"Num. Papers: {num_papers}, Num. Unique Papers: {papers_to_insert}, "
                         f"Num. Duplicates: {num_duplicates}, Papers without DOI: {papers_without_doi}")
    logging.info(f"Total of papers to insert (list): {len(journal)}")


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
