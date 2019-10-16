from db_manager import DBManager
from data_extractor import get_paper_author_names_from_pubmed
from data_loader import load_data_from_files_into_db
from data_wrangler import combine_csv_files
from data_exporter import export_db_into_file, export_author_papers
from utils import get_db_name

import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


if __name__ == '__main__':
    # 1. Combine files in data/raw/full into one CSV file per journal
    logging.info('Combining files...')
    combine_csv_files()

    # 2.Load the data in data/raw/summary and data/processed into the database
    logging.info('Loading data from files...')
    load_data_from_files_into_db()

    # 3. Get information about the papers' authors, including their full names and gender
    logging.info('Getting full name and gender of authors...')
    get_paper_author_names_from_pubmed()

    # 4. Export data of papers to CSV
    logging.info('Exporting data of papers to data/papers.csv ...')
    db_papers = DBManager('bioinfo_papers', db_name=get_db_name())
    fields_to_export = ['title', 'DOI', 'year', 'source', 'citations', 'edamCategory',
                        'link', 'authors', 'gender_last_author']
    export_db_into_file('papers.csv', db_papers, fields_to_export)

    # 5. Export data of authors to CSV
    logging.info('Exporting data of authors to data/authors.csv ...')
    db_authors = DBManager('bioinfo_authors', db_name=get_db_name())
    fields_to_export = ['name', 'gender', 'papers', 'total_citations', 'papers_as_first_author',
                        'papers_as_last_author', 'papers_with_citations']
    export_db_into_file('authors.csv', db_authors, fields_to_export)

    # 6. Export data of authors and papers to CSV
    logging.info('Exporting data of papers and authors to data/papers_authors.csv ...')
    export_author_papers('papers_authors.csv')

    logging.info('The files data/papers.csv, data/authors.csv, and data/papers_authors.csv were created')