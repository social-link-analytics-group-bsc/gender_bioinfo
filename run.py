
from data_extractor import extra_data_untrackable_journals, obtain_author_gender, get_authors_ncbi_journal, \
                           obtain_author_affiliation, obtain_affiliation_from_author
from data_loader import load_data_from_file_into_db, update_data_from_file
from data_wrangler import create_update_paper_authors_collection, compute_authors_h_index, clean_author_countries
from data_exporter import export_db_into_file
from db_manager import DBManager

import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


if __name__ == '__main__':
    db_papers = DBManager('bioinfo_papers')
    db_authors = DBManager('bioinfo_authors')
    # load_data_from_file_into_db('biolitmap_data.csv')
    # extra_data_untrackable_journals(db)
    # create_paper_authors_collection(db)
    # compute_authors_h_index()
    # update_data_from_file('genero_journals.csv')
    # get_authors_ncbi_journal(db)
    # obtain_author_gender(db)
    # create_update_paper_authors_collection(db)
    # compute_authors_h_index()
    # obtain_author_affiliation(db_papers, db_authors)
    obtain_affiliation_from_author(db_papers, db_authors)
    #clean_author_countries()
    #export_db_into_file('bioinfo_authors.csv', db_authors, ['name', 'gender', 'papers', 'total_citations',
    #                                                        'papers_as_first_author', 'papers_with_citations',
    #                                                        'h-index', 'countries'])
