
from data_extractor import extra_data_untrackable_journals, obtain_author_gender
from data_loader import load_data_from_file_into_db
from data_wrangler import create_paper_authors_collection
from db_manager import DBManager

import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


if __name__ == '__main__':
    db = DBManager('bioinfo_papers')
    # load_data_from_file_into_db(db, 'biolitmap_data.csv')
    # extra_data_untrackable_journals(db)
    # obtain_author_gender(db)
    create_paper_authors_collection(db)
