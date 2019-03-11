
from db_manager import DBManager

import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


def create_update_paper_authors_collection(db_papers):
    db_authors = DBManager('bioinfo_authors')

    articles = db_papers.search({})
    for article in articles:
        if 'authors' in article.keys() and article['authors'] is not None:
            article_authors = article['authors']
            article_author_genders = article['authors_gender']
        else:
            # continue if the article doesn't have authors
            continue
        for index, author in enumerate(article_authors):
            author_gender = article_author_genders[index]
            # check if the author exists in the db
            author_in_db = db_authors.find_record({'name': author})
            if author_in_db:
                if 'dois' in author_in_db.keys() and article['DOI'] in author_in_db['dois']:
                    # the article was already processed for this author
                    continue
                # if the author already exists, update record
                author_in_db['dois'].append(article['DOI'])
                author_in_db['citations'].append(int(article['citations']))
                values_to_update = {
                    'papers': author_in_db['papers'] + 1,
                    'total_citations': author_in_db['total_citations'] + int(article['citations']),
                    'dois': author_in_db['dois'],
                    'citations': author_in_db['citations']
                }
                if index == 0:
                    values_to_update['papers_as_first_author'] = author_in_db['papers_as_first_author'] + 1
                # check if the stored gender of the author is unknown, if
                # this is the case replace with the current one
                if author_in_db['gender'] == 'unknown':
                    values_to_update['gender'] = author_gender
                if int(article['citations']) > 0:
                    values_to_update['papers_with_citations'] = author_in_db['papers_with_citations'] + 1
                if author_gender != 'unknown' and author_gender != author_in_db['gender']:
                    logging.warning(f"Author {author}'s with gender inconsistency. "
                                    f"Stored {author_in_db['gender']}. Article (doi {article['DOI']}) author_gender")
                db_authors.update_record({'name': author}, values_to_update)
                logging.info(f"Actualizado author {author}")
            else:
                record_to_save = {
                    'name': author,
                    'gender': author_gender,
                    'papers': 1,
                    'total_citations': int(article['citations']),
                    'papers_as_first_author': 0,
                    'dois': [article['DOI']],
                    'papers_with_citations': 0,
                    'citations': [int(article['citations'])]
                }
                if index == 0:
                    record_to_save['papers_as_first_author'] += 1
                if int(article['citations']) > 0:
                    record_to_save['papers_with_citations'] += 1
                db_authors.save_record(record_to_save)
                logging.info(f"Creado author {author}")


def compute_authors_h_index(override_metric=False):
    db_authors = DBManager('bioinfo_authors')
    authors = db_authors.search({})
    for author in authors:
        if 'h-index' in author.keys() and not override_metric:
            continue
        logging.info(f"Computing h-index of {author['name']}")
        author_citations = author['citations']
        h_index = author['papers_with_citations']
        if h_index > 0:
            while True:
                greater_counter = 0
                for citation in author_citations:
                    if citation >= h_index:
                        greater_counter += 1
                if greater_counter >= h_index:
                    break
                else:
                    h_index -= 1
        logging.info(f"{author['name']} has an h-index of {h_index}")
        db_authors.update_record({'name': author['name']}, {'h-index': h_index})
