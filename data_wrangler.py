
from db_manager import DBManager

import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


def create_paper_authors_collection(db_papers):
    db_authors = DBManager('bioinfo_authors')

    articles = db_papers.search({})
    for article in articles:
        if 'authors' in article.keys():
            article_authors = article['authors']
            article_author_genders = article['authors_gender']
        else:
            # continue if the article doesn't have authors
            continue
        for index, author in enumerate(article_authors):
            author_gender = article_author_genders[index]
            # check if the author exists in the db
            author_in_db = db_authors.find_record({'name': author})
            if article['DOI'] in author_in_db['dois']:
                # the article was already processed for this author
                continue
            if author_in_db and author_in_db.count() > 0:
                # if the author already exists, update record
                author_in_db['dois'].append(article['DOI'])
                author_in_db['citations'].append(article['citations'])
                values_to_update = {
                    'papers': author_in_db['papers'] + 1,
                    'total_citations': author_in_db['total_citations'] + article['citations'],
                    'dois': author_in_db['dois'],
                    'citations': author_in_db['citations']
                }
                if index == 0:
                    values_to_update['papers_as_first_author'] = author_in_db['papers_as_first_author'] + 1
                # check if the stored gender of the author is unknown, if
                # this is the case replace with the current one
                if author_in_db['gender'] == 'unknown':
                    values_to_update['gender'] = author_gender
                if article['citations'] > 0:
                    values_to_update['papers_with_citations'] = author_in_db['papers_with_citations'] + 1
                if author_gender != 'unknown' and author_gender != author_in_db['gender']:
                    logging.warning(f"Author {author}'s with gender inconsistency. "
                                    f"Stored {author_in_db['gender']}. Article (doi {article['DOI']}) author_gender")
                db_authors.update_record({'name': author}, values_to_update)
            else:
                record_to_save = {
                    'name': author,
                    'gender': author_gender,
                    'papers': 1,
                    'total_citations': article['citations'],
                    'papers_as_first_author': 0,
                    'dois': [article['DOI']],
                    'papers_with_citations': 0,
                    'citations': [article['citations']]
                }
                if index == 0:
                    record_to_save['papers_as_first_author'] += 1
                if article['citations'] > 0:
                    record_to_save['papers_with_citations'] += 1
                db_authors.save_record(record_to_save)


def compute_authors_h_index():
    db_authors = DBManager('bioinfo_authors')
    authors = db_authors.search({})
    for author in authors:
        author_citations = author['citations']
        h_index = author['papers']
        while h_index > 0:
            for citation in author_citations:
                if citation < h_index:
                    h_index -= 1
                    break
        db_authors.update_record({'name': author['name']}, {'h-index': h_index})
