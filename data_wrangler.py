
from db_manager import DBManager
from utils import curate_author_name

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
                logging.info(f"Author {author} creado")


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


def clean_author_countries():
    db_authors = DBManager('bioinfo_authors')
    authors = db_authors.search({})
    countries = {'names': [], 'prefixes': []}
    with open(str('data/country_list.txt'), 'r') as f:
        for _, line in enumerate(f):
            line = line.split(':')
            countries['names'].append(line[1].replace('\n', ''))
            countries['prefixes'].append(line[0].replace('\n', ''))
    countries['names'].extend(['UK', 'USA'])
    for author in authors:
        author_current_countries = author['countries']
        countries_to_save = []
        for author_country in author_current_countries:
            if author_country in countries['names']:
                countries_to_save.append(author_country)
            else:
                pass
        logging.info(f"Update the record of the {author['name']}")
        db_authors.update_record({'name': author['name']}, {'countries': countries_to_save})


def fix_author_doi_list():
    db_authors = DBManager('bioinfo_authors')
    db_papers = DBManager('bioinfo_papers')
    authors = db_authors.search({})
    authors_list = [author_db for author_db in authors]
    for author in authors_list:
        logging.info(f"Checking the dois of the author {author['name']}")
        dois_to_remove = []
        for doi in author['dois']:
            paper = db_papers.find_record({'DOI': doi})
            if not paper:
                logging.warning(f"Could not find the publication with the doi {doi}")
            found_author = False
            for paper_author in paper['authors']:
                if paper_author.lower() == author['name'].lower():
                    found_author = True
                    break
            if not found_author:
                logging.info(f"Found that the author is not part of the authors of the paper {doi}")
                dois_to_remove.append(doi)
        for wrong_doi in dois_to_remove:
            author['dois'].remove(wrong_doi)
            logging.info(f"Removing doi {wrong_doi} from the author's list of dois")
        db_authors.update_record({'name': author['name']}, {'dois': author['dois']})


def curate_paper_list_authors():
    db_papers = DBManager('bioinfo_papers')
    papers = db_papers.search({})
    papers_list = [paper_db for paper_db in papers]
    for paper in papers_list:
        logging.info(f"Curating the list of authors of the paper {paper['DOI']}")
        curated_author_names = []
        for author in paper['authors']:
            curated_author_names.append(curate_author_name(author))
        db_papers.update_record({'DOI': paper['DOI']}, {'authors': curated_author_names})


def curate_authors_name():
    db_authors = DBManager('bioinfo_authors')
    authors = db_authors.search({})
    authors_list = [author_db for author_db in authors]
    found_last = False
    for author in authors_list:
        if author['name'] == 'Josep A Calduch-Giner':
            found_last = True
        if found_last:
            logging.info(f"Curating the name of the author {author['name']}")
            author_name = curate_author_name(author['name'])
            db_authors.update_record({'name': author['name']}, {'name': author_name})


def compute_paper_base_url():
    db_papers = DBManager('bioinfo_papers')
    papers = db_papers.search({})
    papers_list = [paper_db for paper_db in papers]
    for paper in papers_list:
        base_url = ''
        slash_counter = 0
        for c in paper['link']:
            if c == '/':
                slash_counter += 1
            if slash_counter <= 2:
                base_url += c
            else:
                break
        logging.info(f"Paper full url {paper['link']}, base url {base_url}")
        db_papers.update_record({'DOI': paper['DOI']}, {'base_url': base_url})
