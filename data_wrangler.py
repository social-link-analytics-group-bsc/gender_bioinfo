from db_manager import DBManager
from googleapiclient.discovery import build
from selenium import webdriver
from utils import curate_author_name, get_config, get_base_url, load_countries_file, get_gender

import bs4
import logging
import pathlib

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)
logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.ERROR)
logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)


def create_author_record(author_name, author_gender, author_index, article, db_authors):
    record_to_save = {
        'name': author_name,
        'gender': author_gender,
        'papers': 1,
        'total_citations': int(article['citations']),
        'papers_as_first_author': 1 if author_index == 0 else 0,
        'dois': [article['DOI']],
        'papers_with_citations': 1 if int(article['citations']) > 0 else 0,
        'citations': [int(article['citations'])]
    }
    db_authors.save_record(record_to_save)
    logging.info(f"Author {author_name} creado")


def update_author_record(author_in_db, author_name, author_index, author_gender, article, db_authors):
    if 'dois' in author_in_db.keys():
        author_in_db['dois'].append(article['DOI'])
        author_dois = author_in_db['dois']
    else:
        author_dois = [article['DOI']]
    if 'citations' in author_in_db.keys():
        author_in_db['citations'].append(int(article['citations']))
        author_citations = author_in_db['citations']
    else:
        author_citations = [int(article['citations'])]
    if 'total_citations' in author_in_db.keys():
        total_citations = author_in_db['total_citations'] + int(article['citations'])
    else:
        total_citations = int(article['citations'])
    values_to_update = {
        'papers': author_in_db['papers'] + 1 if 'papers' in author_in_db.keys() else 1,
        'total_citations': total_citations,
        'dois': author_dois,
        'citations': author_citations
    }
    if author_index == 0:
        if 'papers_as_first_author' in author_in_db.keys():
            values_to_update['papers_as_first_author'] = author_in_db['papers_as_first_author'] + 1
        else:
            values_to_update['papers_as_first_author'] = 1
    if int(article['citations']) > 0:
        if 'papers_with_citations' in author_in_db.keys():
            values_to_update['papers_with_citations'] = author_in_db['papers_with_citations'] + 1
        else:
            values_to_update['papers_with_citations'] = 1
    # check if the stored gender of the author is unknown, if
    # this is the case replace with the current one
    if author_in_db.get('gender') == 'unknown':
        values_to_update['gender'] = author_gender
    if author_gender != 'unknown' and author_gender != author_in_db.get('gender'):
        logging.warning(f"Author {author_name}'s with gender inconsistency. "
                        f"Stored {author_in_db.get('gender')}. Article (doi {article['DOI']}) author_gender")
    db_authors.update_record({'name': author_name}, values_to_update)
    logging.info(f"Actualizado author {author_name}")


def create_update_paper_authors_collection():
    db_papers = DBManager('bioinfo_papers')
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
                update_author_record(author_in_db, author, index, author_gender, article, db_authors)
            else:
                create_author_record(author, author_gender, index, article, db_authors)


def update_author_metrics():
    """
    Update the authors' metrics: papers with citations and papers as first author
    :return:
    """
    db_authors = DBManager('bioinfo_authors')
    db_papers = DBManager('bioinfo_papers')
    authors_db = db_authors.search({})
    authors = [author_db for author_db in authors_db]
    processed_authors = []
    total_authors = len(authors)
    for num_author, author in enumerate(authors):
        logging.info(f"Processing author: {author['name']} ({num_author+1}/{total_authors})")
        for doi in author['dois']:
            paper_db = db_papers.find_record({'DOI': doi})
            if paper_db:
                index = paper_db['authors'].index(author['name'])
                if author['name'] not in processed_authors:
                    # First time
                    processed_authors.append(author['name'])
                    new_vals = {
                        'papers_with_citations': 1 if int(paper_db['citations']) > 0 else 0,
                        'papers_as_first_author': 1 if index == 0 else 0
                    }
                else:
                    papers_with_citations = author['papers_with_citations']
                    if int(paper_db['citations']) > 0:
                        papers_with_citations += 1
                    papers_as_first_author = author['papers_as_first_author']
                    if index == 0:
                        papers_as_first_author += + 1
                    new_vals = {
                        'papers_with_citations': papers_with_citations,
                        'papers_as_first_author': papers_as_first_author
                    }
                db_authors.update_record({'name': author['name']}, new_vals)


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
    for author in authors_list:
        logging.info(f"Curating the name of the author {author['name']}")
        author_name = curate_author_name(author['name'])
        db_authors.update_record({'name': author['name']}, {'name': author_name})


def compute_paper_base_url():
    db_papers = DBManager('bioinfo_papers')
    papers = db_papers.search({})
    papers_list = [paper_db for paper_db in papers]
    for paper in papers_list:
        base_url = get_base_url(paper['link'])
        logging.info(f"Paper full url {paper['link']}, base url {base_url}")
        db_papers.update_record({'DOI': paper['DOI']}, {'base_url': base_url})


def __update_with_ncbi_link(res_item, paper, driver, db_papers):
    if res_item['title'].replace('...', '').lower() in paper['title'].lower():
        driver.get(res_item['link'])
        soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')
        h1s = soup.find_all('h1')
        for h1 in h1s:
            page_title = h1.text.rstrip('.')
            if page_title.lower() == paper['title'].lower():
                base_url = get_base_url(res_item['link'])
                db_papers.update_record({'DOI': paper['DOI']},
                                        {'source_link': paper['link'],
                                         'link': res_item['link'],
                                         'base_url': base_url})
                return True
        return False
    else:
        return False


def search_ncbi_links():
    current_dir = pathlib.Path(__file__).parents[0]
    config_fn = current_dir.joinpath('config.json')
    config = get_config(config_fn)
    PMC_URL = 'https://www.ncbi.nlm.nih.gov/pmc'
    PUBMED_URL = 'https://www.ncbi.nlm.nih.gov/pubmed'
    db_papers = DBManager('bioinfo_papers')
    papers = db_papers.search({'base_url': 'https://journals.plos.org', 'source_link': {'$exists': 0}})
    papers_list = [paper_db for paper_db in papers]
    records_to_update = len(papers_list)
    driver = webdriver.Chrome()
    updated_records = 0
    service = build('customsearch', 'v1', developerKey=config['google_search']['key'], cache_discovery=False)
    for paper in papers_list:
        logging.info(f"Looking for the NCBI link of the paper {paper['DOI']}")
        res = service.cse().list(q=paper['title'], cx=config['google_search']['cx']).execute()
        res_items = res['items']
        found_ncbi_link = False
        for res_item in res_items:
            if PMC_URL in res_item['link']:
                found_ncbi_link = __update_with_ncbi_link(res_item, paper, driver, db_papers)
        if not found_ncbi_link:
            for res_item in res_items:
                if PUBMED_URL in res_item['link']:
                    found_ncbi_link = __update_with_ncbi_link(res_item, paper, driver, db_papers)
        if found_ncbi_link:
            logging.info(f"Updated the link of the paper {paper['DOI']}")
            updated_records += 1
        records_to_update -= 1
        logging.info(f"Remaining {records_to_update} papers")
    logging.info(f"It have been updated the links of {updated_records} papers")


def curate_author_affiliation_country():
    db_authors = DBManager('bioinfo_authors')
    authors = db_authors.search({})
    authors_list = [author for author in authors]
    total_authors = len(authors_list)
    countries = load_countries_file()
    for author in authors_list:
        logging.info(f"Curating the country and affiliation of the author: {author['name']}")
        author_dict = {'affiliations': [], 'countries': []}
        for affiliation in author['affiliations']:
            affiliation_country = affiliation.split(',')[-1].strip()
            if affiliation_country in countries['names']:
                author_dict['countries'].append(affiliation_country)
                author_dict['affiliations'].append(affiliation)
            else:
                logging.info(f"[AUTHOR CURATE]: Affiliation discarded because could not find "
                             f"its country {affiliation}")
        db_authors.update_record({'name': author['name']}, author_dict)
        total_authors -= 1
        logging.info(f"Remaining authors {total_authors}")


def complete_author_genders():
    db_papers = DBManager('bioinfo_papers')
    db_authors = DBManager('bioinfo_authors')
    papers_db = db_papers.search({'authors': {'$exists': 1}, 'authors_gender': {'$exists': 0},
                                  'link': {'$ne': 'https://dx.doi.org/'}})
    papers = [paper_db for paper_db in papers_db]
    for paper in papers:
        authors = paper['authors']
        author_genders = []
        for index, author in enumerate(authors):
            author_db = db_authors.find_record({'name': author})
            if author_db:
                if 'gender' in author_db.keys():
                    author_genders.append(author_db['gender'])
                else:
                    author_genders.append(get_gender(author))
            else:
                author_gender = get_gender(author)
                author_genders.append(author_gender)
                create_author_record(author, author_gender, index, paper, db_authors)
        db_papers.update_record({'DOI': paper['DOI']}, {'authors_gender': author_genders})
