from db_manager import DBManager
from googleapiclient.discovery import build
from recordlinkage import preprocessing, SortedNeighbourhoodIndex, Compare
from selenium import webdriver
from utils import curate_author_name, get_config, get_base_url, load_countries_file, get_gender
from similarity.jarowinkler import JaroWinkler

import csv
import bs4
import datetime
import googlemaps
import logging
import pathlib
import pandas as pd
import os


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
        'citations': [int(article['citations'])],
        'affiliations': '',
        'h-index': 0,
        'papers_as_last_author': 0
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


def do_compute_h_index(author):
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
    return h_index


def compute_authors_h_index(override_metric=False):
    db_authors = DBManager('bioinfo_authors')
    authors = db_authors.search({})
    for author in authors:
        if 'h-index' in author.keys() and not override_metric:
            continue
        logging.info(f"Computing h-index of {author['name']}")
        h_index = do_compute_h_index(author)
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


def fix_gender():
    """
    There are some genders that are named mostly_female or mostly_male, which should
    be converted to female and male, respectively
    """
    db_authors = DBManager('bioinfo_authors')
    authors_db = db_authors.search({'$or': [{'gender': 'mostly_male'}, {'gender': 'mostly_female'}]})
    authors = [author_db for author_db in authors_db]
    for author in authors:
        if author['gender'] == 'mostly_female':
            db_authors.update_record({'_id': author['_id']}, {'gender': 'female'})
        elif author['gender'] == 'mostly_male':
            db_authors.update_record({'_id': author['_id']}, {'gender': 'male'})


# def remove_author_duplicates():
#     db_authors = DBManager('bioinfo_authors')
#     with open('data/duplicates.csv', 'r', encoding='utf-8') as f:
#         file = csv.DictReader(f, delimiter=',')
#         duplicates = defaultdict(list)
#         for line in file:
#             duplicates[line['Cluster ID']].append(line)
#     two_duplicates = dict()
#     for cluster, dup_array in duplicates.items():
#         if len(dup_array) == 2:
#             two_duplicates[cluster] = dup_array
#     processed_clusters = []
#     # read processed duplicates
#     last_cluster = None
#     with open('data/processed_duplicates.csv', 'r') as f:
#         file = csv.DictReader(f, delimiter=',')
#         for line in file:
#             last_cluster = line['cluster']
#     try:
#         dup_counter = 0
#         total_dup = len(two_duplicates)
#         found_last = False
#         for cluster, dup_array in two_duplicates.items():
#             dup_counter += 1
#             processed_clusters.append(cluster)
#             print(f"\n\nProcessing {dup_counter}/{total_dup}")
#             if cluster == last_cluster:
#                 found_last = True
#             if found_last:
#                 authors = []
#                 for duplicate in dup_array:
#                     pprint.pprint(duplicate)
#                     author_db = db_authors.find_record({'name': duplicate['name'], 'gender': duplicate['gender']})
#                     authors.append(author_db)
#                 # ask the user if the cluster actually contains
#                 # a duplicate
#                 is_duplicate = str(input('\nDoes the cluster contain a duplicate? yes (y)/no (n) '))
#                 if is_duplicate == 'y':
#                     logging.info('Creating a new record to merge the duplicates')
#                     merged_author = dict()
#                     # Getting name
#                     name_chosen = int(input(f"Which name do you want to take "
#                                             f"(1) {authors[0]['name']}/"
#                                             f"(2) {authors[1]['name']}?"))
#                     if name_chosen == 1:
#                         merged_author['name'] = authors[0]['name']
#                     else:
#                         merged_author['name'] = authors[1]['name']
#                     # Getting gender
#                     if authors[0]['gender'] == authors[1]['gender']:
#                         merged_author['gender'] = authors[0]['gender']
#                     else:
#                         if authors[0]['gender'] == 'unknown' and authors[1]['gender'] in ['male', 'female']:
#                             merged_author['gender'] = authors[1]['gender']
#                         elif authors[1]['gender'] == 'unknown' and authors[0]['gender'] in ['male', 'female']:
#                             merged_author['gender'] = authors[0]['gender']
#                         else:
#                             gender_chosen = int(input(f"Which gender do you want to consider "
#                                                       f"(1) {authors[0]['gender']}/"
#                                                       f"(2) {authors[1]['gender']}?"))
#                             if gender_chosen == 1:
#                                 merged_author['gender'] = authors[0]['gender']
#                             else:
#                                 merged_author['gender'] = authors[1]['gender']
#                     # Getting papers
#                     merged_author['papers'] = authors[0]['papers'] + authors[1]['papers']
#                     # Getting total citations
#                     merged_author['total_citations'] = authors[0]['total_citations'] + authors[1]['total_citations']
#                     # Getting papers as first author
#                     merged_author['papers_as_first_author'] = authors[0]['papers_as_first_author'] + \
#                                                               authors[1]['papers_as_first_author']
#                     # Getting papers with citations
#                     merged_author['papers_with_citations'] = authors[0]['papers_with_citations'] + \
#                                                              authors[1]['papers_with_citations']
#                     # Getting dois and their citations
#                     merged_author['dois'] = authors[0]['dois']
#                     merged_author['citations'] = authors[0]['citations']
#                     for index, doi in enumerate(authors[1]['dois']):
#                         if doi not in merged_author['dois']:
#                             merged_author['dois'].append(doi)
#                             merged_author['citations'].append(authors[1]['citations'][index])
#                     # Getting affiliations
#                     aff_set = set()
#                     for author in authors:
#                         if 'affiliations' in author.keys():
#                             for affiliation in author['affiliations']:
#                                 aff_set.add(affiliation)
#                     merged_author['affiliations'] = list(aff_set)
#                     merged_author['h-index'] = do_compute_h_index(merged_author)
#                     db_authors.store_record(merged_author)
#                     logging.info('Removing duplicates')
#                     db_authors.remove_record({'_id': authors[0]['_id']})
#                     db_authors.remove_record({'_id': authors[1]['_id']})
#     except:
#         with open('data/processed_duplicates.csv', 'w', encoding='utf-8') as f:
#             writer = csv.DictWriter(f, fieldnames=['cluster'])
#             writer.writeheader()
#             for processed_cluster in processed_clusters:
#                 writer.writerow({'cluster': processed_cluster})


def record_linkage():
    db_authors = DBManager('bioinfo_authors')
    authors_db = db_authors.search({})
    authors = []
    for author_db in authors_db:
        # 1. Cleaning the author's name
        s_author = pd.Series(author_db['name'])
        clean_author = preprocessing.clean(s_author, strip_accents='unicode')
        author_str = clean_author.to_string(header=False, index=False).strip()
        author_str_arr = author_str.split()
        authors.append(
            {
                'first_name': ' '.join(author_str_arr[:len(author_str_arr)-1]),
                'last_name': author_str_arr[-1],
                'sex': author_db['gender']
            }
        )
    # 2. Indexing
    indexer = SortedNeighbourhoodIndex('last_name', window=9)
    authors_pd = pd.DataFrame(authors)
    candidate_links = indexer.index(authors_pd)
    # 3. Comparing
    compare_cl = Compare()
    compare_cl.string('first_name', 'first_name', method='jarowinkler', threshold=0.95, label='first_name')
    compare_cl.string('last_name', 'last_name', method='jarowinkler', threshold=0.85, label='last_name')
    compare_cl.exact('sex', 'sex', label='sex')
    features = compare_cl.compute(candidate_links, authors_pd)
    duplicates = features[features.sum(axis=1) > 2]
    dup_df = pd.DataFrame(columns=['1_first_name', '1_last_name', '1_sex', '2_first_name', '2_last_name', '2_sex'])
    for i in range(0, len(duplicates)):
        columns_dup1 = ['1_' + col_name for col_name in list(authors_pd.loc[duplicates.iloc[i].name[0]].index)]
        dup1 = pd.DataFrame([authors_pd.loc[duplicates.iloc[i].name[0]]],
                            columns=list(authors_pd.loc[duplicates.iloc[i].name[0]].index),
                            index=[i])
        dup1.columns = columns_dup1
        columns_dup2 = ['2_' + col_name for col_name in list(authors_pd.loc[duplicates.iloc[i].name[1]].index)]
        dup2 = pd.DataFrame([authors_pd.loc[duplicates.iloc[i].name[1]]],
                            columns=list(authors_pd.loc[duplicates.iloc[i].name[1]].index),
                            index=[i])
        dup2.columns = columns_dup2
        dup_df = dup_df.append(pd.concat([dup1, dup2], axis=1), ignore_index=True)
    dup_df.to_csv('data/potential_duplicates.csv', index=False)


def __get_author(db_authors, author_names):
    for author_name in author_names:
        author_db = db_authors.find_record({'name': author_name})
        if author_db:
            return author_db
        else:
            author_db = db_authors.find_record({'other_names': {'$in': [author_name]}})
            if author_db:
                return author_db
    return None


def __generate_alternative_names(name):
    name_vec = name.split()
    alternative_names = []
    alternative_name = ''
    for i in range(1, len(name_vec)):
        for j in range(0, len(name_vec)):
            if i == j:
                alternative_name = alternative_name[:len(alternative_name) - 1]
                alternative_name += '-'
            alternative_name += name_vec[j]
            if j < len(name_vec) - 1:
                alternative_name += ' '
        alternative_names.append(alternative_name)
        alternative_name = ''
    return alternative_names


def remove_author_duplicates():
    db_authors = DBManager('bioinfo_authors')
    errors = pd.DataFrame()
    with open('data/remove_duplicates_errors_fixed.csv', 'r', encoding='utf-8') as f:
        file = csv.DictReader(f, delimiter=',')
        for line in file:
            if line['is_duplicated'].strip() == 'si':
                try:
                    logging.info(f"Processing record with id: {line['id']}")
                    first_person = f"{line['1_first_name']} {line['1_last_name']}"
                    first_person_names = [first_person]
                    first_person_names.extend(__generate_alternative_names(first_person))
                    second_person = f"{line['2_first_name']} {line['2_last_name']}"
                    second_person_names = [second_person]
                    second_person_names.extend(__generate_alternative_names(second_person))
                    person_to_keep = line['person_to_keep']
                    if person_to_keep:
                        if person_to_keep == '1':
                            author_to_keep = __get_author(db_authors, first_person_names)
                            author_to_remove = __get_author(db_authors, second_person_names)
                        else:
                            author_to_keep = __get_author(db_authors, second_person_names)
                            author_to_remove = __get_author(db_authors, first_person_names)
                    else:
                        if len(first_person) >= len(second_person):
                            author_to_keep = __get_author(db_authors, first_person_names)
                            author_to_remove = __get_author(db_authors, second_person_names)
                        else:
                            author_to_keep = __get_author(db_authors, second_person_names)
                            author_to_remove = __get_author(db_authors, first_person_names)
                    if 'delete' in author_to_remove.keys():
                        logging.info(f"The author {author_to_remove['name']} has the field delete. "
                                     f"The record will be ignored")
                        continue
                    others_names = author_to_keep.get('other_names')
                    if others_names:
                        if author_to_remove['name'] not in others_names:
                            others_names.append(author_to_remove['name'])
                    else:
                        others_names = [author_to_remove['name']]
                    updated_record = {
                        'papers': author_to_keep['papers'] + author_to_remove['papers'],
                        'total_citations': author_to_keep['total_citations'] + author_to_remove['total_citations'],
                        'papers_as_first_author': author_to_keep['total_citations'] + author_to_remove['total_citations'],
                        'dois': author_to_keep['dois'] + author_to_remove['dois'],
                        'papers_with_citations': author_to_keep['papers_with_citations'] + author_to_remove['papers_with_citations'],
                        'citations': author_to_keep['citations'] + author_to_remove['citations'],
                        'h-index': author_to_keep['h-index'] if author_to_keep['h-index'] > author_to_remove['h-index'] else author_to_remove['h-index'],
                        'other_names': others_names
                    }
                    if author_to_keep.get('affiliations'):
                        updated_record['affiliations'] = author_to_keep['affiliations']
                    if author_to_remove.get('affiliations'):
                        if 'affiliations' in updated_record.keys():
                            if author_to_remove['affiliations'] not in updated_record['affiliations']:
                                updated_record['affiliations'].append(author_to_remove['affiliations'])
                        else:
                            updated_record['affiliations'] = author_to_remove['affiliations']
                    # Update record to keep
                    logging.info(f"Updating the author {author_to_keep['name']}")
                    db_authors.update_record({'name': author_to_keep['name']}, updated_record)
                    # Label record with the delete flag
                    logging.info(f"Labeling the author {author_to_remove['name']} to be deleted")
                    db_authors.update_record({'name': author_to_remove['name']}, {'delete': 1})
                except:
                    if errors.shape[0] == 0:
                        errors = pd.DataFrame(dict(line), index=[0])
                    else:
                        errors = errors.append(dict(line), ignore_index=True)
                    logging.error(f"Problem with the record: {line}")
    if errors.shape[0] > 0:
        errors.to_csv('data/remove_duplicates_errors.csv', index=False)


def label_papers_with_all_authors_with_delete_flag():
    db_papers = DBManager('bioinfo_papers')
    db_authors = DBManager('bioinfo_authors')
    papers = db_papers.search({'delete': {'$exists': 0}})
    papers_flagged = 0
    for paper in papers:
        author_names = paper['authors']
        num_authors_with_delete_flag = 0
        for author_name in author_names:
            author_db = db_authors.find_record({'name': author_name})
            namesake_author = db_authors.find_record({'other_names': {'$in': [author_name]}})
            if not author_db and not namesake_author:
                num_authors_with_delete_flag += 1
            if author_db and 'delete' in author_db.keys() and not namesake_author:
                num_authors_with_delete_flag += 1
        if 0 < len(author_names) == num_authors_with_delete_flag > 0:
            papers_flagged += 1
            logging.info(f"The flag delete will be added to the paper {paper['DOI']}")
            db_papers.update_record({'DOI': paper['DOI']}, {'delete': 1})
    logging.info(f"{papers_flagged} papers were flagged")


def compute_metric_papers_as_last_author():
    db_papers = DBManager('bioinfo_papers')
    db_authors = DBManager('bioinfo_authors')
    papers = db_papers.search({})
    db_authors.update_all_records({'papers_as_last_author': 0})
    for paper in papers:
        author_names = paper['authors']
        if len(author_names) > 0:
            last_author = author_names[-1]
            logging.info(f"Updating the record of {last_author}")
            author_db = db_authors.find_record({'name': last_author})
            if not author_db:
                author_db = db_authors.find_record({'other_names': {'$in': [last_author]}})
                if not author_db:
                    logging.info(f"Author {last_author} does not exist")
                    author_gender = get_gender(last_author)
                    author_index = len(paper['authors'])-1
                    create_author_record(last_author, author_gender, author_index, paper, db_authors)
                    author_db = db_authors.find_record({'name': last_author})
            papers_as_last_author = author_db['papers_as_last_author']
            papers_as_last_author += 1
            db_authors.update_record({'_id': author_db['_id']}, {'papers_as_last_author': papers_as_last_author})


def fix_author_metrics():
    db_authors = DBManager('bioinfo_authors')
    db_papers = DBManager('bioinfo_papers')
    authors = db_authors.search({'updated_dt': {'$exists': 0}})
    tot_authors = authors.count()
    author_counter = 1
    jarowinkler = JaroWinkler()
    for author in authors:
        logging.info(f"[{author_counter}/{tot_authors}] Updating the author: {author['name']}")
        author_names = [author['name']]
        if 'other_names' in author.keys():
            author_names.extend(author['other_names'])
        total_citations, papers_with_citations, papers_as_first_author, papers_as_last_author = 0, 0, 0, 0
        list_citations = []
        is_alternative, other_name = '', ''
        for doi in author['dois']:
            paper_db = db_papers.find_record({'DOI': doi})
            citations = int(paper_db['citations'])
            total_citations += citations
            list_citations.append(citations)
            if citations > 0:
                papers_with_citations += 1
            idx = 0
            found_author = False
            max_similarity_idx, max_similarity_score = -1, -10000
            for idx in range(0, len(paper_db['authors'])):
                if paper_db['authors'][idx] in author_names:
                    found_author = True
                    break
                else:
                    similarity_score = jarowinkler.similarity(author['name'], paper_db['authors'][idx])
                    if similarity_score > max_similarity_score:
                        max_similarity_idx = idx
                        max_similarity_score = similarity_score
            if found_author:
                if idx == 0:
                    papers_as_first_author += 1
                else:
                    if (idx+1) == len(paper_db['authors']):
                        papers_as_last_author += 1
            else:
                print(f"Could not find the author {author['name']} in the paper {doi}")
                is_alternative = str(input(f"Is {paper_db['authors'][max_similarity_idx]} an alternative name of "
                                           f"{author['name']}? (y/n) "))
                if is_alternative == 'y':
                    other_name = paper_db['authors'][max_similarity_idx]
                    if max_similarity_idx == 0:
                        papers_as_first_author += 1
                    elif (max_similarity_idx+1) == len(paper_db['authors']):
                        papers_as_last_author += 1
        author_dict = {
            'papers': len(author['dois']),
            'total_citations': total_citations,
            'papers_as_first_author': papers_as_first_author,
            'papers_with_citations': papers_with_citations,
            'citations': list_citations,
            'papers_as_last_author': papers_as_last_author,
            'updated_dt': datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        }
        if is_alternative == 'y':
            other_names = [other_name]
            if 'other_names' in author.keys():
                other_names.extend(author['other_names'])
            author_dict['other_names'] = other_names
        db_authors.update_record({'_id': author['_id']}, author_dict)
        author_counter += 1


def create_affiliation_collection():
    db_authors = DBManager('bioinfo_authors')
    db_affiliations = DBManager('bioinfo_affiliations')
    authors = db_authors.search({})
    num_affiliations = 0
    for author in authors:
        if 'affiliations' in author:
            author_affiliations = author['affiliations']
            for author_affiliation in author_affiliations:
                affiliation = db_affiliations.find_record({'name': author_affiliation})
                if affiliation:
                    logging.info(f"Updating affiliation: {author_affiliation}")
                    author_ids = affiliation['author_ids']
                    author_ids.append(author['_id'])
                    db_affiliations.update_record({'name': author_affiliation}, {'author_ids': author_ids})
                else:
                    logging.info(f"Creating affiliation: {author_affiliation}")
                    db_affiliations.store_record(
                        {'name': author_affiliation,
                         'country': '',
                         'city': '',
                         'latitude': '',
                         'longitude': '',
                         'author_ids': [author['_id']]
                         }
                    )
                    num_affiliations += 1
        else:
            logging.info(f"The author {author['name']} does not have affiliations")
    logging.info(f"Affiliations created: {num_affiliations}")


def process_affiliations():
    db_affiliations = DBManager('bioinfo_affiliations')
    api_key = ''
    gmaps = googlemaps.Client(key=f"{api_key}")
    affiliations = db_affiliations.search({})
    for affiliation in affiliations:
        geocode_result = gmaps.geocode(affiliation['name'])
        if geocode_result['status'] == 'OK':
            if len(geocode_result['results']) == 1:
                affiliation_dict = {
                    'address': geocode_result['results'][0]['formatted_address'],
                    'latitude': geocode_result['results'][0]['geometry']['location']['lat'],
                    'longitude': geocode_result['results'][0]['geometry']['location']['lng'],
                    'country': geocode_result['results'][0]['address_components'][4]['long_name'],
                    'city': geocode_result['results'][0]['address_components'][5]['long_name'],
                    'postal_code': geocode_result['results'][0]['address_components'][6]['long_name']
                }
                db_affiliations.update_record({'name': affiliation['name']}, affiliation_dict)
            else:
                db_affiliations.update_record({'name': affiliation['name']}, {'error': 'more than one result'})
                logging.info(f"More than one result returned")
        else:
            db_affiliations.update_record({'name': affiliation['name']}, {'error': geocode_result['status']})
            logging.info(f"Returned status: {geocode_result['status']}")


def __save_dataframe_to_csv(dataframe, name):
    journal_csv_file = pathlib.Path('data', 'processed').joinpath(name + '.csv')
    logging.info(f"Saving {dataframe.shape[0]} records in {journal_csv_file}")
    dataframe.to_csv(journal_csv_file, index=False)


def combine_csv_files():
    dir = pathlib.Path('data', 'raw', 'full')
    file_names = sorted(os.listdir(dir))
    journal_name, master_name = '', ''
    master_df = None
    for file_name in file_names:
        journal_name = '_'.join([token for token in file_name.split('_') if token.isalpha()])
        file_name = dir.joinpath(file_name)
        if master_name != journal_name:
            if master_df is not None:
                __save_dataframe_to_csv(master_df, master_name)
            master_df = pd.read_csv(file_name, index_col='DOI')
            master_name = journal_name
        else:
            master_df = master_df.append(pd.read_csv(file_name, index_col='DOI'), ignore_index=True)
    __save_dataframe_to_csv(master_df, master_name)
