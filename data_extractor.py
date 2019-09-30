import bs4
import json
import logging
import pathlib
import random
import re
import requests
import time

from data_wrangler import create_author_record, update_author_record
from db_manager import DBManager
from pubmed import EntrezClient
from selenium import webdriver
from utils import curate_author_name, curate_affiliation_name, load_countries_file, get_gender, title_except, get_config, \
                  get_base_url
from urllib import parse, request


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


###
# Class to extract the authors' affiliation from the
# online publication of the papers
###
class AuthorAffiliationExtractor:
    db_authors, db_papers = None, None
    countries, driver = None, None

    def __init__(self):
        self.db_authors = DBManager('bioinfo_authors')
        self.db_papers = DBManager('bioinfo_papers')
        self.countries = load_countries_file()
        self.driver = webdriver.Chrome()

    def __del__(self):
        self.driver.close()

    def __get_country_from_affiliation(self, affiliation):
        found_countries = []
        for country in self.countries['names']:
            regex_country = re.compile(f", {country}$")
            if regex_country.search(affiliation):
                found_countries.append(country)
        if found_countries:
            if len(found_countries) > 1:
                logging.warning(f"Found more than one country in the affiliation {', '.join(found_countries)}")
            return found_countries[0]
        else:
            return None

    def __record_author_affiliation(self, affiliation, dict_authors, author_name):
        curated_affiliation = curate_affiliation_name(affiliation)
        if 'affiliations' not in dict_authors[author_name].keys():
            dict_authors[author_name]['affiliations'] = [curated_affiliation]
        else:
            dict_authors[author_name]['affiliations'].append(curated_affiliation)
        return curated_affiliation

    def __record_affiliation_country(self, affiliation, dict_authors, author_name):
        curated_affiliation = curate_affiliation_name(affiliation)
        affiliation_country = self.__get_country_from_affiliation(curated_affiliation)
        if affiliation_country:
            if 'countries' not in dict_authors[author_name].keys():
                dict_authors[author_name]['countries'] = [affiliation_country]
            else:
                dict_authors[author_name]['countries'].append(affiliation_country)

    def __update_author_affiliation_and_country(self, dict_authors, paper):
        new_vals = {}
        authors, authors_gender = [], []
        for author_name, val in dict_authors.items():
            author_db = self.db_authors.find_record({'name': author_name})
            authors.append(author_name)
            if author_db and 'gender' in author_db.keys():
                author_gender = author_db['gender']
            else:
                author_gender = get_gender(author_name)
            authors_gender.append(author_gender)
            if author_db:
                if 'countries' in val.keys():
                    if 'countries' not in author_db.keys():
                        new_vals['countries'] = val['countries']
                    else:
                        new_vals['countries'] = author_db['countries']
                        new_vals['countries'].extend(val['countries'])
                        new_vals['countries'] = list(set(new_vals['countries']))
                if 'affiliations' in val.keys():
                    if 'affiliations' not in author_db.keys():
                        new_vals['affiliations'] = val['affiliations']
                    else:
                        new_vals['affiliations'] = author_db['affiliations']
                        new_vals['affiliations'].extend(val['affiliations'])
                        new_vals['affiliations'] = list(set(new_vals['affiliations']))
                if new_vals:
                    self.db_authors.update_record({'name': author_name}, new_vals)
            else:
                record_to_save = {
                    'name': author_name,
                    'gender': author_gender,
                    'papers': 1,
                    'total_citations': int(paper['citations']),
                    'papers_as_first_author': 0,
                    'dois': [paper['DOI']],
                    'papers_with_citations': 1 if int(paper['citations']) > 0 else 0,
                    'citations': [int(paper['citations'])],
                    'affiliations': val['affiliations'],
                    'countries': val['countries']
                }
                self.db_authors.store_record(record_to_save)
        if 'authors' not in paper.keys():
            self.db_papers.update_record({'DOI': paper['DOI']}, {'authors': authors, 'authors_gender': authors_gender})

    def __obtain_author_info_academic(self, paper):
        html = self.driver.page_source
        soup = bs4.BeautifulSoup(html, 'html.parser')
        elements = soup.find_all(class_='info-card-author')
        dict_authors = dict()
        author_name = None
        for element in elements:
            if element.name == 'div':
                for child in element.children:
                    if child.name == 'div':
                        if 'name-role-wrap' in child.attrs['class']:
                            author_name = curate_author_name(child.text)
                            dict_authors[author_name] = {'affiliations': [], 'countries': []}
                        if 'info-card-affilitation' in child.attrs['class']:
                            for descendant in child.contents:
                                if descendant != '\n':
                                    for content in descendant.contents:
                                        if isinstance(content, bs4.element.NavigableString):
                                            curated_affiliation = curate_affiliation_name(content)
                                            dict_authors[author_name]['affiliations'].append(curated_affiliation)
                                            affiliation_country = self.__get_country_from_affiliation(curated_affiliation)
                                            if affiliation_country:
                                                dict_authors[author_name]['countries'].append(affiliation_country)
        self.__update_author_affiliation_and_country(dict_authors, paper)

    def __obtain_author_info_ncbi(self, paper):
        html = self.driver.page_source
        soup = bs4.BeautifulSoup(html, 'html.parser')
        dict_authors = dict()
        # Get authors' names and superscripts
        elements = list(soup.find('div', class_='contrib-group fm-author').children)
        for element in elements:
            if element.name == 'a':
                c_author = curate_author_name(element.text)
                dict_authors[c_author] = {'indexes': []}
            if element.name == 'sup':
                author_index = curate_affiliation_name(element.text)
                if author_index.isdigit():
                    dict_authors[c_author]['indexes'].append(author_index)
        # Get authors' affiliations
        author_affiliations = list(soup.find_all('div', class_='fm-affl'))
        index = '0'
        for affiliation in author_affiliations:
            aff_children = list(affiliation.children)
            for aff_child in aff_children:
                if 'sup' == aff_child.name:
                    index = aff_child.text
                    continue
                else:
                    for author_name, val in dict_authors.items():
                        if val['indexes']:
                            if index in val['indexes']:
                                curated_affiliation = self.__record_author_affiliation(aff_child, dict_authors,
                                                                                       author_name)
                                self.__record_affiliation_country(curated_affiliation, dict_authors, author_name)
        # Update authors' information
        self.__update_author_affiliation_and_country(dict_authors, paper)

    def __obtain_author_info_bmc(self, paper):
        elements = self.driver.find_elements_by_class_name('AuthorName')
        dict_authors = dict()
        for index, element in enumerate(elements):
            author_name = element.text
            dict_authors[author_name] = {'affiliations': [], 'countries': [], 'index': index}
            element.click()
            affs = self.driver.find_elements_by_class_name('tooltip-tether__indexed-item')
            for aff in affs:
                author_affiliation = curate_affiliation_name(aff.text)
                affiliation_country = self.__get_country_from_affiliation(author_affiliation)
                if affiliation_country:
                    dict_authors[author_name]['countries'].append(affiliation_country)
                    dict_authors[author_name]['affiliations'].append(author_affiliation)
                else:
                    logging.warning(f"Affiliation discarded, could not find its country {author_affiliation}")
        # Update authors' information
        self.__update_author_affiliation_and_country(dict_authors, paper)

    def __get_subsequent_str(self, affiliation, enriched_country, char_to_find):
        idx_occurrence = affiliation.index(enriched_country)
        len_country = len(enriched_country)
        idx_start_subsequent_str = idx_occurrence + len_country
        if idx_start_subsequent_str <= len(affiliation) - 1:
            rel_idx_end_subsequent_str = affiliation[idx_start_subsequent_str:].find(char_to_find)
            if rel_idx_end_subsequent_str > -1:
                idx_end_subsequent_str = idx_start_subsequent_str + rel_idx_end_subsequent_str
                return affiliation[idx_start_subsequent_str:idx_end_subsequent_str]
            else:
                return ''
        return ''

    def __parse_affiliation(self, affiliation, author_countries, match_pattern, country, countries):
        # It might happen that the occurrence refers to a city that has the same
        # name of a country (e.g., Georgia), so I checked if the subsequent
        # term in the affiliation is a country. If it is a country
        # then I assume the occurrence is a city otherwise is a country
        if match_pattern in affiliation:
            subsequent_str = self.__get_subsequent_str(affiliation, match_pattern, ',')
            if not subsequent_str:
                subsequent_str = self.__get_subsequent_str(affiliation, match_pattern, '\n')
            subsequent_str = subsequent_str.lstrip(',').rstrip(',').rstrip('\n').strip()
            if title_except(subsequent_str) not in countries['names']:
                num_occurances = affiliation.count(match_pattern)
                for i in range(0, num_occurances):
                    author_countries.append(title_except(country))
                affiliation = affiliation.replace(match_pattern, '___')
        return affiliation

    def __obtain_author_info_plos(self, paper):
        html = self.driver.page_source
        soup = bs4.BeautifulSoup(html, 'html.parser')
        elements = soup.find_all('li', {'data-js-tooltip': 'tooltip_trigger'})
        countries = load_countries_file()
        regex_aff = re.compile(r'\bAffiliations?\b')
        dict_authors = dict()
        for element in elements:
            author_name = curate_author_name(element.find('a', {'class': 'author-name'}).text)
            dict_authors[author_name] = {'affiliations': [], 'countries': []}
            raw_affiliation = element.find('p', {'id': re.compile('^authAffiliations-')}).text
            raw_affiliation = regex_aff.sub('', raw_affiliation).strip()
            raw_affiliation = ' '.join(raw_affiliation.split())  # remove duplicate whitespaces and newline characters
            raw_affiliation += '\n'
            raw_affiliation = raw_affiliation.lower()
            author_countries = []
            for country in countries['names']:
                country = country.lower()
                # Look for the occurrences of country names in the text of the affiliation.
                # To avoid mismatching the country names should be preceded by a comma or semicolon
                # and a white space and should end with a comma (match case 1) or
                # newline character (match case 2).
                #
                # Match Case 1
                match_case_1_comma = ', ' + country + ','
                raw_affiliation = self.__parse_affiliation(raw_affiliation, author_countries,
                                                           match_case_1_comma, country, countries)
                match_case_1_semicolon = '; ' + country + ','
                raw_affiliation = self.__parse_affiliation(raw_affiliation, author_countries,
                                                           match_case_1_semicolon, country, countries)
                # Match Case 2
                match_case_2_comma = ', ' + country + '\n'
                raw_affiliation = self.__parse_affiliation(raw_affiliation, author_countries,
                                                           match_case_2_comma, country, countries)
                match_case_2_semicolon = '; ' + country + '\n'
                raw_affiliation = self.__parse_affiliation(raw_affiliation, author_countries,
                                                           match_case_2_semicolon, country, countries)
            author_affiliations = set()
            for idx, aff in enumerate(raw_affiliation.split('___')):
                curated_affiliation = title_except(curate_affiliation_name(aff))
                if len(curated_affiliation) > 1:
                    author_affiliations.add(curated_affiliation + ', ' + author_countries[idx])
            dict_authors[author_name]['affiliations'] = list(author_affiliations)
            dict_authors[author_name]['countries'] = list(set(author_countries))
        # Update authors' information
        self.__update_author_affiliation_and_country(dict_authors, paper)

    def __do_obtain_affiliation(self, paper):
        logging.info(f"Obtaining affiliation of the author of the paper with DOI: {paper['DOI']}")
        if 'link' in paper.keys():
            self.driver.get(paper['link'])
            if 'academic.oup.com' in paper['base_url']:
                self.__obtain_author_info_academic(paper)
            elif 'ncbi.nlm.nih.gov' in paper['base_url']:
                self.__obtain_author_info_ncbi(paper)
            elif 'bmcgenomics.biomedcentral.com' in paper['base_url'] or \
                 'bmcbioinformatics.biomedcentral.com' in paper['base_url']:
                self.__obtain_author_info_bmc(paper)
            elif 'journals.plos.org' in paper['base_url']:
                self.__obtain_author_info_plos(paper)
            else:
                logging.warning(f"Unknown the domain name of the paper link {paper['link']}")
        else:
            logging.error(f"Paper with doi {paper['DOI']} does not have a link")

    def obtain_author_affiliation_from_paper(self, query):
        papers_db = self.db_papers.search(query)
        papers = [paper_db for paper_db in papers_db]
        logging.info(f"Going to process {len(papers)} papers")
        for paper in papers:
            if paper['link'] == 'https://dx.doi.org/':
                continue
            self.__do_obtain_affiliation(paper)

    def obtain_affiliation_from_author(self):
        authors = self.db_authors.search({'affiliations': {'$exists': 0}})

        for author in authors:
            for doi in author['dois']:
                paper = self.db_papers.find_record({'DOI': doi})
                if paper:
                    self.__do_obtain_affiliation(paper)
                else:
                    logging.info(f"Could not find a paper with the doi {doi}")

    def obtain_affiliation_from_papers_in_file(self, filename):
        file_counter = 0
        processed_links = 0
        with open(str(filename), 'r') as f:
            for _, link in enumerate(f):
                processed_links += 1
                link = link.replace('\n', '')
                if link == 'https://dx.doi.org/':
                    continue
                paper = self.db_papers.find_record({'link': link})
                if paper:
                    logging.info(f"Processed Links: {processed_links}")
                    try:
                        self.__do_obtain_affiliation(paper)
                    except Exception as e:
                        logging.error(e)
                        file_counter += 1
                        with open('data/unprocessed_articles.txt', 'a', encoding='utf-8') as f:
                            f.write(f"{file_counter}- {link} ({e})")
                            f.write('\n')
                else:
                    logging.info(f"Could not find a paper with the link {link}")


def is_robot_page(driver):
    try:
        robot_element = driver.find_element_by_xpath("//button[@id='btnSubmit']")
        return robot_element and robot_element.text.lower() == 'take me to my content'
    except:
        return False


def process_robot_page(doi_link, driver):
    while True:
        time_to_sleep = random.randint(200, 300)
        logging.info(f"Going to sleep for {time_to_sleep} seconds")
        time.sleep(time_to_sleep)
        # after waiting some time, try again
        driver.get("https://dx.doi.org/")
        element = driver.find_element_by_xpath("//input[@name='hdl'][@type='text']")
        element.send_keys(str(doi_link))
        element.submit()
        if is_robot_page(driver):
            continue
        if 'unavailable' in driver.current_url:
            time.sleep(2)
            return None
        else:
            time_to_sleep = random.randint(5, 10)
            time.sleep(time_to_sleep)
            return driver.current_url


def process_article_page(doi_link, driver, count, start, db):
    authors = get_authors(driver)
    now = time.time()
    time_from_beginning = now - start
    logging.info(f"{count}, {time_from_beginning}")
    time_to_sleep = random.randint(5, 10)
    time.sleep(time_to_sleep)
    db.update_record({'DOI': doi_link}, {'link': driver.current_url, 'authors': authors})


def get_authors(driver):
    html = driver.page_source
    soup = bs4.BeautifulSoup(html, 'html.parser')
    elements = soup.find_all('a', class_='linked-name')
    authors = []
    for element in elements:
        authors.append(element.text)
    return authors


def get_authors_links_untrackable_journals(doi_list, db):
    driver = webdriver.Chrome()
    start = time.time()

    for count, doi_link in enumerate(doi_list):
        if doi_link is not None:
            logging.info(f"Processing article with DOI: {doi_link}")
            driver.get("https://dx.doi.org/")
            element = driver.find_element_by_xpath("//input[@name='hdl'][@type='text']")
            element.send_keys(str(doi_link))
            element.submit()
            if 'unavailable' in driver.current_url:
                # page not found...
                logging.info('Page not found!')
                time.sleep(2)
                db.update_record({'DOI': doi_link}, {'link': None, 'authors': None})
            elif is_robot_page(driver):
                # we are detected as robots...
                logging.info('We were detected as robot :(')
                link_to_append = process_robot_page(doi_link, driver)
                if not None:
                    process_article_page(doi_link, driver, count, start, db)
                else:
                    db.update_record({'DOI': doi_link}, {'link': link_to_append, 'authors': None})
            else:
                logging.info('Getting the article link and authors')
                process_article_page(doi_link, driver, count, start, db)
        else:
            db.update_record({'DOI': doi_link}, {'link': None, 'authors': None})

    driver.close()

    return


def get_authors_ncbi_journal(db):
    driver = webdriver.Chrome()
    regex = re.compile("[^\w\s]")

    ncbi_papers = db.search({'authors_gender': {'$exists':0}})
    for paper in ncbi_papers:
        logging.info(f"Processing article with DOI: {paper['DOI']}")
        driver.get(paper['link'])
        authors = driver.find_element_by_class_name("fm-author").find_elements_by_xpath(".//*")
        paper_authors = []
        for author in authors:
            paper_author = regex.sub('', author.text)
            if paper_author != '':
                paper_authors.append(paper_author)
        if paper_authors:
            db.update_record({'DOI': paper['DOI']}, {'link': driver.current_url, 'authors': paper_authors})
        else:
            db.update_record({'DOI': paper['DOI']}, {'link': driver.current_url, 'authors': None})
    return True


def gender_id(article):
    genders = []
    for person in article['authors']:
        author_gender = get_gender(person)
        genders.append(author_gender)

    return genders


def obtain_author_gender(db):
    articles = db.search({'authors': {'$exists': 1, '$ne': None}, 'authors_gender': {'$exists': 0}})
    for article in articles:
        logging.info(f"Finding out the gender of the authors {article['authors']} of the paper {article['DOI']}")
        genders = gender_id(article)
        logging.info(f"Genders identified: {genders}")
        db.update_record({'DOI': article['DOI']}, {'authors_gender': genders})


def get_paper_links(db_papers):
    driver = webdriver.Chrome()
    papers = db_papers.search({'link': {'$exists': 0}})
    links = []
    try:
        for paper in papers:
            logging.info(f"Getting the link of the paper {paper['DOI']}")
            driver.get("https://dx.doi.org/")
            element = driver.find_element_by_xpath("//input[@name='hdl'][@type='text']")
            element.send_keys(str(paper['DOI']))
            element.submit()
            if 'unavailable' in driver.current_url:
                # page not found...
                logging.info('Page not found!')
                db_papers.update_record({'DOI': paper['DOI']}, {'link': None, 'authors': None})
                time.sleep(2)
            elif is_robot_page(driver):
                # we are detected as robots...
                logging.info('We were detected as robot :(')
                paper_link = process_robot_page(paper['DOI'], driver)
                if not None:
                    db_papers.update_record({'DOI': paper['DOI']}, {'link': paper_link})
                    links.append(paper_link)
                else:
                    db_papers.update_record({'DOI': paper['DOI']}, {'link': None, 'authors': None})
            else:
                paper_link = driver.current_url
                db_papers.update_record({'DOI': paper['DOI']}, {'link': paper_link})
                links.append(paper_link)
    except Exception as e:
        logging.error(e)
    finally:
        logging.info(f"Found {len(links)} papers without links")
        current_dir = pathlib.Path(__file__).parents[0]
        fn = current_dir.joinpath('data', 'papers_without_links.txt')
        with open(str(fn), 'a', encoding='utf-8') as f:
            for link in links:
                f.write(link)
                f.write('\n')


# Untrackable journals are those in which the name of the journal
# is not part of the article links, so we had to determine them by
# querying dx.doi.org with article doi
def extract_data_untrackable_journals(db):
    # Collect links and authors from oxford bioinformatics
    oxford_bioinformatics = db.search({'source': 'oxford bioinformatics', 'link': {'$exists': 0},
                                       'authors': {'$exists': 0}})
    list_doi_oxford_bioinformatics = [article['DOI'] for article in oxford_bioinformatics]
    if len(list_doi_oxford_bioinformatics) > 0:
        get_authors_links_untrackable_journals(list_doi_oxford_bioinformatics, db)

    # Collect links and authors from nucleic acids research
    nucleic_bioinformatics = db.search({'source': 'nucleic acids research', 'link': {'$exists': 0},
                                        'authors': {'$exists': 0}})
    list_DOI_nucleic_acids_research = [article['DOI'] for article in nucleic_bioinformatics]
    if len(list_DOI_nucleic_acids_research) > 0:
        get_authors_links_untrackable_journals(list_DOI_nucleic_acids_research, db)


def convert_dois_to_pubmed_ids():
    logging.info("Getting the pubmed id of papers...")
    URL = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?'
    BATCH_SIZE = 200
    db_papers = DBManager('bioinfo_papers')
    papers_db = db_papers.search({'pubmed_id': {'$exists': 0}})
    papers = [paper for paper in papers_db]
    doi_count = 0
    doi_batch = []
    jd = json.JSONDecoder()
    config_file = get_config('config.json')
    request_counter = 0
    logging.info(f"Looking for the pubmed id of {len(papers)} papers")
    for paper in papers:
        doi = paper['DOI']
        doi_batch.append(doi)
        doi_count += 1
        if doi_count < BATCH_SIZE:
            continue
        request_counter += 1
        logging.info(f"Doing the request number {request_counter} to convert dois to pubmed ids. "
                     f"Total papers: {len(doi_batch)}")
        data = {
            'ids': ','.join(doi_batch),
            'idtype': 'doi',
            'format': 'json',
            'email': config_file['pubmed']['email'],
            'tool': config_file['pubmed']['tool']
        }
        request_data = parse.urlencode(data)
        try:
            req = request.Request(URL, data=request_data.encode('utf-8'))
            socket = request.urlopen(req)
            response = socket.read()
            j_response = jd.decode(response.decode('utf-8'))
            if j_response['status'] == 'ok':
                res_json = j_response['records']
                for record in res_json:
                    if 'pmid' in record.keys():
                        db_papers.update_record({'DOI': record['doi']}, {'pubmed_id': record['pmid']})
            else:
                raise Exception(f"The request returned the status {response['status']}")
            time.sleep(1)
            doi_batch = []
            doi_count = 0
        except Exception as e:
            logging.error(e)


def get_paper_authors_from_pubmed(remove_author_field_from_records=False):
    PMC_URL = 'https://www.ncbi.nlm.nih.gov/pmc/articles/'
    ec = EntrezClient()
    db_papers = DBManager('bioinfo_papers')
    db_authors = DBManager('bioinfo_authors')
    if remove_author_field_from_records:
        db_papers.remove_field_from_all_records({'authors': '', 'authors_gender': ''})
    papers_with_pmid = db_papers.search({'pubmed_id': {'$exists': 1}, 'authors': {'$exists': 0}})
    papers = [paper_with_pmid for paper_with_pmid in papers_with_pmid]
    pm_ids = []
    for paper in papers:
        pm_ids.append(paper['pubmed_id'])
    total_ids = len(pm_ids)
    batch_size = 600 if total_ids > 600 else total_ids
    total_chuncks = int(round(total_ids/batch_size,0))
    start_chunk = 0
    end_chunk = batch_size
    authors_list = set()
    for chunk in range(0, total_chuncks):
        try:
            logging.info(f"Getting information from the chunk {chunk + 1} of papers. {batch_size} papers in the chunk.")
            results = ec.fetch_in_bulk_from_list(pm_ids[start_chunk:end_chunk])
            # Process results
            for result in results:
                pm_id = result['MedlineCitation']['PMID']
                article_meta_data = result['MedlineCitation']['Article']
                if 'AuthorList' in article_meta_data.keys():
                    paper = db_papers.find_record({'pubmed_id': pm_id})
                    logging.info(f"Processing paper {article_meta_data['ArticleTitle']} (PMID: {pm_id})")
                    authors = article_meta_data['AuthorList']
                    paper_authors, gender_authors = [], []
                    for index, author in enumerate(authors):
                        if 'ForeName' in author.keys():
                            author_name = author['ForeName'] + ' ' + author['LastName']
                            author_db = db_authors.find_record({'name': author_name})
                            if author_db:
                                if 'gender' not in author_db.keys():
                                    author_gender = get_gender(author_name)
                                else:
                                    author_gender = author_db['gender']
                                # If author exists, update their record
                                update_author_record(author_db, author_name, index, author_gender, paper, db_authors)
                            else:
                                author_gender = get_gender(author_name)
                                # if author doesn't exist, create a record
                                create_author_record(author_name, author_gender, index, paper, db_authors)
                            # Add author and they gender to the arrays of author names and genders
                            if author_name not in paper_authors:
                                paper_authors.append(author_name)
                                gender_authors.append(author_gender)
                            if len(author['AffiliationInfo']) > 0:
                                affiliations = []
                                # Update author's affiliations
                                for affiliation in author['AffiliationInfo']:
                                    aff_list = [curate_affiliation_name(aff) for aff in affiliation['Affiliation'].split(';')]
                                    affiliations.extend(aff_list)
                                if author_name in authors_list:
                                    existing_affiliations = author_db['affiliations']
                                    existing_affiliations.extend(affiliations)
                                    db_authors.update_record({'name': author_name},
                                                             {'affiliations': list(set(existing_affiliations))})
                                else:
                                    db_authors.update_record({'name': author_name}, {'affiliations': affiliations})
                                authors_list.add(author_name)
                    # Update paper's authors
                    db_papers.update_record({'pubmed_id': pm_id}, {'authors': paper_authors,
                                                                   'authors_gender': gender_authors})
                else:
                    if result['PubmedData'].get('ArticleIdList'):
                        for other_id in result['PubmedData']['ArticleIdList']:
                            if 'pmc' in other_id.attributes.values():
                                logging.info(f"Updating the link of the paper pubmed_id={pm_id}")
                                pmc_id = other_id.title().upper()
                                pmc_link = PMC_URL + str(pmc_id) + '/'
                                r = requests.get(pmc_link)
                                if r.status_code == 200:
                                    db_papers.update_record({'pubmed_id': pm_id}, {'link': pmc_link,
                                                                                   'base_url': get_base_url(pmc_link)})

            # Update indexes
            start_chunk = end_chunk
            end_chunk += batch_size
            time.sleep(1)
        except Exception as e:
            logging.error(e)


def get_paper_author_names_from_pubmed():
    ec = EntrezClient()
    db_papers = DBManager('bioinfo_papers')
    db_authors = DBManager('bioinfo_authors')
    papers_with_pmid = db_papers.search({'pubmed_id': {'$exists': 1}, 'authors': {'$exists': 0}})
    papers = [paper_with_pmid for paper_with_pmid in papers_with_pmid]
    pm_ids = []
    for paper in papers:
        pm_ids.append(paper['pubmed_id'])
    total_ids = len(pm_ids)
    batch_size = 600 if total_ids > 600 else total_ids
    total_chuncks = int(round(total_ids / batch_size, 0))
    start_chunk = 0
    end_chunk = batch_size
    num_processed_papers = 0
    for chunk in range(0, total_chuncks):
        try:
            logging.info(f"Getting information from the chunk {chunk + 1} of papers. {batch_size} papers in the chunk.")
            results = ec.fetch_in_bulk_from_list(pm_ids[start_chunk:end_chunk])
            # Process results
            for result in results:
                pm_id = result['MedlineCitation']['PMID']
                num_processed_papers += 1
                article_meta_data = result['MedlineCitation']['Article']
                if 'AuthorList' in article_meta_data.keys():
                    paper_db = db_papers.find_record({'pubmed_id': pm_id})
                    logging.info(f"[{num_processed_papers}/{total_ids}] Processing paper {article_meta_data['ArticleTitle']} (PMID: {pm_id})")
                    authors = article_meta_data['AuthorList']
                    paper_authors, gender_authors = [], []
                    for index, author in enumerate(authors):
                        author_id = paper_db['authors_id'][index]
                        author_db = db_authors.find_record({'id': author_id})
                        if author_db:
                            if 'first_name' not in author_db:
                                if 'ForeName' in author.keys():
                                    author_fullname = author['ForeName'] + ' ' + author['LastName']
                                    author_gender = get_gender(author_fullname)
                                    paper_authors.append(author_fullname)
                                    gender_authors.append(author_gender)
                                    logging.info(f"Updating author with id {author_id}")
                                    db_authors.update_record({'id': author_id},
                                                             {'first_name': author['ForeName'],
                                                              'last_name': author['LastName'],
                                                              'name': author_fullname,
                                                              'gender': author_gender})
                            else:
                                paper_authors.append(author_db['name'])
                                gender_authors.append(author_db['gender'])
                        else:
                            raise Exception(f"Author with id {author_id} does not exist!")
                    db_papers.update_record({'DOI': paper_db['DOI']},
                                            {'authors': paper_authors,
                                             'authors_gender': gender_authors})
            # Update indexes
            start_chunk = end_chunk
            end_chunk += batch_size
            time.sleep(1)
        except Exception as e:
            logging.error(e)


def get_pubmed_id_from_doi():
    ec = EntrezClient()
    db_papers = DBManager('bioinfo_papers')
    papers_without_pmid = db_papers.search({'authors': {'$exists': 0}})
    papers = [paper_without_pmid for paper_without_pmid in papers_without_pmid]
    paper_counter = 0
    papers_with_pmid = 0
    num_papers_without_pmid = 0
    BATCH_SIZE = 10
    doi_list = []
    updated_papers = []
    doi_requested = []
    logging.info(f"Getting the pubmed id of {len(papers)} papers")
    for paper in papers:
        paper_counter += 1
        doi_list.append('"' + paper['DOI'] + '"[doi]')
        doi_requested.append(paper['DOI'])
        if len(doi_list) < BATCH_SIZE:
            continue
        query_term = ' OR '.join(doi_list)
        try:
            logging.info(f"Searching the pubmed id of {len(doi_list)} papers")
            results = ec.search(query_term)
            if int(results['Count']) > 0:
                logging.info(f"Getting pubmed ids of {results['Count']} papers")
                papers_e = ec.fetch_in_batch_from_history(results['Count'], results['WebEnv'], results['QueryKey'],
                                                          batch_size=BATCH_SIZE)
                papers_with_pmid += len(papers_e)
                for paper_e in papers_e:
                    pubmed_id, paper_doi = None, None
                    # Getting the pubmed id of the paper
                    if paper_e['MedlineCitation'].get('PMID'):
                        pubmed_id = paper_e['MedlineCitation']['PMID']
                    # Getting the doi of the paper
                    if paper_e['PubmedData'].get('ArticleIdList'):
                        for other_id in paper_e['PubmedData']['ArticleIdList']:
                            if 'doi' in other_id.attributes.values():
                                paper_doi = other_id.title().lower()
                    if pubmed_id and paper_doi:
                        if paper_doi not in doi_requested:
                            logging.error(f"Obtained the doi {paper_doi}, which was not requested!")
                        else:
                            logging.info(f"Updating paper {paper_doi}")
                            if paper_doi not in updated_papers:
                                db_papers.update_record({'DOI': paper_doi}, {'pubmed_id': pubmed_id})
                                updated_papers.append(paper_doi)
                            else:
                                logging.error(f"Trying to update the paper {paper_doi} that was already updated before")
                    else:
                        logging.warning(f"Could not find the doi {paper_doi} or pubmed id {pubmed_id}")
            else:
                num_papers_without_pmid += len(doi_list)
            doi_list = []
            doi_requested = []
            time.sleep(2)
        except Exception as e:
            logging.error(e)
            raise Exception(e)
    logging.info(f"Final Report------------------------\n"
                 f"- Processed papers: {paper_counter}\n"
                 f"- Papers without pubmed id: {paper_counter-papers_with_pmid}\n"
                 f"- Papers with new pubmed id: {papers_with_pmid}")
