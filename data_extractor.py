import bs4
import logging
import pathlib
import random
import re
import time

from db_manager import DBManager
from selenium import webdriver
from utils import curate_author_name, curate_affiliation_name, load_countries_file, get_gender, title_except


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

    def __get_country_from_affiliation(self, affiliation):
        found_countries = []
        for country in self.countries['names']:
            if country in affiliation:
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
        for author_name, val in dict_authors.items():
            author_db = self.db_authors.find_record({'name': author_name})
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
                    'gender': get_gender(author_name),
                    'papers': 1,
                    'total_citations': int(paper['citations']),
                    'papers_as_first_author': 0,
                    'dois': [paper['DOI']],
                    'papers_with_citations': 0,
                    'citations': [int(paper['citations'])],
                    'affiliations': val['affiliations'],
                    'countries': val['countries']
                }
                self.db_authors.store_record(record_to_save)

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
                            for descendant in child.children:
                                if descendant.name != 'sup' and descendant != '\n':
                                    curated_affiliation = curate_affiliation_name(descendant)
                                    dict_authors[author_name]['affiliations'].append(curated_affiliation)
                                    affiliation_country = self.__get_country_from_affiliation(curated_affiliation)
                                    if affiliation_country:
                                        dict_authors[author_name]['countries'].append(affiliation_country)
        self.__update_author_affiliation_and_country(dict_authors, paper)

    def __obtain_author_info_nucleid(self, paper):
        html = self.driver.page_source
        soup = bs4.BeautifulSoup(html, 'html.parser')
        # Get authors' names and superscripts
        authors = soup.find('div', class_='contrib-group fm-author').text.split(',')
        c_author = authors[0].strip()
        dict_authors = dict()
        author_indexes = []
        for author in authors[1: len(authors)]:
            if author.isdigit():
                author_indexes.append(author)
                continue
            if author[0].isdigit():
                author_indexes.append(author[0])
            dict_authors[c_author] = {'indexes': author_indexes.copy()}
            c_author = curate_author_name(author)
            author_indexes.clear()
        if c_author and c_author not in dict_authors.keys():
            dict_authors[curate_author_name(c_author)] = {'indexes': author_indexes.copy()}
        # Get authors' affiliations
        author_affiliations = list(soup.find('div', class_='fm-affl').children)
        index = '0'
        for affiliation in author_affiliations:
            if 'sup' == affiliation.name:
                index = affiliation.text
                continue
            else:
                for author_name, val in dict_authors.items():
                    if val['indexes']:
                        if index in val['indexes']:
                            curated_affiliation = self.__record_author_affiliation(affiliation, dict_authors,
                                                                                   author_name)
                            self.__record_affiliation_country(curated_affiliation, dict_authors, author_name)
                    else:
                        curated_affiliation = self.__record_author_affiliation(affiliation, dict_authors, author_name)
                        self.__record_affiliation_country(curated_affiliation, dict_authors, author_name)
        # Update authors' information
        self.__update_author_affiliation_and_country(dict_authors, paper)

    def __obtain_author_info_bmc(self, paper):
        elements = self.driver.find_elements_by_class_name('AuthorName')
        dict_authors = dict()
        for element in elements:
            author_name = element.text
            dict_authors[author_name] = {'affiliations': [], 'countries': []}
            element.click()
            affs = self.driver.find_elements_by_class_name('tooltip-tether__indexed-item')
            for aff in affs:
                author_affiliation = curate_affiliation_name(aff.text)
                dict_authors[author_name]['affiliations'].append(author_affiliation)
                affiliation_country = self.__get_country_from_affiliation(author_affiliation)
                dict_authors[author_name]['countries'].append(affiliation_country)
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

    def __parse_affiliation(self, affiliation, author_countries, enriched_country, country, countries):
        # It might happen that the occurrence refers to a city that has the same
        # name of a country (e.g., Georgia), so I checked if the subsequent
        # term in the affiliation is a country. If it is a country
        # then I assume the occurrence is a city otherwise is a country
        if enriched_country in affiliation:
            subsequent_str = self.__get_subsequent_str(affiliation, enriched_country, ',')
            if not subsequent_str:
                subsequent_str = self.__get_subsequent_str(affiliation, enriched_country, '\n')
            subsequent_str = subsequent_str.lstrip(',').rstrip(',').rstrip('\n').strip()
            if title_except(subsequent_str) not in countries['names']:
                num_occurances = affiliation.count(enriched_country)
                for i in range(0, num_occurances):
                    author_countries.append(title_except(country))
                affiliation = affiliation.replace(enriched_country, '___')
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
                # To avoid mismatching the country names should be preceded by a comma and
                # a white space and should end with a comma (match case 1) or
                # newline character (match case 2).
                #
                # Match Case 1
                enriched_country = ', ' + country + ','
                raw_affiliation = self.__parse_affiliation(raw_affiliation, author_countries,
                                                           enriched_country, country, countries)
                # Match Case 2
                enriched_country = ', ' + country + '\n'
                raw_affiliation = self.__parse_affiliation(raw_affiliation, author_countries,
                                                           enriched_country, country, countries)
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
                self.__obtain_author_info_nucleid(paper)
            elif 'bmcgenomics.biomedcentral.com' in paper['base_url'] or \
                 'bmcbioinformatics.biomedcentral.com' in paper['base_url']:
                self.__obtain_author_info_bmc(paper)
            elif 'journals.plos.org' in paper['base_url']:
                self.__obtain_author_info_plos(paper)
            else:
                logging.warning(f"Unknown the domain name of the paper link {paper['link']}")
        else:
            logging.error(f"Paper with doi {paper['DOI']} does not have a link")

    def obtain_author_affiliation_from_paper(self):
        papers = self.db_papers.search({'link': {'$exists': 1}})

        for paper in papers:
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

    def obtain_affiliation_from_papers_in_file(self):
        with open(str('data/papers_without_links.txt'), 'r') as f:
            processed_links = 0
            for _, link in enumerate(f):
                processed_links += 1
                link = link.replace('\n', '')
                if link == 'https://dx.doi.org/':
                    continue
                paper = self.db_papers.find_record({'link': link})
                if paper:
                    logging.info(f"Processed Links: {processed_links}")
                    self.__do_obtain_affiliation(paper)
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
