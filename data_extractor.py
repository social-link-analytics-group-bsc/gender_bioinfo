import bs4
import gender_guesser.detector as gender
import logging
import pathlib
import random
import re
import time

from selenium import webdriver
from hammock import Hammock as GendreAPI


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


def is_robot_page(driver):
    try:
        robot_element = driver.find_element_by_xpath("//button[@id='btnSubmit']")
        return robot_element and robot_element.text.lower() == 'take me to my content'
    except:
        return False


def process_unavailable_page(count, start):
    now = time.time()
    time_from_beginning = now - start
    logging.info(f"{count}, {time_from_beginning}")
    logging.info('Going to sleep for 2 seconds')
    time.sleep(2)
    return None


def process_robot_page(doi_link, driver, count, start):
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
            return process_unavailable_page(count, start)
        else:
            now = time.time()
            time_from_beginning = now - start
            logging.info(f"{count}, {time_from_beginning}")
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
                process_unavailable_page(count, start)
                db.update_record({'DOI': doi_link}, {'link': None, 'authors': None})
            elif is_robot_page(driver):
                # we are detected as robots...
                logging.info('We were detected as robot :(')
                link_to_append = process_robot_page(doi_link, driver, count, start)
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


def get_gender(full_name):
    gendre_api = GendreAPI("http://api.namsor.com/onomastics/api/json/gendre")
    gendre_api2 = gender.Detector(case_sensitive=False)

    first_name = full_name.split()[0]
    last_name = full_name.split()[-1]
    resp = gendre_api(first_name, last_name).GET()
    try:
        author_gender = resp.json().get('gender')
        if author_gender == 'unknown':
            logging.info('Trying to get the author\'s gender using the second api')
            # if the main api returns unknown gender, try with another api
            author_gender = gendre_api2.get_gender(first_name)
            author_gender = 'unknown' if author_gender == 'andy' else author_gender
        return author_gender
    except:
        return 'error_api'


def gender_id(article):
    genders = []

    for person in article['authors']:
        author_gender = get_gender(person)
        genders.append(author_gender)

    return genders


def extra_data_untrackable_journals(db):
    # Collect links and authors from oxford bioinformatics
    oxford_bioinformatics = db.search({'source': 'oxford bioinformatics', 'link': {'$exists': 0},
                                       'authors': {'$exists': 0}})
    list_DOI_oxford_bioinformatics = [article['DOI'] for article in oxford_bioinformatics]
    if len(list_DOI_oxford_bioinformatics) > 0:
        get_authors_links_untrackable_journals(list_DOI_oxford_bioinformatics, db)

    # Collect links and authors from nucleic acids research
    nucleic_bioinformatics = db.search({'source': 'nucleic acids research', 'link': {'$exists': 0},
                                        'authors': {'$exists': 0}})
    list_DOI_nucleic_acids_research = [article['DOI'] for article in nucleic_bioinformatics]
    if len(list_DOI_nucleic_acids_research) > 0:
        get_authors_links_untrackable_journals(list_DOI_nucleic_acids_research, db)


def obtain_author_gender(db):
    articles = db.search({'authors': {'$exists': 1, '$ne': None}, 'authors_gender': {'$exists': 0}})
    for article in articles:
        logging.info(f"Finding out the gender of the authors {article['authors']} of the paper {article['DOI']}")
        genders = gender_id(article)
        logging.info(f"Genders identified: {genders}")
        db.update_record({'DOI': article['DOI']}, {'authors_gender': genders})


def curate_author_name(author_raw):
    regex = re.compile('[0-9*]')
    return regex.sub('', author_raw).replace(' and ', ' ').strip()


def curate_affiliation_name(affiliation_raw):
    return affiliation_raw.replace(' and ', ' ').strip().rstrip(',').lstrip(',')


def update_author_affiliation_and_country(db_authors, dict_authors, paper):
    new_vals = {}
    for author_name, val in dict_authors.items():
        author_db = db_authors.find_record({'name': author_name})
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
                db_authors.update_record({'name': author_name}, new_vals)
        else:
            record_to_save = {
                'name': author_name,
                'gender': get_gender(author_name),
                'papers': 1,
                'total_citations': int(paper['citations']),
                'papers_as_first_author': 0,
                'dois': [paper['DOI']],
                'papers_with_citations': 0,
                'citations': [int(paper['citations'])]
            }
            db_authors.store_record(record_to_save)


def obtain_author_info_academic(db_authors, html, countries, paper):
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
                                affiliation_country = get_country_from_string(countries, curated_affiliation)
                                if affiliation_country:
                                    dict_authors[author_name]['countries'].append(affiliation_country)
    update_author_affiliation_and_country(db_authors, dict_authors, paper)


def get_country_from_string(countries, str):
    for country in countries['names']:
        for word in str.split(' '):
            curated_word = word.replace(',', '').strip()
            if curated_word.lower() == country.lower():
                return country
    return None


def record_author_affiliation(affiliation, dict_authors, author_name):
    curated_affiliation = curate_affiliation_name(affiliation)
    if 'affiliations' not in dict_authors[author_name].keys():
        dict_authors[author_name]['affiliations'] = [curated_affiliation]
    else:
        dict_authors[author_name]['affiliations'].append(curated_affiliation)
    return curated_affiliation


def record_author_country(countries, curated_affiliation, dict_authors, author_name):
    affiliation_country = get_country_from_string(countries, curated_affiliation)
    if affiliation_country:
        if 'countries' not in dict_authors[author_name].keys():
            dict_authors[author_name]['countries'] = [affiliation_country]
        else:
            dict_authors[author_name]['countries'].append(affiliation_country)


def obtain_author_info_nucleid(db_authors, html, countries, paper):
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
                        curated_affiliation = record_author_affiliation(affiliation, dict_authors, author_name)
                        record_author_country(countries, curated_affiliation, dict_authors, author_name)
                else:
                    curated_affiliation = record_author_affiliation(affiliation, dict_authors, author_name)
                    record_author_country(countries, curated_affiliation, dict_authors, author_name)
    # Update authors' information
    update_author_affiliation_and_country(db_authors, dict_authors, paper)


def load_countries_file():
    # Read and store countries
    countries = {'names': [], 'prefixes': []}
    with open(str('data/country_list.txt'), 'r') as f:
        for _, line in enumerate(f):
            line = line.split(':')
            countries['names'].append(line[1].replace('\n', ''))
            countries['prefixes'].append(line[0].replace('\n', ''))
    countries['names'].extend(['UK', 'USA'])
    return countries


def do_obtain_affiliation(paper, driver, db_authors, countries):
    logging.info(f"Obtaining affiliation of the author of the paper with DOI: {paper['DOI']}")
    if 'link' in paper.keys():
        if 'academic.oup.com' in paper['link']:
            driver.get(paper['link'])
            obtain_author_info_academic(db_authors, driver.page_source, countries, paper)
        elif 'ncbi.nlm.nih.gov' in paper['link']:
            driver.get(paper['link'])
            obtain_author_info_nucleid(db_authors, driver.page_source, countries, paper)
        else:
            logging.warning(f"Unknown the domain name of the paper link {paper['link']}")
    else:
        logging.warning(f"Paper with doi {paper['DOI']} does not have a link")


def obtain_author_affiliation(db_papers, db_authors):
    driver = webdriver.Chrome()
    papers = db_papers.search({'link': {'$exists': 1}})

    countries = load_countries_file()

    for paper in papers:
        do_obtain_affiliation(paper, driver, db_authors, countries)


def obtain_affiliation_from_author(db_papers, db_authors):
    driver = webdriver.Chrome()
    authors = db_authors.search({'affiliations': {'$exists': 0}})

    # Read and store countries
    countries = load_countries_file()

    found_last = False
    for author in authors:
        for doi in author['dois']:
            #if doi == '10.1093/nar/gku322':
            #    found_last = True
            #if not found_last:
            #    continue
            paper = db_papers.find_record({'DOI': doi})
            if paper:
                do_obtain_affiliation(paper, driver, db_authors, countries)
            else:
                logging.info(f"Could not find a paper with the doi {doi}")
