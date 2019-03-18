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


def gender_id(article, gendre_api, gendre_api2):
    genders = []

    for person in article['authors']:
        first_name = person.split()[0]
        last_name = person.split()[-1]
        resp = gendre_api(first_name, last_name).GET()
        try:
            author_gender = resp.json().get('gender')
            if author_gender == 'unknown':
                logging.info('Trying to get the author\'s gender using the second api')
                # if the main api returns unknown gender, try with another api
                author_gender = gendre_api2.get_gender(first_name)
                author_gender = 'unknown' if author_gender == 'andy' else author_gender
            genders.append(author_gender)
        except:
            genders.append('error_api')

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
    gendre_api = GendreAPI("http://api.namsor.com/onomastics/api/json/gendre")
    gendre_api2 = gender.Detector(case_sensitive=False)

    articles = db.search({'authors': {'$exists': 1, '$ne': None}, 'authors_gender': {'$exists': 0}})
    for article in articles:
        logging.info(f"Finding out the gender of the authors {article['authors']} of the paper {article['DOI']}")
        genders = gender_id(article, gendre_api, gendre_api2)
        logging.info(f"Genders identified: {genders}")
        db.update_record({'DOI': article['DOI']}, {'authors_gender': genders})


def update_author_affiliations(author_record, affiliation_to_save):
    affiliations = author_record['affiliations']
    for affiliation in affiliations:
        if affiliation.lower() not in affiliation_to_save.lower():
            affiliations.append(affiliation_to_save)
            break
    return affiliations


def update_author_countries(author_record, country_to_save):
    countries = author_record['countries']
    for country in countries:
        if country.lower() not in country_to_save.lower():
            countries.append(country_to_save)
            break
    return countries


def obtain_author_info_academic(db_authors, html):
    regex = re.compile('[0-9]')
    soup = bs4.BeautifulSoup(html, 'html.parser')
    elements = soup.find_all(id='authorInfo_OUP_ArticleTop_Info_Widget')

    for element in elements:
        author_details = element.text
        author_lines = [line for line in author_details.splitlines() if line]
        author_name = author_lines[0].strip()
        author_affiliation = author_lines[1].strip()
        author_research_center = ' '.join(regex.sub('', author_affiliation).split())
        author_country = author_affiliation.split(',')[-1].strip()
        author_db = db_authors.find_record({'name': author_name})
        if author_db:
            new_vals = dict()
            if 'affiliations' in author_db.keys():
                new_vals['affiliations'] = update_author_affiliations(author_db, author_research_center)
            else:
                new_vals['affiliations'] = [author_research_center]
            if 'countries' in author_db.keys():
                new_vals['countries'] = update_author_countries(author_db, author_country)
            else:
                new_vals['countries'] = [author_country]
            db_authors.update_record({'name': author_name}, new_vals)
        else:
            logging.error(f"The author {author_name} doesn't exist in the database!")


def get_country_from_string(countries, str):
    for country in countries['names']:
        for word in str.split(' '):
            curated_word = word.replace(',', '').strip()
            if curated_word.lower() == country.lower():
                return country
    return None


def obtain_author_info_nucleid(db_authors, html, countries):
    soup = bs4.BeautifulSoup(html, 'html.parser')
    # Get authors' names and superscripts
    authors = soup.find('div', class_='contrib-group fm-author').text.split(',')
    c_author = authors[0]
    dict_authors = dict()
    author_indexes = []
    for author in authors[1: len(authors)]:
        for author_txt in author.split(' '):
            if author_txt.isdigit():
                author_indexes.append(author_txt)
            else:
                dict_authors[c_author] = {'indexes': author_indexes.copy()}
                c_author = author_txt
                author_indexes.clear()
    dict_authors[c_author] = {'indexes': author_indexes.copy()}
    # Get authors' affiliations
    author_affiliations = list(soup.find('div', class_='fm-affl').children)
    index = '0'
    for affiliation in author_affiliations:
        if 'sup' == affiliation.name:
            index = affiliation.text
            continue
        else:
            for author_name, val in dict_authors.items():
                if index in val['indexes']:
                    curated_affiliation = affiliation.strip()
                    if 'affiliations' not in dict_authors[author_name].keys():
                        dict_authors[author_name]['affiliations'] = [curated_affiliation]
                    else:
                        dict_authors[author_name]['affiliations'].append(curated_affiliation)
                    affiliation_country = get_country_from_string(countries, curated_affiliation)
                    if affiliation_country:
                        if 'countries' not in dict_authors[author_name].keys():
                            dict_authors[author_name]['countries'] = [affiliation_country]
                        else:
                            dict_authors[author_name]['countries'].append(affiliation_country)
    # Update authors' information
    new_vals = {}
    for author_name, val in dict_authors.items():
        author_db = db_authors.find_record({'name': author_name})
        if 'countries' not in author_db.keys():
            new_vals['countries'] = val['countries']
            new_vals['affiliations'] = val['affiliations']
        else:
            new_vals['countries'] = list(set(author_db['countries'].extend(val['countries'])))
            new_vals['affiliations'] = list(set(author_db['affiliations'].extend(val['affiliations'])))
        db_authors.update_record({'name': author_name}, new_vals)


def obtain_author_affiliation(db_papers, db_authors):
    driver = webdriver.Chrome()
    papers = db_papers.search({'link': {'$exists': 1}})

    # Read and store countries
    countries = {'names': [], 'prefixes': []}
    with open(str('data/country_list.txt'), 'r') as f:
        for _, line in enumerate(f):
            line = line.split(':')
            countries['names'].append(line[1].replace('\n', ''))
            countries['prefixes'].append(line[0].replace('\n', ''))
    countries['names'].append('UK')

    for paper in papers:
        logging.info(f"Obtaining affiliation of the author of the paper with DOI: {paper['DOI']}")
        if 'academic.oup.com' in paper['link']:
            # driver.get(paper['link'])
            # obtain_author_info_academic(db_authors, driver.page_source)
            pass
        else:
            driver.get(paper['link'])
            obtain_author_info_nucleid(db_authors, driver.page_source, countries)
