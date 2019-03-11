import ast
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
