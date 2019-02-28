import bs4
import logging
import pathlib
import random
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


def gender_id(article, gendre_api):
    genders = []

    for person in article['authors']:
        first_name = person.split()[0]
        last_name = person.split()[-1]
        resp = gendre_api(first_name.encode('utf-8'), last_name.encode('utf-8')).GET()
        genders.append(resp.json().get('gender'))

    return genders


def extra_data_untrackable_journals(db):
    # Collect links and authors from oxford bioinformatics
    oxford_bioinformatics = db.search({'source': 'oxford bioinformatics', 'link': {'$exists': 0},
                                       'authors': {'$exists': 0}})
    list_DOI_oxford_bioinformatics = [article['DOI'] for article in oxford_bioinformatics]
    get_authors_links_untrackable_journals(list_DOI_oxford_bioinformatics, db)

    # Collect links and authors from nucleic acids research
    nucleic_bioinformatics = db.search({'source': 'nucleic acids research', 'link': {'$exists': 0},
                                        'authors': {'$exists': 0}})
    list_DOI_nucleic_acids_research = [article['DOI'] for article in nucleic_bioinformatics]
    get_authors_links_untrackable_journals(list_DOI_nucleic_acids_research, db)


def obtain_author_gender(db):
    gendre_api = GendreAPI("http://api.namsor.com/onomastics/api/json/gendre")

    articles = db.search({'authors': {'$and': [{'$exists': 1}, {'$ne': None}]}})
    for article in articles:
        genders = gender_id(article, gendre_api)
        db.update_record({'DOI': article['DOI']}, {'authors_gender': genders})

    # genders_oxford_bioinformatics = gender_id(authors_oxford_bioinformatics, gendre_api)
    # genders_nucleic_acids_research = gender_id(authors_nucleic_acids_research)

    #
    # with open(cwd + '/data/genders_oxford_bioinformatics.txt', 'w') as file_handler:
    #     for item in genders_oxford_bioinformatics:
    #         file_handler.write("{}\n".format(item))
    #
    #
    # with open(cwd + '/data/genders_nucleic_acids_research.txt', 'w') as file_handler:
    #     for item in genders_nucleic_acids_research:
    #         file_handler.write("{}\n".format(item))
    #
    # db_oxford_bioinformatics = foundations_data[foundations_data['source'] == 'oxford bioinformatics']
    # db_oxford_bioinformatics['authors_fullname'] = authors_oxford_bioinformatics
    # db_oxford_bioinformatics['genders'] = genders_oxford_bioinformatics
    # db_oxford_bioinformatics.to_csv(cwd + '/data/db_oxford.csv', index=False)
    #
    # db_nucleic_acids_research = foundations_data[foundations_data['source'] == 'nucleic acids research']
    # db_nucleic_acids_research['authors_fullname'] = authors_nucleic_acids_research
    # db_nucleic_acids_research['genders'] = genders_nucleic_acids_research
    # db_nucleic_acids_research.to_csv(cwd + '/data/db_nucleic.csv', index=False)
