from hammock import Hammock as GendreAPI

import csv
import gender_guesser.detector as gender
import logging
import json
import re
import pathlib
import unicodedata


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


# Get configuration from file
def get_config(config_file):
    with open(str(config_file), 'r') as f:
        config = json.loads(f.read())
    return config


def curate_author_name(author_raw):
    regex = re.compile('[0-9*]')
    author_clean = regex.sub('', author_raw).replace(' and ', ' ').replace('.', '').replace('-', ' ').rstrip(',')\
        .lstrip(',')
    author_clean = ' '.join(author_clean.split())  # remove duplicate whitespaces and newline characters
    return author_clean


def curate_affiliation_name(affiliation_raw):
    affiliation_raw = str(affiliation_raw)
    affiliation_clean = affiliation_raw.replace(' and ', ' ')
    affiliation_clean = affiliation_clean.strip()
    affiliation_clean = affiliation_clean.rstrip(',')
    affiliation_clean = affiliation_clean.lstrip(',')
    affiliation_clean = affiliation_clean.rstrip('\t')
    affiliation_clean = affiliation_clean.lstrip('\t')
    affiliation_clean = affiliation_clean.rstrip('.')
    affiliation_clean = ' '.join(affiliation_clean.split())  # remove duplicate whitespaces and newline characters
    return affiliation_clean


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


def get_base_url(full_url):
    base_url = ''
    slash_counter = 0
    for c in full_url:
        if c == '/':
            slash_counter += 1
        if slash_counter <= 2:
            base_url += c
        else:
            break
    return base_url


def title_except(str_to_title, exceptions=('a', 'an', 'of', 'the')):
    word_list = re.split(' ', str_to_title)
    final = [word_list[0].capitalize()]
    for word in word_list[1:]:
        final.append(word if word in exceptions else word.capitalize())
    return " ".join(final)


def get_db_name():
    current_dir = pathlib.Path(__file__).parents[0]
    config_fn = current_dir.joinpath('config.json')
    config = get_config(config_fn)
    return config['mongo']['db_name']


def normalize_text(text):
    return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode()


def obtain_paper_abstract_and_pubmedid(file_name, paper_eid):
    dir_full = pathlib.Path('data', 'processed')
    journal_file_name = dir_full.joinpath(file_name)
    with open(str(journal_file_name), 'r') as f:
        file = csv.DictReader(f, delimiter=',')
        for line in file:
            if line['EID'] == paper_eid:
                if '.0' in line['PubMed ID']:
                    pubmed_id = line['PubMed ID'].split('.')[0]
                else:
                    pubmed_id = line['PubMed ID']
                return line['Abstract'], pubmed_id, line
    return None, None, None