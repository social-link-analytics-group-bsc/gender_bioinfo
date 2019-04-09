from hammock import Hammock as GendreAPI

import gender_guesser.detector as gender
import logging
import json
import re
import pathlib


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


# Get configuration from file
def get_config(config_file):
    with open(str(config_file), 'r') as f:
        config = json.loads(f.read())
    return config


def curate_author_name(author_raw):
    regex = re.compile('[0-9*]')
    return regex.sub('', author_raw).replace(' and ', ' ').rstrip(',').lstrip(',').strip()


def curate_affiliation_name(affiliation_raw):
    return affiliation_raw.replace(' and ', ' ').rstrip(',').lstrip(',').rstrip('\t').lstrip('\t').strip()


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
