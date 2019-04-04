
import json
import re


# Get configuration from file
def get_config(config_file):
    with open(str(config_file), 'r') as f:
        config = json.loads(f.read())
    return config


def curate_author_name(author_raw):
    regex = re.compile('[0-9*]')
    return regex.sub('', author_raw).replace(' and ', ' ').strip().rstrip(',').lstrip(',')


def curate_affiliation_name(affiliation_raw):
    return affiliation_raw.replace(' and ', ' ').strip().rstrip(',').lstrip(',')


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