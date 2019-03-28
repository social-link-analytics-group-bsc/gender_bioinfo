
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
