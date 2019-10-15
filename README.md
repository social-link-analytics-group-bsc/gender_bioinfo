# Gender Bias in Bioinformatics

This repo contains the scripts and data used to examine gender bias in Bioinformatics. By taking five representative
journals in the field—[Oxford Bioinformatics](https://academic.oup.com/bioinformatics), 
[Plos Computational Biology](https://journals.plos.org/ploscompbiol/), [Nucleic Acids Research](https://academic.oup.com/nar), 
[BMC Bioinformatics](https://bmcbioinformatics.biomedcentral.com/), and 
[BMC Genomics](https://bmcgenomics.biomedcentral.com/)—,we conduct a large-scale analysis of the role of female 
researchers in Bionformatics from 2005 to 2017. We chose to start in 2005 because this was the starting year of Plos 
Computational Biology.

## Data Collection

Data were obtained from [Scopus](https://www.scopus.com), one of today's most complete repository of scientific manuscripts. The data was collected on August 22nd and 23rd, 2019, so the obtained articles correspond to the 
information available on Scopus at that moment. Below the steps were performed to get the data. 

- Query the search engine of Scopus using the next string through *Advaced Search* to extract articles published 
between 2005 and 2017 from above mentioned journals.

`ISSN ( 'JOURNAL_ISSN' ) AND ( LIMIT-TO ( DOCTYPE,"ar" ) OR LIMIT-TO ( DOCTYPE,"cp" ) ) AND ( LIMIT-TO ( PUBYEAR , 2017 )
OR LIMIT-TO ( PUBYEAR , 2016 ) OR LIMIT-TO ( PUBYEAR , 2015 ) OR LIMIT-TO ( PUBYEAR , 2014 ) OR LIMIT-TO ( PUBYEAR , 2013 )
OR LIMIT-TO ( PUBYEAR , 2012 ) OR LIMIT-TO ( PUBYEAR , 2011 ) OR LIMIT-TO ( PUBYEAR , 2010 ) OR LIMIT-TO ( PUBYEAR , 2009 ) 
OR LIMIT-TO ( PUBYEAR , 2008 ) OR LIMIT-TO ( PUBYEAR , 2007 ) OR LIMIT-TO ( PUBYEAR , 2006 ) OR LIMIT-TO ( PUBYEAR , 2005 ) )`

| Journal                    | ISSN      |  Papers Extracted   |
|----------------------------|-----------|------------|
| Oxford Bioinformatics      | 1460-2059 |  8,546     |
| Plos Computational Biology | 1553-734X |  5,132     |
| Nucleic Acids Research     | 1362-4962 |  15,670    |
| BMC Bioinformatics         | 1471-2105 |  7,879     |
| BMC Genomics               | 1471-2164 |  10,200    |
| **Total**                  |           |  47,427    |

- Use the function *Export* to download the data about the articles. CSV was chosen as the *export method* and all of 
the information available per article (citation, bibliographical, abstract, funding, etc.) was asked to export. Here it 
is important to mention that Scopus limits to 2,000 the number of records that can be exported at a time, so in some 
situations, the range of years (2005-2017) was split in several searches to comply with this restriction. 

The raw data downloaded can be found in CSV files located in `data/raw/full`. The `data/raw/summary` directory contains 
files with only citation information about the articles. In total information of 47,427 papers and their corresponding
authors were collected through the described method. The table above shows the number of papers per journal extracted 
from Scopus.

## Getting Started

1. Download and install Python >= 3.4.4;
2. Download and install MongoDB community version;
3. Clone the repository `git clone https://github.com/ParticipaPY/politic-bots.git`;
4. Get into the directory of the repository `cd politic-bots`;
5. Create a virtual environment by running `virtualenv env`;
6. Activate the virtual environment by executing `source env/bin/activate`;
7. Inside the directory of the repository install the project dependencies by running `pip install -r requirements.txt`;
8. Set the database information inside the dictionary `mongo` in `config.json`;
9. Get a key to operate the API of PubMed by following the instructions [here](https://www.ncbi.nlm.nih.gov/books/NBK25497/#chapter2.Usage_Guidelines_and_Requiremen)
10. Set the obtained API key and email address inside the dictionary `pubmed` in `config.json`

## Data Pre-Processing

From `run.py` execute the function `combine_csv_files` in `data_wrangler.py` to combine the files in `data/raw/full` 
into one CSV file per journal. The resulting files will be stored in `data/processed` (you might need to create the
folder *`processed`* inside *`data`* before running the function)

## Data Loading

1. Before loading the data you will need to install **MongoDB Community Edition**. Instructions on how to install it can be
found [here](https://docs.mongodb.com/manual/installation/) 

2. Set up a Mongodb database and create two collections, namely `bioinfo_papers` and `bioinfo_authors`

3. Set in `src/config.json` the information of the MongoDB database that will be used to store the data

4. From `run.py` execute the function `load_data_from_files_into_db` in `data_loader.py` to load the data in `data/raw/summary`
and `data/processed` into the database. Information about papers will be stored in `bioinfo_papers` while information
on papers' authors will be recorded in `bioinfo_authors`. This function takes a while to complete in part because it 
connects to [DOI resolution](https://dx.doi.org/) to extract link of the papers—links to the papers are not provided 
by Scopus. The completion time can be sped up by commenting the line #162 in `data_loader.py`.

Duplicated records (197) and entries without DOI (401) are not included in the loading process. **In total, 46,829 records
are stored in the database**.

## Data Processing

Scopus does not provide the full name of authors—only the initial of the first (and middle) name and the last name.
However, the PubMed identifier of the articles is provided by Scopus. We use the PubMed Id of the articles to hit the 
[API of PubMed](https://www.ncbi.nlm.nih.gov/home/develop/api/) and get information about papers' authors, including 
their full names. To complete the name of authors, execute from `run.py` the function `get_paper_author_names_from_pubmed` 
in `data_extractor.py`. The function takes a while to complete.

**Gender Identification**. Taking the full name of authors, the API [NamSor](http://api.namsor.com/) is used to infer the
authors' gender. In case, NamSor fails to identify the gender, the python package [gender-guesser](https://pypi.org/project/gender-guesser/)
is used to find out the gender of authors. Information on how [NamSor](https://www.namsor.com/) works can be at its 
website.

Through this process we found that 266 articles (0.6%) are not in PubMed, so the information about their authors could 
not be obtained from this source. For different reasons, we could not get information of 12 articles that have PubMed 
identifier. Ten of them are proceedings of conferences, 1 is a PDF with the names of the editorial board of the journal,
and 1 does not have author list.

We could not get information of 2626 authors (0.18%). In some case, Scopus does not provide the last name specifying the 
situation with an empty string or with the text *[No author name available]*. In some other cases, there were 
inconsistencies between the list of authors provided by Scopus and the list of authors obtained from PubMed. This is the
case of articles in which organizations appear as part of the author list. Here, PubMed mentions the organization name 
while Scopus present the name of the organization's members that participated in the article. We also found PubMed 
registries with the first and last name inverted. These registries could not be automatically matched.

In total, the gender of 27708 authors (19%) could be detected by not either of the two gender identification services. 
In 10% of the cases, the gender could not be identified because we could not get the author names from PubMed. For the 
rest, we saw that the identification services has problems with asian names.

## Gender Bias Analysis

The scripts used to conduct all of the gender bias analyses are contained in the Jupyter notebook `analysis/gender_bias_analysis.ipynb` 
 
## Technologies

1. [Python 3.4](https://www.python.org/downloads/)
2. [MongoDB Community Edition](https://www.mongodb.com/download-center#community)—used as data storage repository
3. [Selenium WebDriver](https://www.seleniumhq.org/projects/webdriver/)—used to resolve papers' DOIs
4. [Biopython](https://biopython.org/)—PubMed API client
5. [Jupyter Notebook](https://jupyter.org/)—data exploration and analysis

## Issues

Please use [Github's issue tracker](https://github.com/ParticipaPY/politic-bots/issues/new) to report issues and suggestions.

## Contributors

[Jorge Saldivar](https://github.com/joausaga), [Fabio Curi](https://github.com/fabiocuri), Nataly Buslón, María José Rementería, 
and Alfonso Valencia