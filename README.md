# Gender Bias in Bioinformatics

This repo contains the scripts and data used to examine gender bias in Bioinformatics. By taking five representative
journals in the field—[Oxford Bioinformatics](https://academic.oup.com/bioinformatics), 
[Plos Computational Biology](https://journals.plos.org/ploscompbiol/), [Nucleic Acids Research](https://academic.oup.com/nar), 
[BMC Bioinformatics](https://bmcbioinformatics.biomedcentral.com/), and 
[BMC Genomics](https://bmcgenomics.biomedcentral.com/)—,we conduct a large-scale analysis of the role of female 
researchers in Bioinformatics from 2005 to 2017. We chose to start in 2005 because this was the starting year of Plos 
Computational Biology.

## Data Collection

Data were obtained from [Scopus](https://www.scopus.com), one of today's most complete repository of scientific manuscripts. The data was collected on August 22nd and 23rd, 2019, so the obtained articles correspond to the 
information available on Scopus at that moment. Below, the steps were performed to get the data. 

- Query the search engine of Scopus using the next string through *Advanced Search* to extract articles published 
between 2005 and 2017 from above-mentioned journals.

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
situations, the range of years (2005-2017) was split into several searches to comply with this restriction. 

The raw data downloaded can be found in CSV files located in `data/raw/full`. The `data/raw/summary` directory contains 
files with only citation information about the articles. In total, information of 47,427 papers and their corresponding
authors were collected through the described method. The table above shows the number of papers per journal extracted 
from Scopus.

## Getting Started

1. Download and install Python >= 3.4.4;
2. Download and install MongoDB community version.  Instructions on how to install it can be
found [here](https://docs.mongodb.com/manual/installation/);
3. Set up a Mongodb database and create two collections, namely `bioinfo_papers` and `bioinfo_authors`;
4. Clone the repository `git clone https://github.com/ParticipaPY/politic-bots.git`;
5. Get into the directory of the repository `cd politic-bots`;
6. Create a virtual environment by running `virtualenv env`;
7. Activate the virtual environment by executing `source env/bin/activate`;
8. Inside the directory of the repository install the project dependencies by running `pip install -r requirements.txt`;
9. Set the database information inside the dictionary `mongo` in `config.json`;
10. Get a key to operate the API of PubMed by following the instructions [here](https://www.ncbi.nlm.nih.gov/books/NBK25497/#chapter2.Usage_Guidelines_and_Requiremen)
11. Set the obtained API key and email address inside the dictionary `pubmed` in `config.json`;
12. Run `run.py` to go through all of the data pre-processing, loading, and exporting tasks. Three CSV files result from
the execution of `run.py`, they are stored under the `data` directory.

The following sections explain in details each of the pre-processing, loading, and exporting tasks.

## Data Pre-Processing

From `run.py` execute the function `combine_csv_files` in `data_wrangler.py` to combine files in `data/raw/full` 
into one CSV file per journal. The resulting files are stored in `data/processed` (you might need to create the
folder *`processed`* inside *`data`* before running the function)

## Data Loading

From `run.py` execute the function `load_data_from_files_into_db` in `data_loader.py` to load the data in `data/raw/summary`
and `data/processed` into the database. Information about papers is stored in `bioinfo_papers` while information
on papers' authors is recorded in `bioinfo_authors`. This function takes a while to complete in part because it 
connects to [DOI resolution](https://dx.doi.org/) to extract links of the papers—Scopus does not provide links to the papers. 
The completion time can be sped up by commenting the line #162 in `data_loader.py`.

Duplicated records (194) and entries without DOI (401) are not stored. **In total, 46,832 records are stored in the 
database**. The distribution of  duplicated articles and articles without DOI per journal is shown in the next table.

| Journal                    | Duplicates|Missing DOIs|
|----------------------------|-----------|------------|
| Oxford Bioinformatics      | 79        |  1         |
| Plos Computational Biology | 0         |  11        |
| Nucleic Acids Research     | 12        |  169       |
| BMC Bioinformatics         | 76        |  100       |
| BMC Genomics               | 27        |  120       |
| **Total**                  | 194       |  401       |

## Data Processing

Scopus does not provide the full name of authors—only the initial of the first (and middle) name and the last name.
However, the PubMed identifier of the articles is provided by Scopus. We use the PubMed Id of the articles to hit the 
[API of PubMed](https://www.ncbi.nlm.nih.gov/home/develop/api/) and get information about the papers' authors, including 
their full names. To complete the name of authors, execute from `run.py` the function `get_paper_author_names_from_pubmed` 
in `data_extractor.py`. The function takes a while to complete.

**Gender Identification**. The API [NamSor](http://api.namsor.com/) is hit to infer the authors' gender from the author's 
name. In case, NamSor fails to identify the gender, the python package [gender-guesser](https://pypi.org/project/gender-guesser/)
is used to find out the gender of authors. Information on how [NamSor](https://www.namsor.com/) works can be at its 
website.

Through this process, we find that 266 articles (0.6%) are not in PubMed, so the information about their authors cannot 
be obtained from this source. For different reasons, we cannot get information about 12 articles that have the PubMed 
identifier. Ten of them are proceedings of conferences, 1 is a PDF with the names of the editorial board of the journal, and 1 does 
not have author list.

We cannot get information on 2,626 authors (0.18%). In some cases, Scopus does not provide the last name specifying the 
situation with an empty string or with the text *[No author name available]*. In some other cases, there are 
inconsistencies between the list of authors provided by Scopus and the list of authors obtained from PubMed. This is the
case of articles in which organizations appear as part of the author list. Here, PubMed mentions the organization's name 
while Scopus present the name of the organization's members that authored the article. We also found PubMed 
registries with the first and last name inverted. These registries cannot be automatically matched.

In total, the gender of 27,706 authors (19%) cannot be detected by not either of the two gender identification services. 
In 10% of the cases, the gender cannot be identified because we cannot get the author name from PubMed. For the 
rest, we found that the identification services have problems with Asian names.

## Data Export

Before running the analyses, data are exported to tabular format and saved into CSV files. Data about papers can be
exported to a CSV by running the function `export_db_into_file` included in `data_exporter.py`. The function 
receives as parameters, the name of the CSV file, the database from where to extract the data, and the fields to be 
extracted. The following code snippet shows an example of how to export data about papers into the CSV file `paper.csv`,
which is saved in the directory `data`.

```python
from data_exporter import export_db_into_file
from db_manager import DBManager
from utils import get_db_name
    
db_papers = DBManager('bioinfo_papers', db_name=get_db_name())
fields_to_export = ['title', 'DOI', 'year', 'source', 'citations', 
                    'edamCategory', 'link', 'authors', 
                    'gender_last_author']
export_db_into_file('papers.csv', db_papers, fields_to_export)
```  

In the same manner, information about authors can be exported to a CSV file. The function `export_author_papers` in 
`data_exported.py` creates a CSV that registers the cartesian product between papers and authors. The resulting file
is saved into the directory `data`.

## Gender Bias Analysis

The CSV files resulting from the exporting task (i.e., `data/papers.csv`, `data/authors.csv`, and 
`data/papers_authors.csv`), are used to conduct the gender bias analyses. The analysis scripts are contained in the 
notebook `analysis/gender_bias_analysis.ipynb`. 
 
## Technologies

1. [Python 3.4](https://www.python.org/downloads/)
2. [MongoDB Community Edition](https://www.mongodb.com/download-center#community)—used as data storage repository
3. [Selenium WebDriver](https://www.seleniumhq.org/projects/webdriver/)—used to resolve papers' DOIs
4. [Biopython](https://biopython.org/)—PubMed API client
5. [Jupyter Notebook](https://jupyter.org/)—data exploration and analysis

## Issues

Please use [Github's issue tracker](https://github.com/ParticipaPY/politic-bots/issues/new) to report issues and 
suggestions.

## Contributors

[Jorge Saldivar](https://github.com/joausaga), [Fabio Curi](https://github.com/fabiocuri), Nataly Buslón, María José Rementería, 
and Alfonso Valencia