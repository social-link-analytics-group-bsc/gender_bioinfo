# Large-Scale Study on Gender Bias in Bioinformatics

This repo contains the scripts and data used to examine gender bias in Bioinformatics. By taking five representative
journals in the field—[Oxford Bioinformatics](https://academic.oup.com/bioinformatics), 
[Plos Computational Biology](https://journals.plos.org/ploscompbiol/), [Nucleic Acids Research](https://academic.oup.com/nar), 
[BMC Bioinformatics](https://bmcbioinformatics.biomedcentral.com/), and 
[BMC Genomics](https://bmcgenomics.biomedcentral.com/)—,we conduct a large-scale analysis of the role of female 
researchers in Bionformatics from 2005 to 2017. We chose to start in 2005 because this was the starting year of Plos 
Computational Biology.

## Data Collection

Data were obtained from [Scopus](https://www.scopus.com), one of today's most complete repository of scientific 
manuscripts. The data was collected on August 22nd and 23rd, 2019, so the obtained articles correspond to the 
information available on Scopus at that moment. Below the steps were performed to get the data. 

- Query the search engine of Scopus using the next string through *Advaced Search* to extract articles published 
between 2005 and 2017 from above mentioned journals.

`ISSN ( 'JOURNAL_ISSN' ) AND ( LIMIT-TO ( DOCTYPE,"ar" ) OR LIMIT-TO ( DOCTYPE,"cp" ) ) AND ( LIMIT-TO ( PUBYEAR , 2017 )
OR LIMIT-TO ( PUBYEAR , 2016 ) OR LIMIT-TO ( PUBYEAR , 2015 ) OR LIMIT-TO ( PUBYEAR , 2014 ) OR LIMIT-TO ( PUBYEAR , 2013 )
OR LIMIT-TO ( PUBYEAR , 2012 ) OR LIMIT-TO ( PUBYEAR , 2011 ) OR LIMIT-TO ( PUBYEAR , 2010 ) OR LIMIT-TO ( PUBYEAR , 2009 ) 
OR LIMIT-TO ( PUBYEAR , 2008 ) OR LIMIT-TO ( PUBYEAR , 2007 ) OR LIMIT-TO ( PUBYEAR , 2006 ) OR LIMIT-TO ( PUBYEAR , 2005 ) )`

| Journal                    | ISSN      |
|----------------------------|-----------|
| Oxford Bioinformatics      | 1460-2059 |
| Plos Computational Biology | 1553-734X |
| Nucleic Acid Research      | 1362-4962 |
| BMC Bioinformatics         | 1471-2105 |
| BMC Genomics               | 1471-2164 |

- Use the function *Export* to download the data about the articles. CSV was chosen as the *export method* and all of 
the information available per article (citation, bibliographical, abstract, funding, etc.) was asked to export. Here it 
is important to mention that Scopus limits to 2,000 the number of records that can be exported at a time, so in some 
situations the range of years (2005-2017) was split in several searches to comply with this restriction. 

The raw data downloaded can be found in CSV files located in `data/raw/full`. The `data/raw/summary` directory contains 
files with only citation information about the articles. 

## Data Pre-Processing

From `run.py` run the function `combine_csv_files` in `data_wrangler.py` to combine the files in `data/raw/full` 
into one CSV file per journal. The resulting files will be store in `data/processed` (you might need to create the
folder *`processed`* inside *`data`* before running the function)

## Data Loading

1. Before loading the data you will need to install **MongoDB Community Edition**. Instructions on how to install it can be
found [here](https://docs.mongodb.com/manual/installation/) 

2. Set up a Mongodb database and create two collections, namely `bioinfo_papers` and `bioinfo_authors`

3. Set in `src/config.json` the information of the MongoDB database that will be used to store the data

4. From `run.py` run the function `load_data_from_files_into_db` in `data_loader.py` to load the data in `data/raw/summary`
and `data/processed` into the database. Information about papers will be stored in `bioinfo_papers` while information
on papers' authors will be recorded in `bioinfo_authors`. This process takes a while in part because it takes the DOI of
the papers and extracts from `https://dx.doi.org/` their links. The links to the papers is information not 
provided by Scopus

## Data Processing

1.  

## Gender Identification

1. For each author, get their gender by using the NamSor API (http://api.namsor.com/).

## Data Cleaning

1. Curate the name of authors by removing non alpha characters, such as numbers, or starts, underscores, and commas.

2. Remove duplicate authors from the database.
 
## Technologies

1. [Python 3.4](https://www.python.org/downloads/)
2. [MongoDB Community Edition](https://www.mongodb.com/download-center#community)
3. [Selenium WebDriver](https://www.seleniumhq.org/projects/webdriver/)