# Large-scale study on the bioinformatics research field

## Data Collection

1. Extract articles published between 2000 and 2015 from the journals: Oxford Bioinformatics, 
Plos Computational Biology, Nucleic Acids Research, BMC bioinformatics, and BMC Genomics;

2. Iterate over the articles' dois and get the url of the publications by using the Doi Resolution Service 
(http://dx.doi.org/);  

3. Iterate over the list of articles' urls and extract the authors and their affiliations from each article;

4. For each author, get their gender by using the NamSor API (http://api.namsor.com/)

All of these data are stored in a MongoDB database

## Data Cleaning

1. Curate the name of authors by removing non alpha characters, such as numbers, or starts, underscores, and commas.
 
