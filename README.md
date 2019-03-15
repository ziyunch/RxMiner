# RxMiner
RxMiner is a scalable data pipeline for prescription drug analytics that standardizes Medicaid and Medicare prescriptions using NIH drug, FDA drug, and NEPPS provider information.

# Overview
With a switch to electronic prescriptions, healthcare and insurance companies are providing large drug usage datasets. However, since each provider has their own way to log prescriptions the data sets are non-standard. Using publicly available drug datasets, RxMiner standardizes and joins prescription datasets.

RxMiner features:
- 100 million standardized prescriptions queryable in Redshift
- Automated ingestion of Medicaid, Medicare, NIH, FDA and NEPPS data
- Validation of prescriptions against NIH, FDA and NEPPS
- Union of prescription from Medicaid and Medicare

RxMiner is scalable and built on Amazon S3, EC2, and Redshift on AWS. Tableau can be used to visualize and analyze the standardized dataset or each data source.

[Slides](https://docs.google.com/presentation/d/1z6SpBYRWqAIjsW7Khf0E44y8RqzUDVtGs8ziCrJ-xoA)

![Pipeline](docs/pipeline.png)

# Requirments
* Python 3.7
* Amazon AWS Account
* SODA API

# Installation
Clone the RxMiner project to your local computer or `m4.4xlarge` EC2 instance and install awscli and other requirements.

```bash
$ git clone https://github.com/ziyunch/RxMiner.git
$ pip install awscli
$ pip install ./requirements.txt
```

Next add the following credentials as environment variables to your `~/.bash_profile`.

```bash
# AWS Credentials
export AWS_BUCKET_NAME=XXXX
export AWS_ACCESS_KEY_ID=XXXX
export AWS_SECRET_ACCESS_KEY=XXXX
# SODA API
export SODAPY_APPTOKEN=XXXX
# Redshift configuration
export REDSHIFT_USER=XXXX
export REDSHIFT_PASSWORD=XXXX
export REDSHIFT_HOST_IP=XXXX
export REDSHIFT_PORT=XXXX
export REDSHIFT_DATABASE=XXXX
# PostgreSQL configuration
export POSTGRESQL_USER=XXXX
export POSTGRESQL_PASSWORD=XXXX
export POSTGRESQL_HOST_IP=XXXX
export POSTGRESQL_PORT=XXXX
export POSTGRESQL_DATABASE=XXXX
```

Source the `.bash_profile` when finished.

```bash
$ source ~/.bash_profile
```

Then import data into S3 bucket.

```bash
$ . ./deployment/import.sh
```

Ingest data and stage them in SQL database.

```bash
$ . ./deployment/ingest.sh
```

Prepare sample table based on queries.

```bash
$ . ./deployment/sql.sh
```
# Getting Started

Open the website at [http://rxminer.net](http://rxminer.net). Four graphs are shown in the interactive dashboard. On the top is the market size of all or chosen drugs over the states. Bottom left shows the Top 10 categories of drugs shared most markets for the whole nation or chosen state. Bottom middle shows the market of each drug in the descending order. Bottom right shows the drug usage for each year.

The sample visualization of national drug usage during 2013-2016 was also published on [Tableau public server](https://public.tableau.com/profile/runhan.yu#!/vizhome/rxminer2/Dashboard1).

The standardized dataset or each data source could be connected by `Tableau Desktop 2018.3` or other data analysis or visualization tools.

# Credits

RxMiner was built as a project at Insight Data Engineering in the Winter 2019 session by Runhan Yu. It is availble as open source and is free to use and modify by anyone.
