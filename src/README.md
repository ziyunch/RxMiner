# Import
Raw data were collected from multiple sources using three methods and stored in a S3 bucket.

1. Web crawler
- [import/rxnorm_import.py](https://github.com/ziyunch/RxMiner/blob/master/src/import/rxnorm_import.py)
I used regex pattern `<a\s*href=[\'|"](.*?/kss/rxnorm/RxNorm_full_\d+.zip)[\'|"]>` to find all RxNorm zip files and downloaded them into my S3 bucket.

- [ingest/mylib/rxgen_parser.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/mylib/rxgen_parser.py)
I used `beautifulsoup4` to find all stem names except for those in a subgroup and converted the stem name into regex patterns and stored in a dictionary for future usage.

2. API
- [import/soda_import.py](https://github.com/ziyunch/RxMiner/blob/master/src/import/soda_import.py)
I used SODA API to download Medicare and Medicaid datasets from Socrata database and stored them in my S3 bucket.

3. From other S3 bucket
I created and attached a policy to allow `getObject` request on `arn:aws:s3:::download.open.fda.gov` on my IAM group and copyed national drug code data from FDA's S3 bucket using `aws s3 cp`.

# Ingest
- [ingest/medicaid_ingest.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/medicaid_ingest.py)
I ingested healthcare datasets from Medicaid and read them chunk by chunk. National Drug Codes (NDC) were standardized while processing. The data table was staged in Redshift as `sdud`.

- [ingest/medicare_ingest.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/medicare_ingest.py)
I ingested healthcare datasets from Medicare and read them chunk by chunk. The data table was staged in Redshift as `pupd`. I use a seperate script to parse through Medicare datasets and collect unique generic names and classify the drugs by regex patterns. This table was staged in Redshift as `pupd_genclass`.

- [ingest/docinfo_ingest.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/docinfo_ingest.py)
I ingested and cleaned National Providers Identification (NPI) datasets chunk by chunk. Postal code and practice state information were cleaned while processing. The data table was staged in Redshift as `npidata`.

- [ingest/druginfo_ingest.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/druginfo_ingest.py)
I ingested and cleaned FDA's drug database. NDC were standardized while processing. The data table was staged in Redshift as `ndc9` (generic name specific) and `ndc11` (packaging specific). I also classify drugs by each unique generic names and staged the table in Redshift as `genclass`.

- [ingest/mylib/rxgen_parser.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/mylib/rxgen_parser.py)
I used `beautifulsoup4` to find all stem names except for those in a subgroup and converted the stem name into regex patterns and stored in a dictionary for future usage.

# Database
- [ingest/mylib/db_connec.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/mylib/db_connect.py)
`sqlalchemy` and `sqlalchemy-redshift` were used as the ODBC connector for PostgreSQL and Redshift, respectively.

- [ingest/mylib/glob_func.py](https://github.com/ziyunch/RxMiner/blob/master/src/ingest/mylib/glob_func.py)
I saved the dataframe into `.csv` file in memory (for PostgreSQL) or S3 bucket (for Redshift) and buck copy the table into PostgreSQL or Redshift to increase the saving speed.

#
Raw data were collected and stored in S3 bucket.
```
.
├── npi
│   ├── npidata_pfile_20050523-20190113.csv
│   ├── npidata_pfile_20050523-20190113_FileHeader.csv
│   └── NPPES_Data_Dissemination_CodeValues.pdf
├── openfda
│   ├── drug-ndc-0001-of-0001.json.zip
│   └── ndc_schema.json
├── pupd
│   ├── medicare_pupd_2013.csv
│   ├── medicare_pupd_2014.csv
│   ├── medicare_pupd_2015.csv
│   └── medicare_pupd_2016.csv
├── rxnorm
│   ├── 01022008.zip
│   ├── 01022018.zip
│   ├── ...
│   ├── 12092005.zip
│   └── 12212006.zip
└── sdud
    ├── medicaid_sdud_2013.csv
    ├── medicaid_sdud_2014.csv
    ├── medicaid_sdud_2015.csv
    └── medicaid_sdud_2016.csv
```

# SQL
![Schema Design](docs/schema.png)

The healthcare datasets from medicaid and medicare were first validated by npi (doctor's information) and ndc (drug's information). Drug events failed the validation were collected in the `error` table.

Four columns (`state`, `drug_class`, `generic_name` and `total_drug_cost`) were joined for the national drug usage analysis.

`psycopg2` were used as ODBC connector.