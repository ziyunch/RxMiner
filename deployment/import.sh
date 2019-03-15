#!/bin/bash

python3 ./../src/import/rxnorm_import.py
python3 ./../src/import/soda_import.py

aws s3 cp s3://download.open.fda.gov/drug/ndc/ s3:/$AWS_BUCKET_NAME/openfda/ --recursive
