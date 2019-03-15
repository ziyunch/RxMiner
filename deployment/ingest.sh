#!/bin/bash

python3 ./../src/ingest/medicaid_ingest.py
python3 ./../src/ingest/medicare_ingest.py
python3 ./../src/ingest/medicare_genclass.py
python3 ./../src/ingest/docinfo_ingest.py
python3 ./../src/ingest/druginfo_ingest.py