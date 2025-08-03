#!/bin/bash -x
# Based on https://zenodo.org/records/15169905

wget https://datasets.imdbws.com/title.basics.tsv.gz;
gzip -d title.basics.tsv.gz;

wget https://datasets.imdbws.com/name.basics.tsv.gz;
gzip -d name.basics.tsv.gz;
