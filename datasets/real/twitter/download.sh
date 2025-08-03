#!/bin/bash -x
# Based on https://zenodo.org/records/15169905

wget http://twitter.mpi-sws.org/links-anon.txt.gz;
gzip -d links-anon.txt.gz;
