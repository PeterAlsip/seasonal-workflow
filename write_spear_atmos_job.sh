#!/bin/bash -e

module load python/3.9 gcp cdo/2.1.1

# TODO: how to use custom config
python write_spear_atmos.py -y $1 -m $2 -e $3 -c config_nwa12.yaml