#!/bin/bash -e

module load python/3.9 gcp cdo/2.1.1

python write_spear_atmos.py $1 $2 $3