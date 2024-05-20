#!/bin/bash -e
#SBATCH --output=logs/%j.out
# Usage: sbatch write_spear_atmos_job.sh YEAR MONTH ENSEMBLE CONFIG
module load python/3.9 gcp cdo/2.1.1
python write_spear_atmos.py -y $1 -m $2 -e $3 -c $4