#!/bin/tcsh
#SBATCH --ntasks=1
#SBATCH --job-name=process_spear
#SBATCH --time=2880
#SBATCH --partition=batch

# Wrapper to run workflow to extract and combine SPEAR forecasts.
# Should be sbatch-ed,
# but running more than a few at a time seems to cause
# failures during dmget. 

module load python/3.9
module load gcp
    
set domain=$1
set freq=$2
set var=$3
set ens=$4

set files=`python spear_path.py $domain $freq $var $ens`
dmget $files
gcp $files $TMPDIR/
set processed=`python process_spear.py $TMPDIR $domain $freq $var $ens`
gcp $processed /work/acr/spear/processed/

