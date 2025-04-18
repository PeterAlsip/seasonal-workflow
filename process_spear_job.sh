#!/bin/tcsh
#SBATCH --ntasks=1
#SBATCH --job-name=process_spear
#SBATCH --time=2880
#SBATCH --partition=batch
#SBATCH --output=logs/spear_%j.out

# Wrapper to run workflow to extract and combine SPEAR forecasts.
# Should be sbatch-ed,
# but running more than a few at a time seems to cause
# failures during dmget. 

# Newer prebuilt python envs
# are missing dask and/or yaml
module load miniforge
conda activate /nbhome/acr/python/envs/medpy311_20250213
module load gcp
    
set domain=$1
set freq=$2
set var=$3
set ens=$4
set config=$5

set files=`python spear_path.py -d $domain -f $freq -v $var -e $ens -c $config`
dmget $files
gcp $files $TMPDIR/
set processed=`python process_spear.py -r $TMPDIR -d $domain -f $freq -v $var -e $ens -c $config`
gcp $processed /work/$USER/spear/forecast_output_data/

