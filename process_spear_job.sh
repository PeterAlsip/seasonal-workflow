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

# Example usage:
# sbatch process_spear_job.sh atmos monthly_mean t_ref 1 config_nwa12_cobalt.yaml
# (Gets monthly mean near surface air temperature for ensemble #1 for location and times in config file)

# Other users could change to own venv
source /home/Andrew.C.Ross/git/seasonal-workflow/.venv/bin/activate.csh
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

