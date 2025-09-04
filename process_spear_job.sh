#!/bin/bash -e
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
# sbatch process_spear_job.sh atmos monthly t_ref 1 config_nwa12_cobalt.yaml
# (Gets monthly mean near surface air temperature for ensemble #1 for location and times in config file)


# Domain corresponds to one of the directories that the SPEAR post-processed data are in. Some of the possibilities are:
#- atmos (monthly average atmosphere)
#- atmos_daily (daily average atmosphere)
#- atmos_4xdaily (6 hourly instantaneous atmosphere)
#- ocean (monthly 2D ocean)
#- ocean_z (monthly 3D ocean)

# Other users could change to own venv
source /home/Peter.Alsip/git/seasonal-workflow/.venv/bin/activate
module load gcp

domain=$1
freq=$2
var=$3
ens=$4
config=$5

files=`python src/workflow_tools/spear.py -d $domain -f $freq -v $var -e $ens -c $config`
dmget $files
gcp $files $TMPDIR/
processed=`python process_spear.py -r $TMPDIR -d $domain -f $freq -v $var -e $ens -c $config`
# Note that where to put the output is not read from the config file here.
gcp -cd $processed /work/$USER/spear/forecast_output_data/

