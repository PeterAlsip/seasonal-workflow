# to create liquid precip:
# cdo -setrtoc,-1e9,0,0 -chname,tp,lp -sub ERA5_tp_${y}_padded.nc ERA5_sf_${y}_padded.nc ERA5_lp_${y}_padded.nc

import concurrent.futures as futures
from dataclasses import dataclass
import os
import pandas as pd
from pathlib import Path
import subprocess
import xarray

from getpass import getuser
import sys
sys.path.append('../..')
from utils import HSMGet
hsmget = HSMGet(archive=Path('/archive/uda'), ptmp=Path('/ptmp')/getuser())


def run_cmd(cmd):
   subprocess.run([cmd], shell=True, check=True) 



# TODO: hardcoded config

# To reduce file size, only extract and pad this
# lat/lon subset that encompasses the NWA domain.
REGION_SLICE = 'latitude,0.0,60.0 -d longitude,260.0,325.0'

# Location to save temporary data to.
# Use TMPDIR as set on ppan.
TMP = Path(os.environ['TMPDIR'])

# Location to save the final padded data to.
FINAL = Path('/work/acr/mom6/nwa12/analysis_input_data/atmos')

# Location of the latest ERA5 data
UDA = Path('/archive/uda/CEFI/ERA5/')


variables = {
    'mean_sea_level_pressure': 'msl',
    'surface_pressure': 'sp',
    'total_precipitation': 'tp',
    'snowfall': 'sf',
    'surface_solar_radiation_downwards': 'ssrd',
    'surface_thermal_radiation_downwards': 'strd',
    '2m_temperature': 't2m',
    '2m_dewpoint_temperature': 'd2m',
    '10m_u_component_of_wind': 'u10',
    '10m_v_component_of_wind': 'v10'
}


def thread_worker(month_file):
    out_file = (TMP / month_file.name)
    # Slice to subregion and make time unlimited
    run_cmd(f'ncks -d {REGION_SLICE} --mk_rec_dmn time {month_file.as_posix()} -O {out_file.as_posix()}')
    # Flip latitude so it is south to north.
    run_cmd(f'ncpdq -a "time,-latitude,longitude" {out_file.as_posix()} -O {out_file.as_posix()}')
    return out_file


def main(year):
    for long_name, file_var in variables.items():
        print(file_var)
        found_files = []
        for mon in range(1, 13):
            uda_file = UDA / long_name / f'ERA5_{long_name}_{mon:02d}{year}.nc'
            if uda_file.is_file():
                found_files.append(uda_file)
            else:
                if mon == 1:
                    raise Exception('Did not find any files for this year')
                else:
                    print(f'Found files for month 1 to {mon-1}')
                    break
        
        print('hsmget')
        tmp_files = hsmget(found_files)
        print('add record dim')
        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            processed_files = sorted(executor.map(thread_worker, tmp_files))

        # Join together and format metadata using xarray.
        # Using xarray partly because ncrcat is strangely slow on these files.
        # TODO:  pad
        print('concat')
        ds = xarray.open_mfdataset(processed_files)
        # pad
        tail = ds.isel(time=-1)
        tail['time'] = tail['time'] + pd.Timedelta(hours=1)
        ds = xarray.concat((ds, tail), dim='time').transpose('time', ...)
        # Temporary hack for test run, where the time data
        # was accidentally rolled back by 1 day during qa/qc.
        ds['time'] = ds['time'].to_pandas() + pd.Timedelta(days=1)
        all_vars = list(ds.data_vars.keys()) + list(ds.coords.keys())
        encodings = {v: {'_FillValue': None, 'dtype': 'float32'} for v in all_vars}
        encodings['time'].update({'dtype':'float64', 'calendar': 'gregorian', 'units': 'hours since 1990-01-01'})
        out_file = FINAL / f'ERA5_{file_var}_{year}_padded.nc'
        ds.to_netcdf(out_file, encoding=encodings, unlimited_dims='time')
        ds.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year', type=int, required=True)
    args = parser.parse_args()
    main(args.year)