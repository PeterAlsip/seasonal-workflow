# to create liquid precip:
# cdo -setrtoc,-1e9,0,0 -chname,tp,lp -sub ERA5_tp_${y}_padded.nc ERA5_sf_${y}_padded.nc ERA5_lp_${y}_padded.nc

from dataclasses import dataclass
import os
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
# TODO: interim files have -180 to 180 lon but uda files have 0--360
# REGION_SLICE = 'latitude,0.0,60.0 -d longitude,260.0,325.0'
REGION_SLICE = 'latitude,0.0,60.0 -d longitude,-100.0,-35.0'

# Location to save temporary data to.
# Use TMPDIR as set on ppan.
TMP = Path(os.environ['TMPDIR'])

# Location to save the final padded data to.
FINAL = Path('/work/acr/mom6/nwa12/analysis_input_data/atmos')

# Location of the complete archive of ERA5 data
UDA = Path('/archive/uda/ERA5/Hourly_Data_On_Single_Levels/reanalysis/global/1hr-timestep/annual_file-range/')

INTERIM = Path('/net2/acr/era5/interim')


@dataclass
class ERA5var():
    name: str
    long_name: str
    file_var: str

    @property
    def group(self):
        if 'pressure' in self.long_name or 'temperature' in self.long_name:
            return 'Temperature_and_Pressure'
        elif 'precipitation' in self.long_name:
            return 'Precipitation_and_Rain'
        elif 'snow' in self.long_name:
            return 'Snow'
        elif 'radiation' in self.long_name:
            return 'Radiation_and_Heat'
        elif 'wind' in self.long_name:
            return 'Wind'

variables = [
    ERA5var('P_mean_sea_level', 'mean_sea_level_pressure',  'msl'),
    # ERA5var('P_surface', 'surface_pressure',  'sp'),
    # ERA5var('Pr_total', 'total_precipitation',  'tp'),
    # ERA5var('snowfall', 'snowfall',  'sf'),
    # ERA5var('sfc_solar_rad_downward', 'surface_solar_radiation_downwards',  'ssrd'),
    # ERA5var('sfc_thermal_rad_downward', 'surface_thermal_radiation_downwards',  'strd'),
    ERA5var('T_2m', '2m_temperature',  't2m'),
    ERA5var('Td_2m', '2m_dewpoint_temperature',  'd2m'),
    ERA5var('u_10m', '10m_u_component_of_wind',  'u10'),
    ERA5var('v_10m', '10m_v_component_of_wind',  'v10'),
]


def find_best_file(year, var):
    uda_file = UDA / var.group / var.name / f'ERA5_1hr_{var.long_name}_{year}.nc'
    if uda_file.is_file():
        return hsmget(uda_file)
    else:
        # interim_file = INTERIM / f'ERA5_1hr_{var.file_var}_{year}.nc'
        # temporary name for these files 
        interim_file = INTERIM / f'{var.file_var}.nc'
        if interim_file.is_file():
            return interim_file
        else:
            raise Exception(f'Did not find a file for {year} {var.long_name}. Looked for {interim_file.as_posix()}')


def main(year):
    for var in variables:
        print(var.file_var)
        year_file = find_best_file(year, var)
        tmp_file = TMP / year_file.name
        print('copy')
        run_cmd(f'gcp --sync {year_file.as_posix()} {tmp_file.as_posix()}')
        print('slice')
        sliced_file = tmp_file.with_name(tmp_file.name.replace(var.long_name, var.file_var)).with_suffix('.sliced.nc')
        cmd = f'ncks {tmp_file.as_posix()} -d {REGION_SLICE} -O {sliced_file.as_posix()}'
        run_cmd(cmd)

        print('xarray')
        ds = xarray.open_dataset(sliced_file)
        ds = ds.isel(latitude=slice(None, None, -1))
        ds = ds.rename({'valid_time': 'time'})
        # breakpoint()
        # for key, att in ds[var.file_var].attrs.items():
        #     if 'valid_time' in att:
        #         ds[var.file_var][key] = att.rename({'valid_time': 'time'})
        ds[var.file_var].attrs = {}
        if 'coordinates' in ds[var.file_var].encoding:
            del ds[var.file_var].encoding['coordinates']
        all_vars = list(ds.data_vars.keys()) + list(ds.coords.keys())
        encodings = {v: {'_FillValue': None, 'dtype': 'float32'} for v in all_vars}
        encodings['time'].update({'dtype':'float64', 'calendar': 'gregorian', 'units': 'hours since 1990-01-01'})
        out_file = FINAL / f'ERA5_{var.file_var}_{year}_padded.nc'
        # breakpoint()
        ds.to_netcdf(out_file, encoding=encodings, unlimited_dims='time')
        ds.close()
        # # Make time the unlimited dimension.
        # print('  fix time')
        # # breakpoint()
        # # interim files need to have valid_time renamed to time
        # run_cmd(f'ncrename -d valid_time,time -v valid_time,time {sliced_file.as_posix()}')
        # run_cmd(f'ncks --mk_rec_dmn time {sliced_file.as_posix()} -O {sliced_file.as_posix()}')

        # # Unpack the data; FMS doesn't seem to work with packed data?
        # print('  unpack')
        # # interim files do not need to be unpacked
        # # run_cmd(f'ncpdq -U {sliced_file.as_posix()} -O {sliced_file.as_posix()}')

        # # Latitude is stored north to south, so flip it.
        # print('  flip')
        # run_cmd(f'ncpdq -a "time,-latitude,longitude" {sliced_file.as_posix()} -O {sliced_file.as_posix()}')

        # Delete the large temporary file.
        print(f'  rm')
        tmp_file.unlink()
        sliced_file.unlink()

        # copy the final file to output location
        # out_file = FINAL / f'ERA5_{var.file_var}_{year}_padded.nc'
        # run_cmd(f'gcp {sliced_file.as_posix()} {out_file.as_posix()}')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year', type=int, required=True)
    args = parser.parse_args()
    main(args.year)