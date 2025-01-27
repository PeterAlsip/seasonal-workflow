import numpy as np
import xarray

from utils import modulo, smooth_climatology


def process_climatology(years, input_files, output_dir):
    print('Opening dataset')
    rivers = xarray.open_mfdataset(
        input_files,
        preprocess=lambda x: x.isel(time=slice(1, -1)) # skip padded days (both ends are padded in glofas v4)
    ) 
    vardata = rivers.runoff
    print('Calculating climatology by day')
    ave = vardata.groupby('time.dayofyear').mean('time').sel(dayofyear=slice(1, 365)).load()
    print('Smoothing daily climatology')
    smoothed = smooth_climatology(ave).rename({'dayofyear': 'time'}).load()
    print('Preparing to write')
    smoothed = modulo(smoothed)
    # time gets inserted when using open_mfdataset
    res = rivers[['area', 'lat', 'lon']].isel(time=0).drop_vars('time')
    # add smoothed result to res
    res['runoff'] = smoothed
    print('Writing')
    res.to_netcdf(
        output_dir / f'glofas_runoff_climo_{years[0]:d}_{years[-1]:d}.nc',
        unlimited_dims='time'
    )


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)

    years = np.arange(config['climatology']['first_year'], config['climatology']['last_year']+1)
    input_files = [config['filesystem']['yearly_river_files'].format(year=y) for y in years]
    work_dir = Path(config['filesystem']['forecast_input_data']) / 'rivers'
    work_dir.mkdir(exist_ok=True)
    process_climatology(years, input_files, work_dir)
