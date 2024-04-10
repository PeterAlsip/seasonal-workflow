import numpy as np
import xarray

from utils import modulo, smooth_climatology


def process_climatology(years, input_files, output_dir):
    print('Opening dataset')
    rivers = xarray.open_mfdataset(
        input_files,
        preprocess=lambda x: x.isel(time=slice(None, -1)) # skip padded days
    ) 
    vardata = rivers.runoff
    print('Calculating climatology by day')
    ave = vardata.groupby('time.dayofyear').mean('time').sel(dayofyear=slice(1, 365)).load()
    print('Smoothing daily climatology')
    smoothed = smooth_climatology(ave).rename({'dayofyear': 'time'}).load()
    print('Preparing to write')
    smoothed = modulo(smoothed)
    # time gets inserted when using open_mfdataset
    res = rivers[['area', 'lat', 'lon']].isel(time=0).drop('time')
    # add smoothed result to res
    res['runoff'] = smoothed
    print('Writing')
    res.to_netcdf(
        output_dir / f'glofas_runoff_climo_{years[0]:d}_{years[-1]:d}.nc',
        format='NETCDF3_64BIT',
        engine='netcdf4',
        unlimited_dims='time'
    )


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config')
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)

    years = np.arange(config['dates']['climatology']['first_year'], config['dates']['climatology']['last_year']+1)
    input_files = [config['filesystem']['yearly_river_files'].format(y=y) for y in years]
    work_dir = Path(config['filesystem']['model_input_data']) / 'rivers'
    work_dir.mkdir(exist_ok=True)
    process_climatology(years, input_files, work_dir)
