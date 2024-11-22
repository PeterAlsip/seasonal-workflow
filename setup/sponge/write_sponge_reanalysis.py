import numpy as np
import os
import pandas as pd
from pathlib import Path
import sys
import xarray


sys.path.append('../boundary')
from boundary import reuse_regrid


def round_coords(ds, to=25):
    ds['latitude'] = np.round(ds['latitude'] * to ) / to
    ds['longitude'] = np.round(ds['longitude'] * to) / to
    return ds


hgrid = xarray.open_dataset('../../../nwa12/setup/grid/ocean_hgrid.nc')
target_t = (
    hgrid
    [['x', 'y']]
    .isel(nxp=slice(1, None, 2), nyp=slice(1, None, 2))
    .rename({'y': 'lat', 'x': 'lon', 'nxp': 'xh', 'nyp': 'yh'})
)

input_dir = Path('/work/acr/mom6/nwa12/analysis_input_data/sponge/monthly_filled')
output_dir = input_dir.parents[0]

variables = ['thetao', 'so']

years = [2024]#range(2022, 2024)
for year in years:
    print(f'  {year}')
    files = list(input_dir.glob(f'glorys_*_{year}-??.nc'))
    glorys = (
        xarray.open_mfdataset(files, chunks='auto', preprocess=lambda x: round_coords(x, to=12)) # without auto, chunks will be weird and loading will be slow
        .rename({'latitude': 'lat', 'longitude': 'lon'})
        .sel(depth=slice(None, 5300)) # make sure empty last depth is excluded
        [variables]
    ).load()
    print('Interpolating')
    glorys_to_t = reuse_regrid(
        glorys, target_t, 
        filename=os.path.join(os.environ['TMPDIR'], 'regrid_glorys_t.nc'), 
        method='nearest_s2d', 
        reuse_weights=False, #! Not reusing
        periodic=False
    )
    interped = (
        glorys_to_t(glorys)
        .drop_vars(['lon', 'lat'], errors='ignore')
        .compute()
    ) 

    # Add data points at end of month, since time_bnds aren't used
    # All points extend to 23:59:59 at end of month, except
    # for the end of the year which is padded to 00:00:00 the next Jan 1.
    # normalize=True rolls down to midnight.
    # Not sure how this handles a case where day = 1 but hour > 0
    mstart = [d - pd.offsets.MonthBegin(normalize=True) if d.day > 1 else d for d in interped['time'].to_pandas()]
    mend = [d + pd.DateOffset(months=1) - pd.Timedelta(seconds=1) for d in mstart]
    mend[-1] = mend[-1] + pd.Timedelta(seconds=1)
    starts = interped.copy()
    starts['time'] = mstart
    ends = interped.copy()
    ends['time'] = mend
    bounded = xarray.concat((starts, ends), dim='time').sortby('time')

    # Ensure that order is correct so that time can be unlimited dim
    bounded = bounded.transpose('time', 'depth', 'yh', 'xh')

    bounded['xh'] = (('xh', ), target_t.xh.data)
    bounded['yh'] = (('yh', ), target_t.yh.data)

    all_vars = list(bounded.data_vars.keys()) + list(bounded.coords.keys())
    encodings = {v: {'_FillValue': None} for v in all_vars}
    encodings['time'].update({'dtype':'float64', 'calendar': 'gregorian', 'units': 'days since 1993-01-01'})
    for v in ['xh', 'yh']:
        encodings[v].update({'dtype': np.int32})
    bounded['depth'].attrs = {
        'units': 'meter',
        'cartesian_axis': 'Z',
        'positive': 'down'
    }

    bounded['time'].attrs['cartesian_axis'] = 'T'
    bounded['xh'].attrs = {'cartesian_axis': 'X'}
    bounded['yh'].attrs = {'cartesian_axis': 'Y'}

    print('Writing')
    bounded.to_netcdf(
        os.path.join(output_dir, f'glorys_sponge_monthly_bnd_{year}.nc'),
        format='NETCDF3_64BIT',
        engine='netcdf4',
        encoding=encodings,
        unlimited_dims='time'
    )
    glorys.close()
