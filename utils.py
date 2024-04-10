import numpy as np
import xarray

# This is tricky to install, so importing by adding to path
import sys
sys.path.append('/home/Andrew.C.Ross/git/HCtFlood')
from HCtFlood import kara as flood


def flood_ds(ds, ocean_mask):
    # Find the variable being flooded, which is the first on that is not a coordinate variable
    var = next(x for x in ds.data_vars if x not in ['average_DT', 'average_T1', 'average_T2', 'lat_bnds', 'lon_bnds', 'time_bnds'])
    # ocean_mask is true where model is ocean
    flooded = flood.flood_kara(ds[var].where(ocean_mask)).isel(z=0)
    ds[var] = flooded
    return ds


def pad_ds(ds):
    if not isinstance(ds.time.values[0], np.datetime64):
        # use python datetimes
        ds['time'] = ds['time'].to_index().to_datetimeindex()
    
    # convert time bounds to days
    ds['time_bnds'] = (ds['time_bnds'].astype('int') / (1e9 * 24 * 60 * 60)).astype('int')

    # Pad by duplicating the first data point and inserting it as one day before the start
    tfirst = ds.isel(time=0).copy()
    tfirst['time'] = tfirst['time'] - np.timedelta64(1, 'D')
    tfirst['average_T1'] = tfirst['average_T1'] - np.timedelta64(1, 'D')
    tfirst['average_T2'] = tfirst['average_T2'] - np.timedelta64(1, 'D')
    tfirst['time_bnds'] = tfirst['time_bnds'] - 1

    # Pad by duplicating the last data point and inserting it as one day after the end
    tlast = ds.isel(time=-1).copy()
    tlast['time'] = tlast['time'] + np.timedelta64(1, 'D')
    tlast['average_T1'] = tlast['average_T1'] + np.timedelta64(1, 'D')
    tlast['average_T2'] = tlast['average_T2'] + np.timedelta64(1, 'D')
    tlast['time_bnds'] = tlast['time_bnds'] + 1

    # Combine the duplicated and original data
    tcomb = xarray.concat((tfirst, ds, tlast), dim='time').transpose('time', 'lat', 'lon', 'bnds')
    tcomb['time_bnds'] = tcomb['time_bnds'] + 1

    tcomb['time'].attrs = ds['time'].attrs

    tcomb['lat_bnds'] = (('lat', 'bnds'), tcomb['lat_bnds'].isel(time=0).data)
    tcomb['lon_bnds'] = (('lon', 'bnds'), tcomb['lon_bnds'].isel(time=0).data)
    return tcomb
    

def write_ds(ds, fout):
    for v in ds:
        if ds[v].dtype == 'float64':
            ds[v].encoding['_FillValue']= 1.0e20
    ds.to_netcdf(
        fout,
        format='NETCDF3_64BIT',
        engine='netcdf4',
        encoding={'time': {'dtype': 'float64', 'calendar': 'gregorian'}},
        unlimited_dims=['time']
    )


def modulo(ds):
    ds['time'] = np.arange(0, 365, dtype='float')
    ds['time'].attrs['units'] = 'days since 0001-01-01'
    ds['time'].attrs['calendar'] = 'noleap'
    ds['time'].attrs['modulo'] = ' '
    ds['time'].attrs['cartesian_axis'] = 'T'
    return ds


def smooth_climatology(da, window=5):
    smooth = da.copy()
    for _ in range(2):
        smooth = xarray.concat([
            smooth.isel(dayofyear=slice(-window, None)),
            smooth,
            smooth.isel(dayofyear=slice(None, window))
        ], 'dayofyear')
        smooth = (
            smooth
            .rolling(dayofyear=(window * 2 + 1), center=True, min_periods=1)
            .mean()
            .isel(dayofyear=slice(window, -window))
        )
    return smooth