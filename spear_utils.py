import numpy as np
import os
from pathlib import Path
import subprocess
import xarray
from spear_path import get_spear_path, get_spear_paths

# This is tricky to install, so importing by adding to path
sys.path.append('/home/Andrew.C.Ross/git/HCtFlood')
from HCtFlood import kara as flood


def extract_spear(domain, ystart, mstart, ens, outdir=None):
    if outdir is None:
        tmp = Path(os.environ['TMPDIR'])
        if ens == 'pp_ensemble':
            outdir = tmp / f'{ystart}-{mstart:02d}-{ens}_raw'
        else:
            outdir = tmp / f'{ystart}-{mstart:02d}-e{ens:02d}_raw'
        outdir.mkdir(exist_ok=True)

    if domain == 'atmos':
        files = get_spear_paths(
            ['slp', 't_ref', 'u_ref', 'v_ref', 'q_ref', 'lwdn_sfc', 'swdn_sfc', 'precip'],
            ystart, mstart, 'atmos_daily', 'daily', ens=ens
        )
    elif domain == 'ocean':
        files = get_spear_paths(
            ['so', 'thetao'],
            ystart, mstart, 'ocean_z', 'monthly', ens=ens
        )
        files.append(get_spear_path(ystart, mstart, 'ice_daily', 'daily', 'SSH', ens='pp_ensemble'))
    else:
        raise ValueError(f'Unrecognized domain: {domain}')
    file_strings = list(map(str, files))
    subprocess.run(['dmget ' + ' '.join(file_strings)], shell=True, check=True)
    subprocess.run(['gcp --sync ' + ' '.join(file_strings) + ' ' + str(outdir)], shell=True, check=True)
    new_files = [outdir / f.name for f in files]
    return new_files


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
