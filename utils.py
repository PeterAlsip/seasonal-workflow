from dataclasses import dataclass
from os import environ
from getpass import getuser
import numpy as np
from pathlib import Path
from shutil import which
from subprocess import run, DEVNULL
import xarray


@dataclass
class HSMGet():
    archive: Path = Path('/') # hopefully this will duplicate paths used by frepp
    ptmp: Path = Path('/ptmp') / getuser()
    tmp: Path = Path(environ.get('TMPDIR', ptmp)) # can this ref self already?

    def __call__(self, path_or_paths):
        if which('hsmget') is None:
            print('Not using hsmget')
            return path_or_paths
        elif isinstance(path_or_paths, Path):
            relative = path_or_paths.relative_to(self.archive)
            # hsmget will do the dmget first and this is fine since it's one file
            cmd = f'hsmget -q -a {self.archive.as_posix()} -w {self.tmp.as_posix()} -p {self.ptmp.as_posix()} {relative.as_posix()}'
            run(cmd, shell=True, check=True, stdout=DEVNULL, stderr=DEVNULL)
            return (self.tmp / relative)
        elif iter(path_or_paths):
            p_str = ' '.join([p.as_posix() for p in path_or_paths])
            run(f'dmget {p_str}', shell=True, check=True)
            relative = [p.relative_to(self.archive) for p in path_or_paths]
            rel_str = ' '.join([r.as_posix() for r in relative])
            cmd = f'hsmget -q -a {self.archive.as_posix()} -w {self.tmp.as_posix()} -p {self.ptmp.as_posix()} {rel_str}'
            run(cmd, shell=True, check=True, stdout=DEVNULL, stderr=DEVNULL)
            return [self.tmp / r for r in relative]
        else:
            raise Exception('Need a Path or iterable of Paths to get')
            

def open_var(pp_root, kind, var, hsmget=HSMGet()):
    print(pp_root)
    freq = 'daily' if 'daily' in kind or 'nwshelf' in kind else 'monthly'
    longslice1 = '19930101-20221231' if freq == 'daily' else '199301-202212'
    longfile1 = pp_root / 'pp' / kind / 'ts' / freq / '30yr' / f'{kind}.{longslice1}.{var}.nc'
    if longfile1.exists():
        tmpfile = hsmget(longfile1)
        return xarray.open_dataset(longfile1)[var]
    else:
        short_files = list((pp_root / 'pp' / kind / 'ts' / freq / '5yr').glob(f'{kind}.*.{var}.nc'))
        if len(short_files) > 0:
            tmpfiles = hsmget(sorted(short_files))
            return xarray.open_mfdataset(short_files)[var]
        else:
            raise Exception('Did not find postprocessed files')


def pad_ds(ds):
    if not isinstance(ds.time.values[0], np.datetime64):
        # use python datetimes
        ds['time'] = ds['time'].to_index().to_datetimeindex()
    
    # convert time bounds to days
    if 'time_bnds' in ds:
        ds['time_bnds'] = (ds['time_bnds'].astype('int') / (1e9 * 24 * 60 * 60)).astype('int')

    # Pad by duplicating the first data point and inserting it as one day before the start
    tfirst = ds.isel(time=0).copy()
    for var in ['time', 'average_T1', 'average_T2']:
        if var in tfirst:
            tfirst[var] = tfirst[var] - np.timedelta64(1, 'D')
    if 'time_bnds' in tfirst:
        tfirst['time_bnds'] = tfirst['time_bnds'] - 1

    # Pad by duplicating the last data point and inserting it as one day after the end
    tlast = ds.isel(time=-1).copy()
    for var in ['time', 'average_T1', 'average_T2']:
        if var in tlast:
            tlast[var] = tlast[var] + np.timedelta64(1, 'D')
    if 'time_bnds' in tlast:
        tlast['time_bnds'] = tlast['time_bnds'] + 1

    # Combine the duplicated and original data
    tcomb = xarray.concat((tfirst, ds, tlast), dim='time').transpose('time', 'lat', 'lon', 'bnds')

    if 'time_bnds' in tcomb:
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


def smooth_climatology(da, window=5, dim='dayofyear'):
    smooth = da.copy()
    for _ in range(2):
        smooth = xarray.concat([
            smooth.isel(**{dim: slice(-window, None)}),
            smooth,
            smooth.isel(**{dim: slice(None, window)})
        ], dim)
        smooth = (
            smooth
            .rolling(**{dim: (window * 2 + 1)}, center=True, min_periods=1)
            .mean()
            .isel(**{dim: slice(window, -window)})
        )
    return smooth