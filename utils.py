import errno
import re
from dataclasses import dataclass
from functools import singledispatchmethod
from getpass import getuser
from os import environ
from pathlib import Path
from shutil import which
from subprocess import DEVNULL, run
from typing import Any

import numpy as np
import pandas as pd
import xarray


@dataclass
class HSMGet:
    archive: Path = Path('/')  # hopefully this will duplicate paths used by frepp
    ptmp: Path = Path('/ptmp') / getuser()
    tmp: Path = Path(environ.get('TMPDIR', ptmp))

    def _run(self, cmd: str, stdout=DEVNULL, stderr=DEVNULL) -> None:
        esc = re.sub(r'([\(\)])', r'\\\1', cmd)
        run(esc, shell=True, check=True, stdout=stdout, stderr=stderr)

    @singledispatchmethod
    def __call__(self, path_or_paths: Any, **kwargs: Any) -> Any:
        raise TypeError('Unsupported type for path to hsmget. Expected str, Path, or list[Path]')

    @__call__.register
    def _call_str(self, path: str, **kwargs: Any) -> Path:
        cast_path = Path(path)
        return self.__call__(cast_path, **kwargs)
    
    @__call__.register
    def _call_path(self, path: Path, **kwargs: Any) -> Path:
        if which('hsmget') is None:
            print('Not using hsmget')
            return path
        relative = path.relative_to(self.archive)
        # hsmget will do the dmget first and this is fine since it's one file
        cmd = f'hsmget -q -a {self.archive} -w {self.tmp} -p {self.ptmp} {relative.as_posix()}'
        self._run(cmd, **kwargs)
        return self.tmp / relative

    @__call__.register
    def _call_paths(self, paths: list, **kwargs: Any) -> list[Path]:
        if which('hsmget') is None:
            print('Not using hsmget')
            return paths
        p_str = ' '.join([p.as_posix() for p in paths])
        self._run(f'dmget {p_str}')
        relative = [p.relative_to(self.archive) for p in paths]
        rel_str = ' '.join([r.as_posix() for r in relative])
        cmd = f'hsmget -q -a {self.archive} -w {self.tmp} -p {self.ptmp} {rel_str}'
        self._run(cmd, **kwargs)
        return [self.tmp / r for r in relative]


def open_var(
    pp_root: Path, kind: str, var: str, hsmget: HSMGet = HSMGet()
) -> xarray.DataArray:
    freq = 'daily' if 'daily' in kind else 'monthly'
    pp_dir = pp_root / 'pp' / kind / 'ts' / freq
    if not pp_dir.is_dir():
        raise FileNotFoundError(
            errno.ENOENT, 'Could not find post-processed directory', str(pp_dir)
        )
    # Get all of the available post-processing chunk directories (assuming chunks in units of years)
    available_chunks = list(pp_dir.glob('*yr'))
    if len(available_chunks) == 0:
        raise FileNotFoundError(
            errno.ENOENT, 'Could not find post-processed chunk subdirectory'
        )
    # Sort from longest to shortest chunk
    sorted_chunks = sorted(
        available_chunks, key=lambda x: int(x.name[0:-2]), reverse=True
    )
    for chunk in sorted_chunks:
        # Look through the available chunks and return for the
        # largest chunk that has file(s).
        matching_files = list(chunk.glob(f'{kind}.*.{var}.nc'))
        # Treat 1 and > 1 files separately, though the > 1 case could probably handle both.
        if len(matching_files) > 1:
            tmpfiles = hsmget(sorted(matching_files))
            return xarray.open_mfdataset(tmpfiles, decode_timedelta=True)[
                var
            ]  # Avoid FutureWarning about decode_timedelta
        elif len(matching_files) == 1:
            tmpfile = hsmget(matching_files[0])
            return xarray.open_dataset(tmpfile, decode_timedelta=True)[
                var
            ]  # Avoid FutureWarning about decode_timedelta
    else:
        raise FileNotFoundError(
            errno.ENOENT,
            'Could not find any post-processed files. Check if frepp failed.',
        )


def pad_ds(ds: xarray.Dataset) -> xarray.Dataset:
    if not isinstance(ds.time.values[0], np.datetime64):
        # use python datetimes
        ds['time'] = ds['time'].to_index().to_datetimeindex()

    # convert time bounds to days
    if 'time_bnds' in ds:
        ds['time_bnds'] = (ds['time_bnds'].astype('int') / (1e9 * 24 * 60 * 60)).astype(
            'int'
        )

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
    tcomb = xarray.concat((tfirst, ds, tlast), dim='time').transpose(
        'time', 'lat', 'lon', 'bnds'
    )

    if 'time_bnds' in tcomb:
        tcomb['time_bnds'] = tcomb['time_bnds'] + 1

    tcomb['time'].attrs = ds['time'].attrs

    tcomb['lat_bnds'] = (('lat', 'bnds'), tcomb['lat_bnds'].isel(time=0).data)
    tcomb['lon_bnds'] = (('lon', 'bnds'), tcomb['lon_bnds'].isel(time=0).data)
    return tcomb


def write_ds(ds: xarray.Dataset, fout: str | Path) -> None:
    for v in ds:
        if ds[v].dtype == 'float64':
            ds[v].encoding['_FillValue'] = 1.0e20
    ds.to_netcdf(
        fout,
        format='NETCDF3_64BIT',
        engine='netcdf4',
        encoding={'time': {'dtype': 'float64', 'calendar': 'gregorian'}},
        unlimited_dims=['time'],
    )


def modulo(ds: xarray.Dataset) -> xarray.Dataset:
    ds['time'] = np.arange(0, 365, dtype='float')
    ds['time'].attrs['units'] = 'days since 0001-01-01'
    ds['time'].attrs['calendar'] = 'noleap'
    ds['time'].attrs['modulo'] = ' '
    ds['time'].attrs['cartesian_axis'] = 'T'
    return ds


def round_coords(
    ds: xarray.Dataset, to: float, lat: str = 'latitude', lon: str = 'longitude'
) -> xarray.Dataset:
    ds[lat] = np.round(ds[lat] * to) / to
    ds[lon] = np.round(ds[lon] * to) / to
    return ds


def smooth_climatology(
    da: xarray.DataArray | xarray.Dataset, window: int = 5, dim: str = 'dayofyear'
) -> xarray.DataArray:
    smooth = da.copy()
    for _ in range(2):
        smooth = xarray.concat(
            [
                smooth.isel(**{dim: slice(-window, None)}),
                smooth,
                smooth.isel(**{dim: slice(None, window)}),
            ],
            dim,
        )
        smooth = (
            smooth.rolling(**{dim: (window * 2 + 1)}, center=True, min_periods=1)
            .mean()
            .isel(**{dim: slice(window, -window)})
        )
    return smooth


def match_obs_to_forecasts(
    obs: xarray.DataArray | xarray.Dataset,
    forecasts: xarray.DataArray | xarray.Dataset,
    init_dim: str = 'init',
    lead_dim: str = 'lead',
) -> xarray.DataArray | xarray.Dataset:
    matching_obs = []
    for l in forecasts[lead_dim]:
        # TODO: this is hard-coded to assume monthly data
        target_times = forecasts[init_dim].to_index() + pd.DateOffset(months=l)
        try:
            match = obs.sel(time=target_times)
        except KeyError as err:
            missing_times = [t for t in target_times if t not in obs['time']]
            print('These forecast times are not in the observations:')
            print(missing_times)
            raise err
        match['time'] = forecasts['init'].values
        match['lead'] = l
        matching_obs.append(match)
    matching_obs = xarray.concat(matching_obs, dim='lead').rename({'time': 'init'})
    matching_obs = matching_obs.transpose('init', 'lead', ...)
    return matching_obs
