import datetime as dt
from glob import glob
import numpy as np
from os import path
import xarray


def prepro(ds):
    ds['start'] = (('start', ), [dt.datetime(int(ds['time.year'][0]), int(ds['time.month'][0]), 1)])
    ds['lead'] = (('time', ), np.arange(len(ds['time'])))
    ds = ds.swap_dims({'time': 'lead'})
    ds = ds.rename({'time': 'valid_time'})
    # convert calendar from julian to normal gregorian
    # do it here while valid_time is 1D
    ds['valid_time'] = (['lead'], ds['valid_time'].to_index().to_datetimeindex())
    return ds


def slice_ds(ds, xslice, yslice):
    if xslice is None and yslice is None:
        return ds
    
    slice_dict = {}
    if xslice is not None:
        x = xslice.split(',')
        for xcoord in ['xh', 'xq', 'xT']:
            if xcoord in ds.coords:
                slice_dict.update({xcoord: slice(float(x[0]), float(x[1]))})
    if yslice is not None:
        y = yslice.split(',')
        for ycoord in ['yh', 'yq', 'yT']:
            if ycoord in ds.coords:
                slice_dict.update({ycoord: slice(float(y[0]), float(y[1]))})
    return ds.sel(**slice_dict)


def process_monthly(root, domain, var, ens=None, xslice=None, yslice=None):
    files = sorted(glob(path.join(root, f'{domain}.*-*.{var}.nc')))
    processed = xarray.open_mfdataset(files, preprocess=prepro, combine='nested', concat_dim='start')[var]
    processed = slice_ds(processed, xslice, yslice)

    if ens != 'pp_ensemble':
        fname = path.join(root, f'{domain}.monthly_mean.ens_{int(ens):02d}.{var}.nc')
    else:
        fname = path.join(root, f'{domain}.monthly_mean.ensmean.{var}.nc')
    processed.to_netcdf(fname)
    print(fname)


def process_daily(root, domain, var, ens=None, xslice=None, yslice=None):
    files = sorted(glob(path.join(root, f'{domain}.*-*.{var}.nc')))
    processed = xarray.open_mfdataset(files, preprocess=prepro, combine='nested', concat_dim='start')[var]
    processed = slice_ds(processed, xslice, yslice)

    if ens != 'pp_ensemble':
        fname = path.join(root, f'{domain}.daily_mean.ens_{int(ens):02d}.{var}.nc')
    else:
        fname = path.join(root, f'{domain}.daily_mean.ensmean.{var}.nc')
    processed.to_netcdf(fname)
    print(fname)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('root')
    parser.add_argument('domain')
    parser.add_argument('freq')
    parser.add_argument('var')
    parser.add_argument('ensemble', default='pp_ensemble')
    parser.add_argument('-x', '--xslice', type=str, help='Limits of x region', default='-100,-30')
    parser.add_argument('-y', '--yslice', type=str, help='initialization year', default='5,60')
    args = parser.parse_args()

    if args.freq == 'monthly':
        process_monthly(args.root, args.domain, args.var, ens=args.ensemble, xslice=args.xslice, yslice=args.yslice)
    elif args.freq == 'daily':
        process_daily(args.root, args.domain, args.var, ens=args.ensemble, xslice=args.xslice, yslice=args.yslice)
