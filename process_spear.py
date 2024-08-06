import datetime as dt
from glob import glob
import numpy as np
import os
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
        for xcoord in ['xh', 'xq', 'xT', 'lon']:
            if xcoord in ds.coords:
                if ds[xcoord].max() > 180:
                    # If data longitude is 0--360, convert lon slice from config to 0--360
                    slice_dict.update({xcoord: slice(np.mod(float(xslice[0]), 360), np.mod(float(xslice[1]), 360))})                
                else:
                    slice_dict.update({xcoord: slice(float(xslice[0]), float(xslice[1]))})
    if yslice is not None:
        for ycoord in ['yh', 'yq', 'yT', 'lat']:
            if ycoord in ds.coords:
                slice_dict.update({ycoord: slice(float(yslice[0]), float(yslice[1]))})
    return ds.sel(**slice_dict)


def process_spear(root, domain, freq, var, ens=None, xslice=None, yslice=None):
    files = sorted(glob(os.path.join(root, f'{domain}.*-*.{var}.nc')))
    processed = xarray.open_mfdataset(files, preprocess=prepro, combine='nested', concat_dim='start', chunks=None, parallel=False)[var]
    processed = slice_ds(processed, xslice, yslice)

    if ens != 'pp_ensemble':
        fname = os.path.join(root, f'{domain}.{freq}_mean.ens_{int(ens):02d}.{var}.nc')
    else:
        fname = os.path.join(root, f'{domain}.{freq}_mean.ensmean.{var}.nc')
    processed.to_netcdf(fname)
    print(fname)


if __name__ == '__main__':
    import argparse
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--root', default=os.environ['TMPDIR'])
    parser.add_argument('-d', '--domain', required=True)
    parser.add_argument('-f', '--freq', required=True)
    parser.add_argument('-v', '--var', required=True)
    parser.add_argument('-e', '--ensemble', required=True)
    parser.add_argument('-c', '--config', default=None)
    args = parser.parse_args()

    if args.config is not None:
        with open(args.config, 'r') as file: 
            config = safe_load(file)
            xslice = (config['domain']['west_lon'], config['domain']['east_lon'])
            yslice = (config['domain']['south_lat'], config['domain']['north_lat'])
    else:
        xslice = yslice = None

    process_spear(args.root, args.domain, args.freq, args.var, ens=args.ensemble, xslice=xslice, yslice=yslice)
