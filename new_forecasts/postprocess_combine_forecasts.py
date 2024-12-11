import numpy as np
import pandas as pd
from pathlib import Path
import xarray


def process_all_vars(y, m, all_vars, config, cmdargs):
    model_output_data = Path(config['filesystem']['model_output_data'])
    members = (model_output_data / 'extracted' / cmdargs.domain).glob(f'{y}-{m:02d}-e??.{cmdargs.domain}.nc')
    model_ds = xarray.open_mfdataset(members, combine='nested', concat_dim='member')
    model_ds = model_ds.drop_vars(['ens', 'verif', 'mstart', 'ystart'], errors='ignore').squeeze().load()
    first_year = config['climatology']['first_year']
    last_year = config['climatology']['last_year']
    # todo: different method if lead has units months or days
    if model_ds['lead'].attrs['units'] == 'days':
        valid_time = [model_ds.init.values + pd.Timedelta(days=int(l)) for l in model_ds.lead]
    else:
        valid_time = [model_ds.init.values + pd.DateOffset(months=int(l)) for l in model_ds.lead]
    model_ds['valid_time'] = (('lead', ), valid_time)
    for var in all_vars:
        print(var)
        climo_file = model_output_data / f'climatology_{cmdargs.domain}_{var}_{first_year}_{last_year}.nc'
        climo_exists = False
        if climo_file.exists():
            climo = xarray.open_dataset(climo_file)
            if m in climo.month:
                climo_exists = True
                anom = model_ds[var] - climo[var].sel(month=m)
                anom.name = f'{var}_anom'
                res = xarray.merge((model_ds[[var, 'valid_time']], anom))
        if not climo_exists:
            print(f'Climatology not found for month {m}. Setting anomalies to nan')
            res = model_ds[[var, 'valid_time']].copy()
            res[f'{var}_anom'] = res[var] * np.nan
        res = res.swap_dims({'lead': 'valid_time'}).transpose('valid_time', 'member', ...)
        encoding = {v: {'dtype': 'int32'} for v in ['lead', 'member', 'month', 'valid_time'] if v in res}
        fname = f'{var}.nwa.full.ssfcast.v2024-01-1.monthly.enss.i{y}{m:02d}.nc' # TODO
        # TODO: config where to put this
        res.to_netcdf(model_output_data / 'individual' / fname, encoding=encoding)



if __name__ == '__main__':
    import argparse
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-v', '--var', type=str, default='all')
    parser.add_argument('-r','--rerun', action='store_true')
    parser.add_argument('-y', '--year', type=int, help='Only extract from this year, instead of all years in config')
    parser.add_argument('-m', '--month', type=int, help='Only extract from this month, instead of all months in config')    
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)
    # TODO: config exactly where to put these files
    (Path(config['filesystem']['model_output_data']) / 'individual').mkdir(exist_ok=True)
    if ',' in args.var:
        all_vars = args.var.split(',')
    elif args.var == 'all':
        all_vars = config['variables'][args.domain]
    else:
        all_vars = [args.var]
    process_all_vars(args.year, args.month, all_vars, config, args)

