from argparse import ArgumentParser, Namespace
import concurrent.futures
import numpy as np
import os
from pathlib import Path
from subprocess import run, CompletedProcess
from typing import Any
import xarray
from utils import smooth_climatology


def run_nco(nco_tool: str, var: str, in_files: str, out_file: Path) -> CompletedProcess:
    cmd = f'{nco_tool} -v {var} -h {in_files} -O {out_file}'
    print(cmd)
    return run(cmd, shell=True, check=True)


def process_var(var: str, config: Any, cmdargs: Namespace) -> None:
    first_year = config['climatology']['first_year']
    last_year = config['climatology']['last_year']
    nens = config['retrospective_forecasts']['ensemble_size']
    tmp = Path(os.environ['TMPDIR'])
    model_output_data = Path(config['filesystem']['forecast_output_data'])
    threads = cmdargs.threads
    members = []
    futures = []

    if cmdargs.mean:
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        # Large files: ensemble average, then concatenate averages
            for m in config['retrospective_forecasts']['months']:
                for y in range(config['retrospective_forecasts']['first_year'], config['retrospective_forecasts']['last_year']+1):
                    month_file = tmp / f'{cmdargs.domain}_{var}_{y}_{m:02d}_ensmean.nc'
                    files = list((model_output_data / 'extracted' / cmdargs.domain).glob(f'{y}-{m:02d}-e??.{cmdargs.domain}.nc'))
                    if len(files) == 1: # single ensemble member
                        futures.append(executor.submit(run_nco, 'ncks', var, str(files[0]), month_file))
                        members.append(month_file)
                    elif len(files) > 1: 
                        file_str = ' '.join(map(lambda x: x.as_posix(), files))
                        futures.append(executor.submit(run_nco, 'ncea', var, file_str, month_file))
                        members.append(month_file)  
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            # Regular files: concatenate initializations together
            for e in range(1, nens+1):
                out_file = tmp / f'{cmdargs.domain}_{var}_e{e:02d}.nc'
                if not out_file.exists() or cmdargs.rerun:
                    files = []
                    for y in range(config['retrospective_forecasts']['first_year'], config['retrospective_forecasts']['last_year']+1):
                        for m in config['retrospective_forecasts']['months']:
                            tentative = model_output_data / 'extracted' / cmdargs.domain / f'{y}-{m:02d}-e{e:02d}.{cmdargs.domain}.nc'
                            if tentative.is_file():
                                files.append(tentative)
                    if len(files) > 0:
                        file_str = ' '.join(map(lambda x: x.as_posix(), files))
                        futures.append(executor.submit(run_nco, 'ncrcat', f'{var},member', file_str, out_file))
                        # futures.append(executor.submit(run(f'ncrcat -v {var},member -h {file_str} -O {out_file}', shell=True, check=True)))
                members.append(out_file)

    for future in futures:
        try:
            _ = future.result()
        except Exception as e:
            print(f"Task generated an exception: {e}")

    concat_dim = 'init' if cmdargs.mean else 'member'
    print(f'Concat by {concat_dim}')
    model_ds = xarray.open_mfdataset(members, combine='nested', concat_dim=concat_dim, decode_timedelta=False).sortby('init') # sorting is important for slicing later
    model_ds = model_ds.drop_vars(['ens', 'verif', 'mstart', 'ystart'], errors='ignore').load()
    model_ds['lead'] = np.arange(len(model_ds['lead']))
    print('Ensemble mean and anomalies')
    if cmdargs.mean:
        ensmean = model_ds
    else:
        ensmean = model_ds.mean('member')
    climo = ensmean[var].sel(init=slice(f'{first_year}-01-01', f'{last_year}-12-31')).groupby('init.month').mean('init')
    if 'daily' in cmdargs.domain or len(model_ds.lead) >= 365:
        print('Smoothing daily climatology')
        climo = smooth_climatology(climo, dim='lead')
    anom = model_ds.groupby('init.month') - climo
    anom = anom.rename({v: f'{v}_anom' for v in anom.data_vars})
    model_ds = xarray.merge([model_ds, anom])
    # Write the climatology, being sure that appropriate coords are ints.
    # Also trying to remove the empty dimension "time" from the output.
    encoding = {v: {'dtype': 'int32'} for v in ['month']}
    climo.encoding = {}
    print('Writing climatology')
    climo.to_netcdf(model_output_data / f'climatology_{cmdargs.domain}_{var}_{first_year}_{last_year}.nc',
        encoding=encoding)
    # Do the same for the full set of forecasts
    encoding = {v: {'dtype': 'int32'} for v in ['member', 'month'] if v in model_ds}
    encoding.update({var: dict(zlib=True, complevel=3) for var in model_ds.data_vars})
    print('Writing forecasts')
    fname = f'forecasts_{cmdargs.domain}_{var}_ensmean.nc' if cmdargs.mean else f'forecasts_{cmdargs.domain}_{var}.nc'
    model_ds.to_netcdf(model_output_data / fname, encoding=encoding)


if __name__ == '__main__':
    from yaml import safe_load
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-v', '--var', type=str, required=True)
    parser.add_argument('-r', '--rerun', action='store_true')
    parser.add_argument('-m', '--mean', action='store_true', help='Include only ensemble mean in combined result, dropping individual members.')
    parser.add_argument('-t', '--threads', type=int, default=1)
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)
    if ',' in args.var:
        cmdvar = args.var.split(',')
        for v in cmdvar:
            process_var(v, config, args)
    else:
        process_var(args.var, config, args)

