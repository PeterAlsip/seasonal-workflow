# Can also do something like:
# sbatch --export=ALL --wrap="python postprocess_extract_fields.py -c config_nwa12_physics.yaml -d ocean_daily -y 2019 -m 3"
import concurrent.futures as futures
import datetime as dt
import numpy as np
from pathlib import Path
import subprocess
import xarray

from forecast_lib import ForecastRun


def process_file(forecast: ForecastRun, variables: list[str] | None = None, infile: Path | str | None = None, outfile: Path | str | None = None) -> None:
    if infile is None:
        infile = forecast.vftmp_dir / forecast.file_name
    if outfile is None:
        outfile = forecast.outdir / forecast.out_name
    print(f'process_file({infile})')
    with xarray.open_dataset(infile, decode_timedelta=False) as ds:
        if variables is None:
            variables = list(ds.data_vars)
        ds = ds[variables]
        ds['member'] = int(forecast.ens)
        ds['init'] = dt.datetime(int(forecast.ystart), int(forecast.mstart), 1)   
        ds['lead'] = (('time', ), np.arange(len(ds['time'])))
        if 'daily' in forecast.domain or len(ds['lead']) > 12:
            ds['lead'].attrs['units'] = 'days'
        else:
            ds['lead'].attrs['units'] = 'months'
        ds = ds.swap_dims({'time': 'lead'}).set_coords(['init', 'member'])
        ds = ds.expand_dims('init')
        ds = ds.transpose('init', 'lead', ...)
        ds = ds.drop_vars('time')
        ds.attrs[f'cefi_archive_version_ens{forecast.ens:02d}'] = str(forecast.archive_dir.parent)
        # Compress output to significantly reduce space
        encoding = {var: dict(zlib=True, complevel=3) for var in variables}
        ds.to_netcdf(outfile, unlimited_dims='init', encoding=encoding)


def process_run(forecast: ForecastRun, variables: list[str], rerun: bool = False, clean: bool = False) -> None:
    # Check if a processed file exists
    if not (forecast.outdir / forecast.out_name).is_file() or rerun:
        # Check if an extracted data file exists
        if (forecast.vftmp_dir / forecast.file_name).is_file():
            process_file(forecast, variables=variables)
        # Check if a cached tar file exists
        elif (forecast.ptmp_dir / forecast.file_name).is_file():
            forecast.copy_from_ptmp()
            process_file(forecast, variables=variables)
        elif forecast.exists:
            forecast.copy_from_archive()
            forecast.copy_from_ptmp()
            process_file(forecast, variables=variables)
        else:
            print(f'{forecast.archive_dir/forecast.tar_file} not found; skipping.')
            return
        if clean:
            (forecast.vftmp_dir / forecast.file_name).unlink()


def main(args):
    with open(args.config, 'r') as file: 
        config = safe_load(file)
    if args.new:
        nens = config['new_forecasts']['ensemble_size']
        if args.year is None or args.month is None:
            raise Exception('Must provide year and month for the new forecast to extract')
        first_year = last_year = args.year
        months = [args.month]
    else:
        first_year = args.year if args.year is not None else config['retrospective_forecasts']['first_year']
        last_year = args.year if args.year is not None else config['retrospective_forecasts']['last_year']
        months = [args.month] if args.month is not None else config['retrospective_forecasts']['months']
        nens = config['retrospective_forecasts']['ensemble_size']
    outdir = Path(config['filesystem']['forecast_output_data']) / 'extracted' / args.domain
    outdir.mkdir(exist_ok=True, parents=True)
    variables = config['variables'][args.domain]
    if args.tmp:
        from os import environ
        vftmp = Path(environ['TMPDIR'])
    else:
        from getpass import getuser
        vftmp = Path('/vftmp') / getuser()
    all_runs = [
        ForecastRun(
            ystart=ystart, 
            mstart=mstart, 
            ens=ens,
            name=config['name'],
            template=config['filesystem']['forecast_history'],
            domain=args.domain,
            outdir=outdir,
            vftmp=vftmp
        )
        for ystart in range(first_year, last_year+1) 
            for mstart in months 
                for ens in range(1, nens+1)
    ]
    # Prefer to dmget all files that need it in one command, if possible.
    runs_to_dmget = []
    for run in all_runs:
        if not (run.outdir / run.out_name).is_file() or args.rerun:
            if run.needs_dmget:
                runs_to_dmget.append(run)#(run.archive_dir / run.tar_file))

    # Try running one dmget command for all files.
    if len(runs_to_dmget) > 0:
        print(f'dmgetting {len(runs_to_dmget)} files')
        file_names = [str(run.archive_dir / run.tar_file) for run in runs_to_dmget]
        dmget = subprocess.run([f'dmget {" ".join(file_names)}'], shell=True, capture_output=True, universal_newlines=True)
        # If a tape is bad, the single dmget will fail.
        # Try running dmget separately for each individual file.
        # If the dmget fails, remove the run from the all_runs list
        # so that it is not extracted or worked on later. 
        if dmget.returncode > 0:
            if 'unable to recall the requested file' in dmget.stderr:
                print('dmget failed. Running dmget separately for each file.')
                for run in runs_to_dmget:
                    try:
                        subprocess.run([f'dmget {run.archive_dir / run.tar_file}'], shell=True, check=True)
                    except subprocess.CalledProcessError:
                        print(f'Could not dmget {run.archive_dir / run.tar_file}. Removing from list of files to extract.')
                        all_runs.remove(run)
            else:
                # dmget failed, but not with the usual error associated with a bad file/tape.
                raise subprocess.CalledProcessError(dmget.returncode, str(dmget.args), output=dmget.stdout, stderr=dmget.stderr)
    else:
        print('No files to dmget')

    with futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        executor.map(lambda x: x.process_run(variables, rerun=args.rerun, clean=args.tmp), all_runs)


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-t', '--threads', type=int, default=2)
    parser.add_argument('-y', '--year', type=int, 
        help='Only extract from this year, instead of all years in config')
    parser.add_argument('-m', '--month', type=int, 
        help='Only extract from this month, instead of all months in config')
    parser.add_argument('-r','--rerun', action='store_true')
    parser.add_argument('-n','--new', action='store_true',
        help='Flag if this is a new near real time forecast instead of a retrospective.')
    parser.add_argument('--tmp', action='store_true', 
        help='Store data in $TMPDIR instead of top level /vftmp/$USER')
    args = parser.parse_args()
    main(args)