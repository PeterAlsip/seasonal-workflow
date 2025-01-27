# Can also do something like:
# sbatch --export=ALL --wrap="python postprocess_extract_fields.py -c config_nwa12_physics.yaml -d ocean_daily -y 2019 -m 3"

from dataclasses import dataclass
import datetime as dt
from getpass import getuser
import numpy as np
from pathlib import Path
import subprocess
import xarray


# Using ptmp to cache full history files
PTMP = Path('/ptmp') / getuser()

# Using /vftmp/$USER as a cache for nc files extracted from tar files.
# This means this script must be run on analysis,
# and ideally it should always be run on the 
# same analysis node because each node has a 
# different /vftmp.
VFTMP = Path('/vftmp') / getuser()


@dataclass
class ForecastRun:
    ystart: int
    mstart: int
    ens: int
    template: str
    outdir: Path
    name: str = ''
    domain: str = 'ocean_month'
    dry_run: bool = False

    @property
    def archive_dir(self):
        """
        Using a string template for the name of a single forecast's
        history directory on archive, and format it with the current forecast's
        year, month, and ensemble member.
        """
        return Path(self.template.format(year=self.ystart, month=self.mstart, ensemble=self.ens))

    @property
    def tar_file(self):
        """
        Name of the tar file stored on archive.
        """
        return f'{self.ystart}{self.mstart:02d}01.nc.tar'

    @property
    def ptmp_dir(self):
        """
        Location on /ptmp to cache data. This is intended to be the same path used by frepp
        so that it can take advantage of the frepp cache.
        """
        return PTMP / self.archive_dir.relative_to(self.archive_dir.root) / f'{self.ystart}{self.mstart:02d}01.nc'
    
    @property 
    def vftmp_dir(self):
        """
        Location on vftmp to cache extracted data.
        """
        return VFTMP / 'forecast_data' / self.name / f'e{self.ens:02d}'

    @property
    def file_name(self):
        """
        Name of the file in the tar file to extract.
        """
        return f'{self.ystart}{self.mstart:02d}01.{self.domain}.nc'

    @property
    def out_name(self):
        """
        Name to give the final processed file.
        """
        return f'{self.ystart}-{self.mstart:02d}-e{self.ens:02d}.{self.domain}.nc'
    
    @property
    def exists(self):
         return (run.archive_dir / run.tar_file).is_file()
    
    @property
    def needs_dmget(self):
        return self.exists and not (run.vftmp_dir / run.file_name).is_file() and not (run.ptmp_dir / run.file_name).is_file()
    
    def run_cmd(self, cmd):
        print(cmd)
        if not self.dry_run:
            subprocess.run([cmd], shell=True, check=True)

    def copy_from_archive(self):
        """
        Extract the file for this domain, from the tar file on archive, to the path on /ptmp.
        """
        if not self.exists:
            raise FileNotFoundError(f'File {(self.archive_dir / self.tar_file)} does not exist.')
        self.ptmp_dir.mkdir(parents=True, exist_ok=True)
        cmd = f'tar xf {(self.archive_dir / self.tar_file).as_posix()} -C {self.ptmp_dir.as_posix()} ./{self.file_name}'
        self.run_cmd(cmd)
    
    def copy_from_ptmp(self):
        """
        Copy the file for this domain from ptmp to vftmp.
        """
        self.vftmp_dir.mkdir(parents=True, exist_ok=True)
        cmd = f'gcp {(self.ptmp_dir / self.file_name).as_posix()} {self.vftmp_dir.as_posix()}'
        self.run_cmd(cmd)

    def process_file(self, variables=None, infile=None, outfile=None):
        if infile is None:
            infile = self.vftmp_dir / self.file_name
        if outfile is None:
            outfile = self.outdir / self.out_name
        print(f'process_file({infile})')
        if not self.dry_run:
            with xarray.open_dataset(infile) as ds:
                if variables is None:
                    variables = list(ds.data_vars)
                ds = ds[variables]
                ds['member'] = int(self.ens)
                ds['init'] = dt.datetime(int(self.ystart), int(self.mstart), 1)   
                ds['lead'] = (('time', ), np.arange(len(ds['time'])))
                if 'daily' in self.domain or len(ds['lead']) > 12:
                    ds['lead'].attrs['units'] = 'days'
                else:
                    ds['lead'].attrs['units'] = 'months'
                ds = ds.swap_dims({'time': 'lead'}).set_coords(['init', 'member'])
                ds = ds.expand_dims('init')
                ds = ds.transpose('init', 'lead', ...)
                ds = ds.drop_vars('time')
                # Compress output to significantly reduce space
                encoding = {var: dict(zlib=True, complevel=3) for var in variables}
                ds.to_netcdf(outfile, unlimited_dims='init', encoding=encoding)


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-r','--rerun', action='store_true')
    parser.add_argument('-n','--new', action='store_true')
    parser.add_argument('-D','--dry', action='store_true')
    parser.add_argument('-y', '--year', type=int, help='Only extract from this year, instead of all years in config')
    parser.add_argument('-m', '--month', type=int, help='Only extract from this month, instead of all months in config')
    args = parser.parse_args()
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
    all_runs = [
        ForecastRun(
            ystart=ystart, 
            mstart=mstart, 
            ens=ens,
            name=config['name'],
            template=config['filesystem']['forecast_history'],
            domain=args.domain,
            dry_run=args.dry,
            outdir=outdir
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
        if not args.dry:
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

    for run in all_runs:
        # Check if a processed file exists
        if not (run.outdir / run.out_name).is_file() or args.rerun:
            # Check if an extracted data file exists
            if (run.vftmp_dir / run.file_name).is_file():
                run.process_file(variables=variables)
            # Check if a cached tar file exists
            elif (run.ptmp_dir / run.file_name).is_file():
                run.copy_from_ptmp()
                run.process_file(variables=variables)
            else:
                run.copy_from_archive()
                run.copy_from_ptmp()
                run.process_file(variables=variables)

