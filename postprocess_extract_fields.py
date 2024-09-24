from dataclasses import dataclass
import datetime as dt
from getpass import getuser
import numpy as np
from pathlib import Path
import subprocess
import xarray


# Expect, and only extract, these variables from the given domain
# (TODO: this could probably go in config)
_DOMAIN_VARIABLES = {
    'ocean_month': ['tos', 'tob', 'sos', 'sob', 'MLD_003', 'ssh', 'ustar'], # ssh or zos
    'ocean_daily': ['tos', 'tob', 'ssh', 'ssh_max'],
    'ocean_cobalt_btm': ['btm_o2', 'btm_co3_sol_arag', 'btm_co3_ion', 'btm_htotal'],
    'ocean_cobalt_omip_sfc': ['chlos', 'no3os', 'phos'],
    'ocean_cobalt_neus': ['chlos', 'no3os', 'po4os', 'zmesoos', 
                          'nsmp_100', 'nmdp_100', 'nlgp_100', 
                          'sfc_no3lim_smp', 'sfc_no3lim_mdp', 'sfc_no3lim_lgp', 
                          'sfc_irrlim_lgp', 'sfc_irrlim_mdp', 'sfc_irrlim_smp'],
    'ocean_neus': ['MLD_003', 'ustar'],
    'ocean_cobalt_daily_2d': ['chlos', 'btm_o2', 'btm_co3_sol_arag', 'btm_co3_ion', 'btm_htotal']
}

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
    
    def run_cmd(self, cmd):
        print(cmd)
        if not self.dry_run:
            subprocess.run([cmd], shell=True, check=True)

    def copy_from_archive(self):
        """
        Extract the file for this domain, from the tar file on archive, to the path on /ptmp.
        """
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

    def process_file(self, outfile, infile=None):
        if infile is None:
            infile = self.vftmp_dir / self.file_name
        print(f'process_file({infile})')
        if not self.dry_run:
            ds = xarray.open_dataset(infile)[_DOMAIN_VARIABLES[self.domain]]
            ds['member'] = int(self.ens)
            ds['init'] = dt.datetime(int(self.ystart), int(self.mstart), 1)   
            ds['lead'] = (('time', ), np.arange(len(ds['time'])))
            ds['lead'].attrs['units'] = 'months'
            ds = ds.swap_dims({'time': 'lead'}).set_coords(['init', 'member'])
            ds = ds.expand_dims('init')
            ds = ds.transpose(*(['init', 'lead'] + [d for d in ds.dims if d not in ['init', 'lead']]))
            ds = ds.drop_vars('time')
            ds.to_netcdf(outfile, unlimited_dims='init')
            ds.close()


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-r','--rerun', action='store_true')
    parser.add_argument('-D','--dry', action='store_true')
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)

    first_year = config['forecasts']['first_year']
    last_year = config['forecasts']['last_year']
    nens = config['forecasts']['ensemble_size']
    outdir = Path(config['filesystem']['model_output_data']) / 'extracted' / args.domain
    outdir.mkdir(exist_ok=True, parents=True)

    files_to_dmget = []

    for ystart in range(first_year, last_year+1):
        for mstart in config['forecasts']['months']:
            for ens in range(1, nens+1):
                run = ForecastRun(
                    ystart=ystart, 
                    mstart=mstart, 
                    ens=ens,
                    name=config['name'],
                    template=config['filesystem']['forecast_history'],
                    domain=args.domain,
                    dry_run=args.dry
                )
                # Check if a processed file exists
                if not (outdir / run.out_name).is_file() or args.rerun:
                    # Check if an extracted data file exists
                    if not (run.vftmp_dir / run.file_name).is_file() and not (run.ptmp_dir / run.file_name).is_file():
                        # Check if the raw data exists in archive
                        if (run.archive_dir / run.tar_file).is_file():
                            files_to_dmget.append((run.archive_dir / run.tar_file).as_posix())
                        else:
                            print(f'{(run.archive_dir / run.tar_file).as_posix()} not found in archive')

    if len(files_to_dmget) > 0:
        print(f'dmgetting {len(files_to_dmget)} files')
        if not args.dry:
            subprocess.run([f'dmget {" ".join(files_to_dmget)}'], shell=True, check=True)
    else:
        print('No files to dmget')

    for ystart in range(first_year, last_year+1):
        for mstart in config['forecasts']['months']:
            for ens in range(1, nens+1):
                run = ForecastRun(
                    ystart=ystart, 
                    mstart=mstart, 
                    ens=ens,
                    name=config['name'],
                    template=config['filesystem']['forecast_history'],
                    domain=args.domain,
                    dry_run=args.dry
                )
                outfile = outdir / run.out_name
                # Check if a processed file exists
                if not outfile.is_file() or args.rerun:
                    # Check if an extracted data file exists
                    if (run.vftmp_dir / run.file_name).is_file():
                        run.process_file(outfile)
                    # Check if a cached tar file exists
                    elif (run.ptmp_dir / run.file_name).is_file():
                        run.copy_from_ptmp()
                        run.process_file(outfile)
                    else:
                        # Check if the raw data exists in archive
                        if (run.archive_dir / run.tar_file).is_file():
                            run.copy_from_archive()
                            run.copy_from_ptmp()
                            run.process_file(outfile)
                        else:
                            print(f'{(run.archive_dir / run.tar_file).as_posix()} not found in archive')
