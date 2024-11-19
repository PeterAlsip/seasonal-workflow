import concurrent.futures as futures
from functools import partial
from getpass import getuser
from os import environ
from pathlib import Path
import xarray
from subprocess import run, DEVNULL
import sys
sys.path.append('../..')
from utils import HSMGet
from boundary import Segment



def run_cmd(cmd):
    run([cmd], shell=True, check=True, stdout=DEVNULL, stderr=DEVNULL)

hsmget = HSMGet(archive=Path('/archive/uda'), ptmp=Path('/ptmp')/getuser())
TMP = Path(environ['TMPDIR'])

# temporarily hardcoded config
latmin = 5
latmax = 60
lonmin = -100
lonmax = -30
output_dir = '/work/acr/mom6/nwa12/analysis_input_data/boundary/monthly'
hgrid = xarray.open_dataset('../../../nwa12/setup/grid/ocean_hgrid.nc')
segments = [
    Segment(1, 'south', hgrid, output_dir=output_dir),
    Segment(2, 'north', hgrid, output_dir=output_dir),
    Segment(3, 'east', hgrid, output_dir=output_dir)
]

def thread_worker(in_file, out_dir):
    out_file = out_dir / in_file.name
    # run_cmd(f'ncks -d latitude,{float(latmin)},{float(latmax)} -d longitude,{float(lonmin)},{float(lonmax)} {in_file.as_posix()} -O {out_file.as_posix()}')
    run_cmd(f'cdo setmisstonn -sellevidx,1/49 -sellonlatbox,{float(lonmin)},{float(lonmax)},{float(latmin)},{float(latmax)} {in_file.as_posix()} {out_file.as_posix()}')
    # out_file.with_suffix('.tmp').rename(out_file)
    return out_file


def main(year, mon, threads):
    if mon == 'all':
        for m in range(1, 13):
            print(m)
            main(year, m, threads)
    else:
        mon = int(mon)
        for var in ['uv', 'so', 'thetao', 'zos']:
            print(f'    {var}')
            if var == 'uv':
                files  = list(((Path('/archive/uda/Global_Ocean_Physics_Reanalysis/global/daily/') / 'uo' / str(year)).glob(f'*_{year}{mon:02d}??_R*.nc')))
                files += list(((Path('/archive/uda/Global_Ocean_Physics_Reanalysis/global/daily/') / 'vo' / str(year)).glob(f'*_{year}{mon:02d}??_R*.nc')))
            else:
                files = list(((Path('/archive/uda/Global_Ocean_Physics_Reanalysis/global/daily/') / var / str(year)).glob(f'*_{year}{mon:02d}??_R*.nc')))
            copied_files = hsmget(files)

            with futures.ThreadPoolExecutor(max_workers=threads) as executor:
                processed_files = sorted(executor.map(partial(thread_worker, out_dir=TMP), copied_files))

            if var in ['so', 'thetao']:
                run_cmd(f'cdo timavg  -cat {" ".join(map(lambda x: x.as_posix(), processed_files))} /work/acr/mom6/nwa12/analysis_input_data/sponge/monthly_filled/glorys_{var}_{year}-{mon:02d}.nc')
            ds = (
                xarray.open_mfdataset(processed_files)
                .rename({'latitude': 'lat', 'longitude': 'lon'})
            )
            if 'depth' in ds.coords:
                ds = ds.rename({'depth': 'z'})
            for seg in segments:
                if var == 'uv':
                    seg.regrid_velocity(ds['uo'], ds['vo'], suffix=f'{year}-{mon:02d}', flood=False)
                else:
                    seg.regrid_tracer(ds[var], suffix=f'{year}-{mon:02d}', flood=False)
            for f in processed_files:
                f.unlink()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year', type=int, required=True)
    parser.add_argument('-m', '--month', default='all')
    parser.add_argument('-t', '--threads', type=int, default=4)
    args = parser.parse_args()
    main(args.year, args.month, args.threads)
