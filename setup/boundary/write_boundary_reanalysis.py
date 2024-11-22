from calendar import monthrange
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

reanalysis_path = Path('/archive/uda/Global_Ocean_Physics_Reanalysis/global/daily/')
analysis_path = Path('/archive/uda/CEFI/GLOBAL_ANALYSISFORECAST_PHY_001_024/')


def find_best_files(year, mon, var):
    if var == 'uv':
        # For velocity, find the individual components separately.
        # Since u and v are in the same file for the analysis,
        # analysis files will be duplicated; remove them.
        files  = find_best_files(year, mon, 'uo')
        files += find_best_files(year, mon, 'vo')
        files = list(set(files))
    else:
        # Use reanalysis files when they are available,
        # and find and use the analysis files when not.
        files = []
        for day in range(1, monthrange(year, mon)[1]+1):
            # Search for reanalysis file for the day.
            # If there are multiple files with different R* for the day,
            # choose the last one by sorted order.
            reanalysis_file = sorted((reanalysis_path / var / str(year)).glob(f'*_{year}{mon:02d}{day:02d}_R*.nc'))
            if len(reanalysis_file) > 0:
                files.append(reanalysis_file[-1])
            else:
                # The variable naming for the analysis is complicated.
                # Assuming that the 202406 in cmems_mod_glo_phy_anfc_0.083deg_P1D-m_202406 will never change.
                if var == 'zos':
                    # /archive/uda/CEFI/GLOBAL_ANALYSISFORECAST_PHY_001_024/cmems_mod_glo_phy_anfc_0.083deg_P1D-m_202406/2024/09/glo12_rg_1d-m_20240920-20240920_2D_hcst_R20241002.nc 
                    analysis_file = sorted((analysis_path / 'cmems_mod_glo_phy_anfc_0.083deg_P1D-m_202406' / str(year) / f'{mon:02d}').glob(f'glo12_rg_1d-m_{year}{mon:02d}{day:02d}-{year}{mon:02d}{day:02d}_2D_hcst_R*.nc'))
                elif var in ['thetao', 'so']:
                    analysis_file = sorted((analysis_path / f'cmems_mod_glo_phy-{var}_anfc_0.083deg_P1D-m_202406' / str(year) / f'{mon:02d}').glob(f'glo12_rg_1d-m_{year}{mon:02d}{day:02d}-{year}{mon:02d}{day:02d}_3D-{var}_hcst_R*.nc'))
                elif var in ['uo', 'vo']:
                    # /archive/uda/CEFI/GLOBAL_ANALYSISFORECAST_PHY_001_024/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_202406/2024/09/glo12_rg_1d-m_20240920-20240920_3D-uovo_hcst_R20241002.nc
                    analysis_file = sorted((analysis_path / 'cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_202406' / str(year) / f'{mon:02d}').glob(f'glo12_rg_1d-m_{year}{mon:02d}{day:02d}-{year}{mon:02d}{day:02d}_3D-uovo_hcst_R*.nc'))
                else:
                    raise Exception('Unknown variable')
                if len(analysis_file) > 0:
                    files.append(analysis_file[-1])
                else:
                    print(f'Did not a find a file for {year}-{mon:02d}-{day:02d} {var}')
    return files


def thread_worker(in_file, out_dir):
    out_file = out_dir / in_file.name
    # run_cmd(f'ncks -d latitude,{float(latmin)},{float(latmax)} -d longitude,{float(lonmin)},{float(lonmax)} {in_file.as_posix()} -O {out_file.as_posix()}')
    run_cmd(f'cdo setmisstonn -sellevidx,1/49 -sellonlatbox,{float(lonmin)},{float(lonmax)},{float(latmin)},{float(latmax)} {in_file.as_posix()} {out_file.as_posix()}')
    # out_file.with_suffix('.tmp').rename(out_file)
    return out_file


def main(year, mon, var, threads, dry=False):
    if mon == 'all':
        for m in range(1, 13):
            print(m)
            main(year, m, var, threads, dry=dry)
    else:
        mon = int(mon)
        if var == 'all':
            for v in ['uv', 'so', 'thetao', 'zos']:
                main(year, mon, v, threads, dry=dry)
        else:
            print(var)
            files = find_best_files(year, mon, var)
            if dry:
                print(f'Found {len(files)} files')
                for f in files:
                    print(f.as_posix())
            else:
                copied_files = hsmget(files)

                with futures.ThreadPoolExecutor(max_workers=threads) as executor:
                    processed_files = sorted(executor.map(partial(thread_worker, out_dir=TMP), copied_files))

                # Save data for use with sponge.
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
    parser.add_argument('-v', '--var', default='all')
    parser.add_argument('-t', '--threads', type=int, default=4)
    parser.add_argument('-D','--dry', action='store_true')
    args = parser.parse_args()
    main(args.year, args.month, args.var, args.threads, dry=args.dry)
