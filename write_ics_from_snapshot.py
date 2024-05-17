import datetime as dt
import subprocess
import os
from pathlib import Path
import tarfile
import xarray


# Path to store temporary output to:
TMP = Path(os.environ['TMPDIR'])

# Drop these variables from the snapshots:
DROP_VARS = ['mass_wt', 'opottempmint', 'somint', 'uice', 'vice']


def run_cmd(cmd, print_cmd=True):
    if print_cmd:
        print(cmd)
    subprocess.run([cmd], shell=True, check=True) 


def ics_from_snapshot(component, history, ystart, mstart):
    target_time = f'{ystart}-{mstart:02d}-01'
    if mstart == 1:
        yfile = ystart - 1
    else:
        yfile = ystart 

    snapshot_file = history / f'{yfile}0101.nc.tar'

    # extract the snapshot from the tar file to tmp
    if not (TMP / f'./{yfile}0101.{component}_snap.nc').exists():
        # dmget the tar file
        run_cmd(f'dmget {snapshot_file.as_posix()}')
        print('extracting')
        tar = tarfile.open(snapshot_file, mode='r:')
        member = tar.getmember(f'./{yfile}0101.{component}_snap.nc')
        tar.extractall(path=TMP, members=[member])

    # open and modify the tmp snapshot file
    print('modifying')
    ds = xarray.open_dataset(TMP / f'./{yfile}0101.{component}_snap.nc', decode_cf=False)
    ds['time'].attrs['calendar'] = 'gregorian'
    ds = xarray.decode_cf(ds)
    ds = ds.drop_vars(DROP_VARS, errors='ignore')
    if 'uo' in ds and 'vo' in ds:
        ds = ds.rename({'uo': 'u', 'vo': 'v'})
    snapshot = ds.sel(time=target_time)
    snapshot['time'] = dt.datetime(ystart, mstart, 1)
    snapshot = snapshot.expand_dims(time=1)
    snapshot = snapshot.fillna(0.0)
    all_vars = list(snapshot.data_vars.keys()) + list(snapshot.coords.keys())
    encodings = {v: {'_FillValue': None, 'dtype': 'float32'} for v in all_vars}
    snapshot.attrs['source'] = snapshot_file.as_posix()

    # write the results to tmp
    print('writing')
    output_name = f'forecast_ics_{component}_{ystart}-{mstart:02d}.nc'
    tmp_output = TMP / output_name
    snapshot.to_netcdf(
        tmp_output,
        format='NETCDF3_64BIT',
        engine='netcdf4',
        encoding=encodings,
        unlimited_dims='time'
    )
    return tmp_output


def main(config, cmdargs):
    history = Path(config['filesystem']['analysis_history'])
    outdir = Path(config['filesystem']['model_input_data']) / 'initial'
    outdir.mkdir(exist_ok=True)
    tmp_files = [ics_from_snapshot(c, history, cmdargs.year, cmdargs.month) for c in config['snapshots']]
    file_str = ' '.join(map(lambda x: x.name, tmp_files))
    cmd = f'tar cvf {outdir.as_posix()}/forecast_ics_{cmdargs.year}-{cmdargs.month:02d}.tar -C {TMP} {file_str}'
    run_cmd(cmd)
    for f in tmp_files:
        f.unlink()


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year', type=int)
    parser.add_argument('-m', '--month', type=int)
    parser.add_argument('-c', '--config')
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)
    main(config, args)
    