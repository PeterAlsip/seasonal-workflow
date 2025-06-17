from concurrent import futures
from functools import partial
from pathlib import Path

import pandas as pd
import xarray
from loguru import logger

from workflow_tools.io import HSMGet
from workflow_tools.utils import run_cmd

hsmget = HSMGet(archive=Path('/archive/uda'))


# Location to save temporary data to.
TMP = hsmget.tmp


variables = {
    'mean_sea_level_pressure': 'msl',
    'surface_pressure': 'sp',
    'total_precipitation': 'tp',
    'snowfall': 'sf',
    'surface_solar_radiation_downwards': 'ssrd',
    'surface_thermal_radiation_downwards': 'strd',
    '2m_temperature': 't2m',
    '2m_dewpoint_temperature': 'd2m',
    '10m_u_component_of_wind': 'u10',
    '10m_v_component_of_wind': 'v10',
}


def thread_worker(month_file, region_slice):
    out_file = TMP / month_file.name
    # Slice to subregion and make time unlimited
    run_cmd(
        f'ncks {region_slice} --mk_rec_dmn time {month_file} -O {out_file}'
    )
    # Flip latitude so it is south to north.
    run_cmd(
        f'ncpdq -a "time,-latitude,longitude" {out_file} -O {out_file}'
    )
    return out_file


def main(year, interim_path, output_dir, lon_lat_box):
    for long_name, file_var in variables.items():
        logger.info(file_var)
        found_files = []
        for mon in range(1, 13):
            uda_file = interim_path / long_name / f'ERA5_{long_name}_{mon:02d}{year}.nc'
            if uda_file.is_file():
                found_files.append(uda_file)
            elif mon == 1:
                raise Exception('Did not find any files for this year')
            else:
                logger.info(f'Found files for month 1 to {mon - 1}')
                break

        logger.info('hsmget')
        tmp_files = hsmget(found_files)
        logger.info('add record dim')
        # These should be formatted ok if they are floats
        # (nco requires decimal point)
        region_slice = f'-d longitude,{lon_lat_box[0]},{lon_lat_box[1]} \
            -d latitude,{lon_lat_box[2]},{lon_lat_box[3]}'
        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            processed_files = sorted(
                executor.map(
                    partial(thread_worker, region_slice=region_slice), tmp_files
                )
            )

        # Join together and format metadata using xarray.
        # Using xarray partly because ncrcat is strangely slow on these files.
        logger.info('concat')
        ds = xarray.open_mfdataset(processed_files)
        # pad
        tail = ds.isel(time=-1)
        tail['time'] = tail['time'] + pd.Timedelta(hours=1)
        ds = xarray.concat((ds, tail), dim='time').transpose('time', ...)
        all_vars = list(ds.data_vars.keys()) + list(ds.coords.keys())
        encodings = {v: {'_FillValue': None, 'dtype': 'float32'} for v in all_vars}
        encodings['time'].update(
            {
                'dtype': 'float64',
                'calendar': 'gregorian',
                'units': 'hours since 1990-01-01',
            }
        )
        out_file = output_dir / f'ERA5_{file_var}_{year}_padded.nc'
        ds.to_netcdf(out_file, encoding=encodings, unlimited_dims='time')
        ds.close()


if __name__ == '__main__':
    import argparse
    from pathlib import Path

    from workflow_tools.config import load_config

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-y', '--year', type=int, required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    interim_path = config.filesystem.interim_data.ERA5
    output_dir = config.filesystem.nowcast_input_data / 'atmos'
    output_dir.mkdir(exist_ok=True)
    d = config.domain
    box = [
        float(d.west_lon) % 360,
        float(d.east_lon) % 360,
        float(d.south_lat),
        float(d.north_lat)
    ]
    main(args.year, interim_path, output_dir, box)
