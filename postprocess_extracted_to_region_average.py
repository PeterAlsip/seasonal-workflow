from pathlib import Path

from loguru import logger
import xarray

if __name__ == '__main__':
    import argparse
    from yaml import safe_load

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-r', '--rerun', action='store_true')
    args = parser.parse_args()
    with open(args.config, 'r') as file:
        config = safe_load(file)

    model_output_data = Path(config['filesystem']['forecast_output_data'])
    nens = config['retrospective_forecasts']['ensemble_size']
    masks = xarray.open_dataset(config['regions']['mask_file'])
    outdir = model_output_data / 'extracted_region_average' / args.domain
    outdir.mkdir(parents=True, exist_ok=True)

    for e in range(1, nens + 1):
        # Note: this will pull in new forecasts in addition to retrospective forecasts.
        files = (model_output_data / 'extracted' / args.domain).glob(
            f'????-??-e{e:02d}.{args.domain}.nc'
        )
        for f in files:
            outname = outdir / f.name
            if not outname.exists() or args.rerun:
                logger.info(f)
                averages = []
                with xarray.open_dataset(f, decode_timedelta=False) as ds:
                    if 'xh_sub01' in ds and 'yh_sub01' in ds:
                        ds = ds.rename({'xh_sub01': 'xh', 'yh_sub01': 'yh'})
                    for reg in config['regions']['names']:
                        weights = masks['areacello'].where(masks[reg]).fillna(0)
                        ave = ds.weighted(weights).mean(['yh', 'xh']).load()
                        ave['region'] = reg
                        ave = ave.set_coords('region')
                        averages.append(ave)
                    averages = xarray.concat(averages, dim='region')
                    encoding = {
                        v: {'dtype': 'int32'}
                        for v in ['lead', 'member', 'month']
                        if v in averages
                    }
                    averages.to_netcdf(
                        outname, encoding=encoding, unlimited_dims=['init']
                    )
