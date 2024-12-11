from pathlib import Path
import xarray


def process_var(var, config, cmdargs):
    model_output_data = Path(config['filesystem']['forecast_output_data'])

    fname = f'forecasts_{cmdargs.domain}_{var}_ensmean.nc' if cmdargs.mean else f'forecasts_{cmdargs.domain}_{var}.nc'
    ds = xarray.open_dataset(model_output_data / fname)

    masks = xarray.open_dataset(config['regions']['mask_file'])
    if 'yh_sub01' in ds and 'xh_sub01' in ds:
        ds = ds.rename({f'{v}_sub01': v for v in ['yh', 'xh']})

    averages = []
    for reg in config['regions']['names']:
        print(reg)
        weights = masks['areacello'].where(masks[reg]).fillna(0)
        ave = ds.weighted(weights).mean(['yh', 'xh']).load()
        ave['region'] = reg
        ave = ave.set_coords('region')
        averages.append(ave)

    averages = xarray.concat(averages, dim='region')
    outname = f'forecasts_{cmdargs.domain}_{var}_ensmean_regionmean.nc' if cmdargs.mean else f'forecasts_{cmdargs.domain}_{var}_regionmean.nc'
    averages.to_netcdf(model_output_data / outname)

if __name__ == '__main__':
    import argparse
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-v', '--var', type=str, required=True)
    parser.add_argument('-m', '--mean', action='store_true', help='Include only ensemble mean in combined result, dropping individual members.')
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)
    if ',' in args.var:
        cmdvar = args.var.split(',')
        for v in cmdvar:
            process_var(v, config, args)
    else:
        process_var(args.var, config, args)
