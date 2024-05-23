from pathlib import Path
import xarray


def process_file(f):
    ds = xarray.open_dataset(f)
    # set_coords is needed to avoid later warnings from
    # concat with newer xarray
    return ds.swap_dims({'time': 'lead'}).set_coords('init')


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from yaml import safe_load
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    args = parser.parse_args()
    with open(args.config, 'r') as file: 
        config = safe_load(file)
    
    model_output_data = Path(config['filesystem']['model_output_data'])
    model_output_data.mkdir(exist_ok=True)
    first_year = config['climatology']['first_year']
    last_year = config['climatology']['last_year']
    nens = config['forecasts']['ensemble_size']

    members = []
    for e in range(1, nens+1):
        print(f'Loading member {e:02d}')
        files = (model_output_data / 'extracted' / args.domain).glob(f'????-??-e{e:02d}.{args.domain}.nc')
        member = xarray.concat((process_file(f) for f in files), dim='init').load()
        member['lead'].attrs['units'] = 'months'
        member = member.rename({'time': 'verif'})
        member['member'] = e
        member = member.set_coords('member')
        members.append(member)

    print('Concat')
    model_ds = xarray.concat(members, dim='member').sortby('init') # sorting is important for slicing later
    model_ds = model_ds.drop_vars(['ens', 'verif', 'mstart', 'ystart'], errors='ignore')
    print('Ensemble mean and anomalies')
    ensmean = model_ds.mean('member')
    climo = ensmean.sel(init=slice(f'{first_year}-01-01', f'{last_year}-12-31')).groupby('init.month').mean('init')
    anom = model_ds.groupby('init.month') - climo
    anom = anom.rename({v: f'{v}_anom' for v in anom.data_vars})
    model_ds = xarray.merge([model_ds, anom])
    # Write the climatology, being sure that appropriate coords are ints.
    # Also trying to remove the empty dimension "time" from the output.
    encoding = {v: {'dtype': 'int32'} for v in ['lead', 'month']}
    climo.encoding = {}
    print('Writing climatology')
    climo.to_netcdf(model_output_data / f'climatology_{args.domain}_{first_year}_{last_year}.nc',
        encoding=encoding)
    # Do the same for the full set of forecasts
    encoding = {v: {'dtype': 'int32'} for v in ['lead', 'member', 'month']}
    print('Writing forecasts')
    model_ds.to_netcdf(model_output_data / f'forecasts_{args.domain}.nc', encoding=encoding)
