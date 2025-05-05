from pathlib import Path
from subprocess import run


def run_cmd(cmd):
    run([cmd], shell=True, check=True)


def main(year, input_dir, output_dir, n_segments):
    for var in ['thetao', 'so', 'uv', 'zos']:
        for seg in range(1, n_segments + 1):
            available_months = []
            # Search for December of the previous year to use
            # to pad the beginning of the yearly file.
            # If not found, roll the time of the first day back by one.
            prev_month = input_dir / f'{var}_{seg:03d}_{year - 1}-12.nc'
            if prev_month.exists():
                tail_file = prev_month.with_suffix('.tail.nc')
                run_cmd(f'ncks {prev_month} -d time,-1,-1 -O {tail_file}')
            else:
                print('Padding with first time')
                first_file = input_dir / f'{var}_{seg:03d}_{year}-01.nc'
                tail_file = first_file.with_suffix('.tail.nc')
                # Pick out the first time from the first month.
                run_cmd(f'ncks {first_file} -d time,0,0 -O {tail_file}')
                # Pad by subtracting one day from the time.
                run_cmd(f'ncap2 -s "time-=24" {tail_file} -O {tail_file}')
            available_months.append(tail_file)
            # Search for the months of this year, stopping
            # if one month is not found.
            for mon in range(1, 13):
                expected_file = input_dir / f'{var}_{seg:03d}_{year}-{mon:02d}.nc'
                if expected_file.exists():
                    available_months.append(expected_file)
                else:
                    break
            if len(available_months) < 2:  # end of prev year plus this year
                raise Exception('Did not find data')
            # Search for January of the next year to use
            # to pad the end of the yearly file.
            # If not found, roll the time of the last day forward by one.
            next_month = input_dir / f'{var}_{seg:03d}_{year + 1}-01.nc'
            if len(available_months) == 13 and next_month.exists():
                head_file = next_month.with_suffix('.head.nc')
                run_cmd(f'ncks {next_month} -d time,0,0 -O {head_file}')
            else:
                print('Padding with last time')
                last_file = available_months[-1]
                head_file = last_file.with_suffix('.head.nc')
                # Pick out the first time from the first month.
                run_cmd(f'ncks {last_file} -d time,-1,-1 -O {head_file}')
                # Pad by subtracting one day from the time.
                run_cmd(f'ncap2 -s "time+=24" {head_file} -O {head_file}')
            available_months.append(head_file)
            output_file = output_dir / f'{var}_{seg:03d}_{year}.nc'
            cmd = f'ncrcat {" ".join(map(str, available_months))} -O {output_file}'
            print(cmd)
            run_cmd(cmd)
            # TODO: proleptic_gregorian attribute carries over because of setting
            # the time units when extracting.
            # Fix it here for now.
            # Also, time is an int but this could be ok since it's not int64.
            run_cmd(f'ncatted -a calendar,time,o,c,gregorian -O {output_file}')
            tail_file.unlink()
            head_file.unlink()
            print(' ')


if __name__ == '__main__':
    import argparse
    from pathlib import Path

    from yaml import safe_load

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-y', '--year', type=int, required=True)
    args = parser.parse_args()
    with open(args.config, 'r') as file:
        config = safe_load(file)
    in_dir = Path(config['filesystem']['nowcast_input_data']) / 'boundary' / 'monthly'
    out_dir = in_dir.parents[0]
    n_seg = len(config['domain']['boundaries'])
    main(args.year, in_dir, out_dir, n_seg)
