from pathlib import Path
from subprocess import run

def run_cmd(cmd):
    run([cmd], shell=True, check=True)


def main(year, input_dir, output_dir, n_segments):
    for var in ['thetao', 'so', 'uv', 'zos']:
        for seg in range(1, n_segments+1):
            available_months = []
            prev_month = input_dir / f'{var}_{seg:03d}_{year-1}-12.nc'
            if prev_month.exists():
                available_months.append(prev_month)
            else:
                print('Padding with first time')
                first_file = input_dir / f'{var}_{seg:03d}_{year}-01.nc'
                tail_file = first_file.with_suffix('.tail.nc')
                # Pick out the first time from the first month.
                run_cmd(f'ncks {first_file.as_posix()} -d time,0,0 -O {tail_file.as_posix()}')
                # Pad by subtracting one day from the time.
                run_cmd(f'ncap2 -s "time-=1" {tail_file.as_posix()} -O {tail_file.as_posix()}')
                available_months.append(tail_file)
            for mon in range(1, 13):
                expected_file = input_dir / f'{var}_{seg:03d}_{year}-{mon:02d}.nc'
                if expected_file.exists():
                    available_months.append(expected_file)
            if len(available_months) < 2: # end of prev year plus this year
                raise Exception('Did not find data')
            next_month = input_dir / f'{var}_{seg:03d}_{year+1}-01.nc'
            if next_month.exists():
                available_months.append(next_month)
            else:
                print('TODO: extend time past end')
                pass # extend time of last file
            output_file = output_dir / f'{var}_{seg:03d}_{year}.nc'
            cmd = f'ncrcat {" ".join(map(lambda x: x.as_posix(), available_months))} -O {output_file.as_posix()}'
            print(cmd)
            run_cmd(cmd)
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
    in_dir = Path(config['filesystem']['nowcast_input_data'] / 'boundary' / 'monthly')
    out_dir = in_dir.parents[0]
    n_seg = len(config['domain']['boundaries'])
    main(args.year, in_dir, out_dir, n_seg)
