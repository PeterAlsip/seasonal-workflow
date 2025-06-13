from getpass import getuser
from pathlib import Path

from loguru import logger

from config import load_config

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True)
    parser.add_argument('-d', '--domain', type=str, default='ocean_month')
    parser.add_argument('-D', '--dry', action='store_true')
    args = parser.parse_args()
    config = load_config(args.config)

    model_output_data = config.filesystem.forecast_output_data
    # Fully extracted files, generally on /work
    files = (model_output_data / 'extracted' / args.domain).glob(
        f'????-??-e??.{args.domain}.nc'
    )
    for f in files:
        logger.info(f'rm {f.name}')
        if not args.dry:
            f.unlink()

    # Temporary files directly extracted from tar history files,
    # on /vftmp
    VFTMP = Path('/vftmp') / getuser()
    for ens in range(1, config.retrospective_forecasts.ensemble_size + 1):
        tmp_dir = VFTMP / 'forecast_data' / config.name / f'e{ens:02d}'
        files = tmp_dir.glob(f'????????.{args.domain}.nc')
        for f in files:
            logger.info(f'rm {f.name}')
            if not args.dry:
                f.unlink()
