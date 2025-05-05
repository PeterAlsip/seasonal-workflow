import os
from pathlib import Path
from subprocess import run


def run_cmd(cmd, print_cmd=True):
    if print_cmd:
        print(cmd)
    run(cmd, shell=True, check=True)


def main(d2m_file, sp_file, tmpdir, outdir):
    d2m_str = d2m_file.as_posix()
    sp_str = sp_file.as_posix()
    tmp_str = tmpdir.as_posix()
    if outdir is None:
        outdir = d2m_file.parent
    svp_str = (tmpdir / 'svp_tmp.nc').as_posix()
    sphum_str = (
        tmpdir / d2m_file.name.replace('d2m', 'sphum')
    ).as_posix()  # assuming d2m in name
    run_cmd(f'gcp {d2m_str} {tmp_str}')
    run_cmd(f'gcp {sp_str} {tmp_str}')
    run_cmd(
        f'cdo expr,"svp=611.2*exp(17.67*(d2m-273.15)/(d2m-29.65))" {(tmpdir / d2m_file.name).as_posix()} {svp_str}'
    )
    run_cmd(
        f'cdo -expr,"_mr=0.622*svp/(sp-svp);sphum=_mr/(1+_mr);" -merge {svp_str} {(tmpdir / sp_file.name).as_posix()} {sphum_str}'
    )
    run_cmd(f'gcp {sphum_str} {outdir}')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--d2m', required=True)
    parser.add_argument('-p', '--sp', required=True)
    parser.add_argument('-o', '--out', default=None)
    args = parser.parse_args()
    tmp = Path(os.environ['TMPDIR'])
    main(Path(args.d2m), Path(args.sp), tmp, args.out)
