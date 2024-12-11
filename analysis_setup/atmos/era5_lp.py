from pathlib import Path
from subprocess import run


def main(tp_file, sf_file, tmpdir, outdir):
    tp_str = tp_file.as_posix()
    sf_str = sf_file.as_posix()
    if outdir is None:
        outdir = tp_file.parent
    lp_str = (tmpdir / tp_file.name.replace('tp', 'lp')).as_posix()
    cmd = f'cdo -setrtoc,-1e9,0,0 -chname,tp,lp -sub {tp_str} {sf_str} {lp_str}'
    print(cmd)
    run(cmd, shell=True, check=True)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tp', required=True)
    parser.add_argument('-s', '--sf', required=True)
    parser.add_argument('-o', '--out', default=None)
    args = parser.parse_args()
    main(Path(args.tp), Path(args.sf), args.out)
