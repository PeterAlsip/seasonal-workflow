# Using ptmp to cache full history files
import subprocess
from dataclasses import dataclass
from getpass import getuser
from pathlib import Path


@dataclass
class ForecastRun:
    ystart: int
    mstart: int
    ens: int
    template: str
    outdir: Path
    name: str = ''
    domain: str = 'ocean_month'
    vftmp: Path = Path('/vftmp') / getuser()
    ptmp: Path = Path('/ptmp') / getuser()

    @property
    def archive_dir(self) -> Path:
        """
        Using a string template for the name of a single forecast's
        history directory on archive, and format it with the current forecast's
        year, month, and ensemble member.
        """
        return Path(
            self.template.format(year=self.ystart, month=self.mstart, ensemble=self.ens)
        )

    @property
    def tar_file(self) -> str:
        """
        Name of the tar file stored on archive.
        """
        return f'{self.ystart}{self.mstart:02d}01.nc.tar'

    @property
    def ptmp_dir(self) -> Path:
        """
        Location on /ptmp to cache data. This is intended to be the same path used by frepp
        so that it can take advantage of the frepp cache.
        """
        return (
            self.ptmp
            / self.archive_dir.relative_to(self.archive_dir.root)
            / f'{self.ystart}{self.mstart:02d}01.nc'
        )

    @property
    def vftmp_dir(self) -> Path:
        """
        Location on vftmp to cache extracted data.
        """
        return self.vftmp / 'forecast_data' / self.name / f'e{self.ens:02d}'

    @property
    def file_name(self) -> str:
        """
        Name of the file in the tar file to extract.
        """
        return f'{self.ystart}{self.mstart:02d}01.{self.domain}.nc'

    @property
    def out_name(self) -> str:
        """
        Name to give the final processed file.
        """
        return f'{self.ystart}-{self.mstart:02d}-e{self.ens:02d}.{self.domain}.nc'

    @property
    def exists(self) -> bool:
        return (self.archive_dir / self.tar_file).is_file()

    @property
    def needs_dmget(self) -> bool:
        return (
            self.exists
            and not (self.vftmp_dir / self.file_name).is_file()
            and not (self.ptmp_dir / self.file_name).is_file()
        )

    def run_cmd(self, cmd: str) -> None:
        print(cmd)
        subprocess.run([cmd], shell=True, check=True)

    def copy_from_archive(self) -> None:
        """
        Extract the file for this domain, from the tar file on archive, to the path on /ptmp.
        """
        if not self.exists:
            raise FileNotFoundError(
                f'File {(self.archive_dir / self.tar_file)} does not exist.'
            )
        self.ptmp_dir.mkdir(parents=True, exist_ok=True)
        cmd = f'tar xf {(self.archive_dir / self.tar_file).as_posix()} -C {self.ptmp_dir.as_posix()} ./{self.file_name}'
        self.run_cmd(cmd)

    def copy_from_ptmp(self) -> None:
        """
        Copy the file for this domain from ptmp to vftmp.
        """
        self.vftmp_dir.mkdir(parents=True, exist_ok=True)
        cmd = f'gcp {(self.ptmp_dir / self.file_name).as_posix()} {self.vftmp_dir.as_posix()}'
        self.run_cmd(cmd)
