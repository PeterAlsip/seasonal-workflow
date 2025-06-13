from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from yaml import safe_load


class RetrospectiveForecasts(BaseModel):
    first_year: int
    last_year: int
    months: list[int]
    ensemble_size: int

class NewForecasts(BaseModel):
    ensemble_size: int

class Climatology(BaseModel):
    first_year: int
    last_year: int

class Boundaries(BaseModel):
    south: int
    north: int
    east: int

class Domain(BaseModel):
    south_lat: float
    north_lat: float
    west_lon: float
    east_lon: float
    hgrid_file: Path
    ocean_mask_file: Path
    ocean_static_file: Path
    boundaries: Boundaries

class Regions(BaseModel):
    mask_file: Path
    names: list[str]

class InterimData(BaseModel):
    ERA5: Path
    GLORYS_reanalysis: Path
    GLORYS_analysis: Path
    GloFAS_ldd: Path
    GloFAS_v4: str
    GloFAS_interim: str
    GloFAS_interim_monthly: str
    GloFAS_extension_climatology: Path

class Filesystem(BaseModel):
    forecast_input_data: Path
    nowcast_input_data: Path
    forecast_output_data: Path
    gaea_input_data: Path
    yearly_river_files: str
    open_boundary_files: Path
    glorys_interpolated: Path
    interim_data: InterimData
    analysis_history: Path
    analysis_extensions: Optional[list[Path]] = []
    nowcast_history: str
    forecast_history: str
    combined_name: str

class Config(BaseModel):
    name: str
    retrospective_forecasts: RetrospectiveForecasts
    new_forecasts: NewForecasts
    snapshots: list[str]
    climatology: Climatology
    domain: Domain
    regions: Regions
    variables: dict[str, list[str]]
    filesystem: Filesystem

def load_config(config_path: str | Path) -> Config:
    """Load and parse the YAML configuration file."""
    with open(config_path) as f:
        data = safe_load(f)
    return Config.model_validate(data)

if __name__ == '__main__':
    config = load_config('config_nwa12_cobalt.yaml')
    print(config.model_dump_json(indent=2))
