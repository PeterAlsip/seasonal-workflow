# seasonal-workflow

## Environment setup

### First time setup (on GFDL analysis)

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and add it to your path.

Then,
```sh
module load python/3.13 
module load esmf/8.7.0 
uv sync
source .venv/bin/activate.csh # or without the .csh depending on shell
```

### Loading python and required modules (on GFDL analysis)

```sh
module load nco/5.2.4
module load cdo/2.4.4
module load gcp hsm/1.3.0
module load python/3.13 
module load esmf/8.7.0 
source .venv/bin/activate.csh # or without the .csh depending on shell
```


## Forecast steps (without rocoto workflow)

Steps:

0. Create a configuration yaml file.

1. Create data for the nudged analysis run
    + Set up other data (ERA5, GLORYS boundary conditions, etc) as usual  
    + Nudging time scale: `write_damping_file.py`
    + Monthly average T and S to nudge to: `write_nudging_data.py`

2. Run the nudged analysis simulation
    + See [this link](https://github.com/NOAA-CEFI-Regional-Ocean-Modeling/regional-mom6-xml/blob/5969a267989f7f661d8e604cc2f666011a3f582a/NWA12/NWA12_physics.xml#L636) for an example physics-only XML experiment with nudging
    + See xml/diag_table_snapshots for an example diag table to use to save monthly snapshots from the nudged simulation that can be used as initial conditions. 

4. Set up a forecast simulation
    + Atmospheric forcing from a SPEAR forecast: `write_spear_atmos.py`
    + River climatology: `write_river_climo.py`
    + Boundary condition climatology: `write_boundary_climo.py`
    + Initial conditions derived from the nudged run: `write_ics_from_snapshot.py`

5. Run many forecast simulations
    + Build a template XML based on NWA12_forecast_common.xml
        - The template XML contains XML data that is common to all experiments, with places to fill in the experiment start year and month. 
        - Each ensemble member is its own experiment. Each member inherits shared data from a base experiment and adds its own unique files. 
        - The example NWA12_forecast_common.xml file contains experiments using both climatological open boundary conditions and SPEAR-derived OBCs. 
        - The template XML is included by the forecast XMLs. Any change to the template XML will automatically propagate to the forecast XMLs. 
    + Generate an XML for the forecast start year and month. 
        - `python write_forecast_xml.py NWA12_forecast_common.xml 1993 03` will create NWA12_forecast_1993_03.xml which contains data for a forecast starting on 1993-03-01, using the template file NWA12_forecast_common.xml. 
        - The ensemble member experiments in the generated XML can be frerun-ed. 
        - The generated XML can also be used to compile the model if needed (only needs to be done once).

6. Post-process the forecast data
    + Extract some 2D fields from all available history tar files: `postprocess_extract_fields.py`
    + Combine the extracted data files together and calculate lead-dependent climatologies and anomalies: `postprocess_combine_fields.py`

## Disclaimer

The United States Department of Commerce (DOC) GitHub project code is provided on an 'as is' basis and the user assumes responsibility for its use. The DOC has relinquished control of the information and no longer has responsibility to protect the integrity, confidentiality, or availability of the information. Any claims against the Department of Commerce stemming from the use of its GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.

This project code is made available through GitHub but is managed by NOAA-GFDL at [https://www.gfdl.noaa.gov](https://www.gfdl.noaa.gov).
