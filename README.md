# seasonal-workflow

Steps:

0. Create a configuration yaml file.

1. Create data for the nudged analysis run

    a. Set up other data (ERA5, GLORYS boundary conditions, etc) as usual
  
    b. Nudging time scale: `write_damping_file.py`
  
    c. Monthly average T and S to nudge to: `write_nudging_data.py`

2. Run the nudged analysis simulation

    a. See [this link](https://github.com/NOAA-CEFI-Regional-Ocean-Modeling/regional-mom6-xml/blob/5969a267989f7f661d8e604cc2f666011a3f582a/NWA12/NWA12_physics.xml#L636) for an example physics-only XML experiment with nudging

    b. See xml/diag_table_snapshots for an example diag table to use to save monthly snapshots from the nudged simulation that can be used as initial conditions. 

4. Set up a forecast simulation
   
    a. Atmospheric forcing from a SPEAR forecast: `write_spear_atmos.py`
   
    b. River climatology: `write_river_climo.py`
   
    c. Boundary condition climatology: `write_boundary_climo.py`
   
    d. Initial conditions derived from the nudged run: `write_ics_from_snapshot.py`

5. Run many forecast simulations

6. Post-process the forecast data
   
    a. Extract some 2D fields from all available history tar files: `postprocess_extract_fields.py`

   b. Combine the extracted data files together and calculate lead-dependent climatologies and anomalies: `postprocess_combine_fields.py`
