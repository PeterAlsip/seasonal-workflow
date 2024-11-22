

atmos:
pad ERA5
create sphum from d2m
create liquid precip with `cdo -setrtoc,-1e9,0,0 -chname,tp,lp -sub ERA5_tp_2022_padded.nc ERA5_sf_2022_padded.nc ERA5_lp_2022_padded.nc`

boundary:
write_boundary_reanalysis.py
- extract 1 month of glorys from uda
- slice and fill
- monthly average and save for sponging
- create boundary conditions

sponge:


rivers:
extend with glofas v4

(also need to create river nutrient file for glofas v4)


