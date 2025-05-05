import xarray as xr
import sys

file_ = sys.argv[1]

ds = xr.open_dataset(file_, group='product')
#ds_ = 'TEMPO_NO2_L3_V03_20231205T125853Z_S002.nc'
#ds_p = xr.open_dataset(ds_, group='product')
#new_file = ds_p._replace('.nc','_no2.nc')
#ds_p.to_netcdf(new_file)
ds.to_netcdf(file_[::-3]+'_no2.nc')
