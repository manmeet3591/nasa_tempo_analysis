import xarray as xr
import glob
import os

# Define the pattern to match files
file_pattern = "TEMPO_NO2_L3_V03_*.nc"

# Get a list of all matching files in the current directory
files = glob.glob(file_pattern)

# Loop through each file and process it
for file_name in files:
    try:
        # Open the NetCDF file
        ds = xr.open_dataset(file_name, group='product')

        # Modify the file name to add '_no2' before '.nc'
        new_file_name = file_name.replace('.nc', '_no2.nc')

        # Save the dataset to the new file
        ds.to_netcdf(new_file_name)

        print(f"Processed: {file_name} -> {new_file_name}")

    except Exception as e:
        print(f"Error processing {file_name}: {e}")
