#!/usr/bin/env python
# coding: utf-8

# In[1]:


import xarray as xr
import glob
import re
import os
import shutil
from datetime import datetime
from tqdm import tqdm

def extract_datetime(filename):
    match = re.search(r'_(\d{8}T\d{6})Z_', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%dT%H%M%S')
    raise ValueError(f"❌ Could not parse datetime from {filename}")

# Load coordinate templates
lat_ds = xr.open_dataset("tempo_lat.nc")
lon_ds = xr.open_dataset("tempo_lon.nc")

files = sorted(glob.glob('TEMPO_NO2_L3_V03_*.nc'))
zarr_store = 'tempo_streamed.zarr'


# In[2]:


import xarray as xr
import glob
import os
from datetime import datetime

# Step 1: List files (modify number or pattern as needed)
file_list = sorted(glob.glob("TEMPO_NO2_L3_V03_*.nc"))

# Step 2: Define a preprocess function to inject correct datetime
def assign_correct_time(ds):
    filename = ds.encoding['source']
    timestamp_str = os.path.basename(filename).split('_')[4]  # e.g., '20230802T151249Z'
    dt = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%SZ")
    return ds.assign_coords(time=("time", [dt]))

# Step 3: Load multiple files with assigned datetimes
ds = xr.open_mfdataset(
    file_list,
    combine='nested',
    concat_dim='time',
    chunks={'time': 100, 'latitude': 1000, 'longitude': 1000},
    preprocess=assign_correct_time
)
import xarray as xr
# Step 4: Select variable of interest (adjust if needed)
ds = ds[['vertical_column_troposphere']]

# Step 5: Add day-of-week coordinate
ds = ds.assign_coords(dayofweek=ds.time.dt.dayofweek)

# Step 6: Group and compute weekly climatology
weekly_clim = ds.groupby('dayofweek').mean('time')

# Step 7: Rename day indices to names (optional)
day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
weekly_clim = weekly_clim.assign_coords(dayofweek=[day_names[d] for d in weekly_clim.dayofweek.values])

# # Step 8: Save result to NetCDF
# weekly_clim.to_netcdf("weekly_climatology.nc")
# print("✅ Saved weekly climatology to 'weekly_climatology.nc'")


# # In[3]:


# weekly_clim


# In[5]:


# Load coordinate templates
lat_ds = xr.open_dataset("tempo_lat.nc")
lon_ds = xr.open_dataset("tempo_lon.nc")


# In[6]:


lon_ds


# In[7]:


import xarray as xr

# Extract the actual coordinate values from lat_ds and lon_ds
latitudes = lat_ds.latitude
longitudes = lon_ds.longitude

# Assign them as coordinates to the weekly_clim dataset
weekly_clim = weekly_clim.assign_coords({
    "latitude": latitudes,
    "longitude": longitudes
})


# # In[9]:


# weekly_clim


# In[10]:


from dask.diagnostics import ProgressBar
import dask

# Lazily prepare the writing
delayed_obj = weekly_clim.to_zarr(
    "weekly_clim.zarr",
    mode='w',
    compute=False,
    consolidated=True
)

# Use a progress bar when computing
with ProgressBar():
    dask.compute(delayed_obj)


# In[11]:


# # Select just the point of interest BEFORE triggering any compute or plot
# point_data = weekly_clim.sel(
#     latitude=30.2672, longitude=-97.7431, method='nearest'
# ).vertical_column_troposphere

# # Force computation only on the sliced (small) data
# point_data = point_data.compute()

# # Now it's safe to plot
# point_data.plot()


# In[12]:


# import xarray as xr
# import glob
# import os
# from datetime import datetime
# import matplotlib.pyplot as plt
# import pandas as pd
# from tqdm import tqdm

# # Define a list of cities with lat/lon
# cities = {
#     "Austin": (30.2672, -97.7431),
#     "New York": (40.7128, -74.0060),
#     "Los Angeles": (34.0522, -118.2437),
#     "Chicago": (41.8781, -87.6298),
#     "Houston": (29.7604, -95.3698),
#     "Phoenix": (33.4484, -112.0740),
#     "Philadelphia": (39.9526, -75.1652),
#     "San Antonio": (29.4241, -98.4936),
#     "San Diego": (32.7157, -117.1611),
#     "Dallas": (32.7767, -96.7970),
#     "San Jose": (37.3382, -121.8863),
#     "Detroit": (42.3314, -83.0458),
#     "Seattle": (47.6062, -122.3321),
#     "Miami": (25.7617, -80.1918),
#     "Denver": (39.7392, -104.9903),
#     "Boston": (42.3601, -71.0589)
# }

# # Load precomputed weekly climatology
# weekly_clim = xr.open_dataset('weekly_climatology.nc')  # Optional if already saved
# # Or use your in-memory weekly_clim directly if just processed

# # Set font sizes
# plt.rcParams.update({
#     'font.size': 18,
#     'axes.titlesize': 18,
#     'axes.labelsize': 18,
#     'xtick.labelsize': 14,
#     'ytick.labelsize': 14
# })

# # Create 4x4 grid of subplots
# fig, axes = plt.subplots(4, 4, figsize=(24, 20))
# axes = axes.flatten()

# # Plot for each city
# for i, (city, (lat, lon)) in tqdm(enumerate(cities.items())):
#     ax = axes[i]

#     # Find nearest grid cell
#     city_data = weekly_clim.sel(latitude=lat, longitude=lon, method='nearest')
    
#     # Extract NO2 values and plot
#     y = city_data.vertical_column_troposphere
#     x = city_data.dayofweek.values  # These are strings like 'Monday', etc.
    
#     ax.plot(x, y)
#     ax.set_title(city)
#     ax.set_ylabel('NO₂ (molecules/cm²)')
#     ax.set_xlabel('Day of Week')
#     ax.tick_params(axis='x', rotation=45)

# # Global title and layout
# plt.suptitle('Weekly Mean NO₂ Concentration by City (NASA TEMPO)', fontsize=22)
# plt.tight_layout(rect=[0, 0, 1, 0.97])

# # Save & show
# plt.savefig('us_cities_no2_weekly_climatology.png', dpi=300)
# plt.show()


# In[ ]:




