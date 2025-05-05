import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import xarray as xr
import numpy as np
from matplotlib.colors import LogNorm
from matplotlib.ticker import LogLocator, LogFormatterSciNotation

# Load the weekly climatology dataset
weekly_clim = xr.open_zarr('weekly_clim.zarr')
tempo_clim = weekly_clim.mean(dim='dayofweek')

# Sample cities dictionary
cities = {
    "Austin": (30.2672, -97.7431),
    "New York": (40.7128, -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298),
    "Houston": (29.7604, -95.3698),
    "Phoenix": (33.4484, -112.0740),
    "Philadelphia": (39.9526, -75.1652),
    "San Antonio": (29.4241, -98.4936),
    "San Diego": (32.7157, -117.1611),
    "Dallas": (32.7767, -96.7970),
    "San Jose": (37.3382, -121.8863),
    "Detroit": (42.3314, -83.0458),
    "Seattle": (47.6062, -122.3321),
    "Miami": (25.7617, -80.1918),
    "Denver": (39.7392, -104.9903),
    "Boston": (42.3601, -71.0589)
}

# Extract data variable name
data_var = list(tempo_clim.data_vars)[0]
clim_data = tempo_clim[data_var]

# Mask non-positive values (required for LogNorm)
positive_data = clim_data.where(clim_data > 0)

# Manually set vmin and vmax to visualize NO2 better
vmin = 1e15
vmax = 1e18

# Check validity
if vmin <= 0 or vmax <= 0 or vmin >= vmax:
    raise ValueError("Invalid vmin or vmax values for LogNorm.")

# Set up the map
fig = plt.figure(figsize=(12, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

# Plot the data
img = positive_data.plot(
    ax=ax,
    transform=ccrs.PlateCarree(),
    cmap='inferno_r',
    norm=LogNorm(vmin=vmin, vmax=vmax),
    cbar_kwargs={
        'label': f"{data_var} (units)",  # Replace with actual units if known
        'shrink': 0.6,
        'aspect': 20,
        'pad': 0.02
    }
)

# Format colorbar with scientific notation
colorbar = ax.collections[0].colorbar
colorbar.ax.yaxis.set_major_locator(LogLocator(base=10.0))
colorbar.ax.yaxis.set_major_formatter(LogFormatterSciNotation())

# Add map features
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.add_feature(cfeature.STATES, linestyle='-', edgecolor='gray')

# Plot cities with cross markers, leader lines, and spaced labels
for i, (city, (lat, lon)) in enumerate(cities.items()):
    # Plot a black cross
    ax.plot(lon, lat, marker='x', color='black', markersize=6, transform=ccrs.PlateCarree())

    # Define offset direction to reduce overlap
    dx = 1.0 if i % 2 == 0 else -1.2
    dy = 1.0 if i % 3 == 0 else -1.0

    text_lon = lon + dx
    text_lat = lat + dy

    # Draw line from city to label
    ax.plot([lon, text_lon], [lat, text_lat], color='black', linewidth=0.5, transform=ccrs.PlateCarree())

    # Add text label
    ax.text(text_lon, text_lat, city, fontsize=9, weight='bold', color='black', transform=ccrs.PlateCarree())

# Add title
plt.title("NO2 Climatology", fontsize=16)
plt.tight_layout()

# Save the figure
plt.savefig("weekly_climatology_map.png", dpi=300)
# plt.show()
