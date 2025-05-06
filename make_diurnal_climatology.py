import matplotlib.pyplot as plt
import xarray as xr
from tqdm import tqdm

# Cities and coordinates
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

# Manual UTC offset from Eastern Time
utc_offset_from_eastern = {
    'New York': 0, 'Boston': 0, 'Philadelphia': 0, 'Miami': 0, 'Detroit': 0,
    'Chicago': -1, 'Houston': -1, 'Dallas': -1, 'San Antonio': -1, 'Austin': -1,
    'Denver': -2, 'Phoenix': -2,
    'Los Angeles': -3, 'San Diego': -3, 'San Jose': -3, 'Seattle': -3
}

# Load dataset
diurnal_clim = xr.open_zarr('diurnal_clim.zarr')

plt.rcParams.update({
    'font.size': 16,
    'axes.titlesize': 16,
    'axes.labelsize': 16,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14
})

fig, axes = plt.subplots(4, 4, figsize=(24, 22))
axes = axes.flatten()

data_var = list(diurnal_clim.data_vars)[0]

for i, (city, (lat, lon)) in tqdm(enumerate(cities.items())):
    ds_city = diurnal_clim.sel(
        latitude=slice(lat - 0.5, lat + 0.5),
        longitude=slice(lon - 0.5, lon + 0.5)
    ).compute()

    city_mean = ds_city[data_var].mean(dim=['latitude', 'longitude'])

    offset = utc_offset_from_eastern.get(city, 0)
    adjusted_hours = (city_mean['hour'] + offset) % 24
    city_mean_local = city_mean.assign_coords(hour=adjusted_hours)

    # Sort by adjusted local hour instead of grouping
    hourly_mean = city_mean_local.sortby('hour')

    axes[i].plot(hourly_mean['hour'], hourly_mean, marker='o', markersize=4, linewidth=1.5)
    axes[i].set_title(city, fontsize=14)
    axes[i].set_ylabel('molecules/cm²')
    axes[i].set_xlim(0, 23)
    axes[i].set_xticks(range(0, 24, 3))
    axes[i].grid(True, linestyle='--', linewidth=0.5, alpha=0.7)

    if i >= 12:
        axes[i].set_xlabel('Hour of Day (Local Time)')
        axes[i].set_xticklabels(range(0, 24, 3), rotation=45)
        axes[i].tick_params(axis='x', pad=6)
    else:
        axes[i].set_xticklabels([])
        axes[i].set_xlabel("")

    ymin = hourly_mean.min().values * 0.95
    ymax = hourly_mean.max().values * 1.05
    axes[i].set_ylim([ymin, ymax])

plt.suptitle('Diurnal NO₂ Climatology (Local Time) over US Cities', fontsize=22)
plt.tight_layout()
fig.subplots_adjust(top=0.93, bottom=0.08, hspace=0.4, wspace=0.3)

plt.savefig('us_cities_no2_diurnal_climatology_local_hour_shifted.png', dpi=300)
plt.show()
