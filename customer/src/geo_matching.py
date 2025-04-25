import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from geopy.distance import geodesic
import numpy as np

from domains.customer.name_similarity_scoring import add_similarity_score

def ensure_same_and_projected_crs(gdf1, gdf2, target_crs='EPSG:32633'): # Example: UTM Zone 33N (meters)
    """
    Ensures both GeoDataFrames have the same CRS and projects them to a target projected CRS.

    Args:
        gdf1 (GeoDataFrame): The first GeoDataFrame.
        gdf2 (GeoDataFrame): The second GeoDataFrame.
        target_crs (str): The target projected CRS (e.g., 'EPSG:32633' for meters).

    Returns:
        tuple: The reprojected GeoDataFrames (gdf1, gdf2).
    """
    if gdf1.crs is None:
        gdf1.crs = 'EPSG:4326'  # Assume WGS 84 if no CRS is set
    if gdf2.crs is None:
        gdf2.crs = 'EPSG:4326'  # Assume WGS 84 if no CRS is set

    if gdf1.crs != target_crs:
        gdf1 = gdf1.to_crs(target_crs)
    if gdf2.crs != target_crs:
        gdf2 = gdf2.to_crs(target_crs)

    return gdf1, gdf2

def join_geodataframes_by_lat_lon_columns(gdf1, gdf2, left_lat='lat1', left_lon='lon1', right_lat='lat2', right_lon='lon2', how='inner', distance=50):
    """
    Joins two GeoDataFrames based on latitude and longitude columns with different names.

    Args:
        gdf1 (GeoDataFrame): The left GeoDataFrame.
        gdf2 (GeoDataFrame): The right GeoDataFrame.
        left_lat (str): The latitude column name in gdf1.
        left_lon (str): The longitude column name in gdf1.
        right_lat (str): The latitude column name in gdf2.
        right_lon (str): The longitude column name in gdf2.
        how (str): Type of join. 'inner', 'left', or 'right'.
        distance (float): The maximum distance (in meters) for matching points.
    Returns:
        GeoDataFrame: The joined GeoDataFrame.
    """

    # Create geometry columns with standard names
    gdf1['geometry'] = gpd.points_from_xy(gdf1[left_lon], gdf1[left_lat])
    gdf2['geometry'] = gpd.points_from_xy(gdf2[right_lon], gdf2[right_lat])

    # Ensure both GeoDataFrames are in the same projected CRS (e.g., UTM for meters)
    gdf1, gdf2 = ensure_same_and_projected_crs(gdf1, gdf2)

    # Perform spatial join based on distance (now in meters)
    joined_gdf = gpd.sjoin_nearest(gdf1, gdf2, how=how, max_distance=distance, lsuffix='_left', rsuffix='_right')

    # Calculate the actual geodesic distance between the original lat/lon points, handling NaNs
    def calculate_geodesic_distance(row):
        lat1 = row[left_lat]
        lon1 = row[left_lon]
        lat2 = row[right_lat]
        lon2 = row[right_lon]
        if np.isnan(lat1) or np.isnan(lon1) or np.isnan(lat2) or np.isnan(lon2):
            return np.nan  # Return NaN if any coordinate is NaN
        else:
            return geodesic((lat1, lon1), (lat2, lon2)).meters

    joined_gdf['actual_distance_m'] = joined_gdf.apply(calculate_geodesic_distance, axis=1)

    # Remove temporary geometry columns
    gdf1.drop(columns=['geometry'], inplace=True)
    gdf2.drop(columns=['geometry'], inplace=True)

    return joined_gdf

def create_geodataframe_from_lat_lon(df, lat_col='latitude', lon_col='longitude', crs='EPSG:4326'):
    """
    Creates a GeoDataFrame from a Pandas DataFrame with latitude and longitude columns.

    Args:
        df (DataFrame): The Pandas DataFrame.
        lat_col (str): The name of the latitude column.
        lon_col (str): The name of the longitude column.
        crs (str): Coordinate Reference System (CRS). Default is EPSG:4326.

    Returns:
        GeoDataFrame: The created GeoDataFrame.
    """

    geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=crs)
    return gdf
