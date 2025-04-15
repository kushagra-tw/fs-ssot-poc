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

    # Perform spatial join based on distance
    gdf1, gdf2 = ensure_same_and_projected_crs(gdf1, gdf2)

    joined_gdf = gpd.sjoin_nearest(gdf1, gdf2, how=how, max_distance=distance)

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

    # print(joined_gdf.head(10))

    nearest_candidates = joined_gdf.loc[(joined_gdf['actual_distance_m'] <= distance)]

    # Remove temporary geometry columns
    gdf1.drop(columns=['geometry'], inplace=True)
    gdf2.drop(columns=['geometry'], inplace=True)

    return nearest_candidates

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

# # Example usage:
#
# focus_file = '/fs-ssot-poc/domains/customer/DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv'
# focus_data = pd.read_csv(focus_file, low_memory=False)
#
# nces_file = '/fs-ssot-poc/domains/customer/DataFiles/NCES_PUBL_PRIV_POSTSEC_SCHOOL_LOCATIONS.csv'
# nces_data = pd.read_csv(nces_file, low_memory=False)
# # print(focus_data.columns)
# # print(nces_data.columns)
#
# focus_geodf = create_geodataframe_from_lat_lon(focus_data,lat_col='ADDRESS_LATITUDE',lon_col='ADDRESS_LONGITUDE')
# nces_geodf = create_geodataframe_from_lat_lon(nces_data,lat_col='LAT',lon_col='LON')
# # Example using left_on and right_on
#

# #
# joined_gdf = join_geodataframes_by_lat_lon_columns(focus_geodf, nces_geodf,
#                                                    left_lat='ADDRESS_LATITUDE', left_lon='ADDRESS_LONGITUDE', right_lat='LAT',
#                                                    right_lon='LON', how='left', distance=50)
#
# df_with_name_similarity = add_similarity_score(joined_gdf, 'SCHOOL_NAME', 'SCH_NAME', 'school_name_similarity_ratio')
# # df_with_name_district_similarity = add_similarity_score(joined_gdf, 'SCHOOL_NAME', 'SCH_NAME', 'school_name_similarity_ratio')
# print(df_with_name_similarity)