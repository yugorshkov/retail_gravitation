import geopandas
import numpy as np
import pandas as pd
from geopandas import GeoDataFrame

from src.crs import Albers_Equal_Area_Russia


def add_user_shops(city_shops: GeoDataFrame, user_input: dict) -> GeoDataFrame:
    """"""
    df = pd.DataFrame(
        {
            "name": [user_input[coords][0] for coords in user_input],
            "store_area": [user_input[coords][1] for coords in user_input],
            "geometry": [f"POINT({coords})" for coords in user_input],
        }
    )
    df["geometry"] = geopandas.GeoSeries.from_wkt(df["geometry"])
    user_shops_data = geopandas.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    user_shops_data.to_crs(Albers_Equal_Area_Russia, inplace=True)
    shops = pd.concat([city_shops, user_shops_data])
    return shops


def huff_gravity_model(residents: GeoDataFrame, shops: GeoDataFrame) -> GeoDataFrame:
    gdf = shops.sjoin(residents)
    gdf = gdf.drop("index_right", axis=1).reset_index(drop=True)
    gdf["dist"] = gdf.distance(gdf["coords"])
    gdf["dist"] = np.where(gdf["dist"] == 0, 30, gdf["dist"])
    gdf["attract"] = gdf["store_area"] / gdf["dist"] ** 2
    gdf = gdf.assign(totattract=gdf.groupby("HOUSE_ID")["attract"].transform("sum"))
    gdf["marketshare"] = gdf["attract"] / gdf["totattract"]
    return gdf


def expected_number_of_consumers(gdf: GeoDataFrame):
    gdf = gdf[gdf["shop_id"].isna()]
    gdf["traffic"] = (gdf["INHAB"] * gdf["marketshare"]).astype("int")
    gdf.to_crs("EPSG:4326", inplace=True)
    gdf = gdf.groupby(["name", "store_area"], as_index=False).agg(
        {"traffic": "sum"}
    )
    return gdf
