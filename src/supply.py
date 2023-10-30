import geopandas
import numpy as np
import pandas as pd

from crs import Albers_Equal_Area_Russia


def add_user_shops(city_shops: geopandas.GeoDataFrame, user_input: dict):
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


def measure_market_share(residents, shops):
    gdf = shops.sjoin(residents)
    gdf = gdf.drop("index_right", axis=1).reset_index(drop=True)
    gdf["dist"] = gdf.distance(gdf["coords"])
    gdf["dist"] = np.where(gdf["dist"] == 0, 30, gdf["dist"])
    gdf["attract"] = gdf["store_area"] / gdf["dist"] ** 2
    gdf = gdf.assign(totattract=gdf.groupby("HOUSE_ID")["attract"].transform("sum"))
    gdf["marketshare"] = gdf["attract"] / gdf["totattract"]
    return gdf


def func(gdf):
    gdf = gdf[gdf["shop_id"].isna()]
    gdf["traffic"] = (gdf["INHAB"] * gdf["marketshare"]).astype("int")
    gdf.to_crs("EPSG:4326", inplace=True)
    gdf = gdf.groupby(["name", "geometry", "store_area"], as_index=False).agg(
        {"traffic": "sum"}
    )
    return gdf
