import geopandas
import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from pandas import DataFrame

from src.crs import Albers_Equal_Area_Russia


def add_user_shops(city_shops: GeoDataFrame, user_input: DataFrame) -> GeoDataFrame:
    """Добовить информацию о магазинах, введённую пользователем в веб-интерфейсе
    к данным о магазинах города из OSM"""
    user_shops_data = geopandas.GeoDataFrame(
        user_input,
        geometry=geopandas.points_from_xy(user_input.longitude, user_input.latitude),
        crs="EPSG:4326",
    )
    user_shops_data.to_crs(Albers_Equal_Area_Russia, inplace=True)
    shops = pd.concat([city_shops, user_shops_data])
    return shops


def huff_gravity_model(residents: GeoDataFrame, shops: GeoDataFrame) -> GeoDataFrame:
    """Используя гравитационную модель Хаффа, вычисляем какая часть жильцов каждого
    дома в зоне влияния тороговой точки пойдёт в неё за покупками."""
    gdf = shops.sjoin(residents, how="left")
    gdf = gdf.drop("index_right", axis=1).reset_index(drop=True)
    gdf["dist"] = gdf.distance(gdf["coords"])
    gdf["dist"] = np.where(gdf["dist"] == 0, 30, gdf["dist"])
    gdf["attract"] = gdf["store_area"] / gdf["dist"] ** 2
    gdf = gdf.assign(totattract=gdf.groupby("HOUSE_ID")["attract"].transform("sum"))
    gdf["marketshare"] = gdf["attract"] / gdf["totattract"]
    return gdf


def expected_number_of_consumers(gdf: GeoDataFrame):
    """Определяем количество потенциальных покупателей для заданных
    пользователем магазинов."""
    gdf = gdf[gdf["shop_id"].isna()]
    gdf["traffic"] = (gdf["INHAB"] * gdf["marketshare"]).fillna(0).astype("int")
    gdf.to_crs("EPSG:4326", inplace=True)
    gdf = gdf.groupby(["name", "store_area"], as_index=False).agg({"traffic": "sum"})
    return gdf
