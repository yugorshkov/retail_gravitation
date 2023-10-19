import json
import os

import geopandas
import numpy as np
import pyrosm
from prefect import flow, task

from crs import Albers_Equal_Area_Russia

SA_STORE_AREA_LIMIT = 300  # 300-500
SUPERMARKET_AREA_PART = 0.3  # 0.3-0.5
CONVENIENCE_AREA_PART = 0.15  # 0.15-0.3
WALKING_DISTANCE = 400  # 400-800


@task(retries=3, retry_delay_seconds=5)
def get_osm_data(city: dict, local_prefix: str = "data") -> None:
    os.system(
        f"src/osm_get_data.sh {city['region']} {local_prefix} {city['name']} {city['osm_id']}"
    )


@task
def process_apartment_buildings_data(path: str) -> geopandas.GeoDataFrame:
    gdf = geopandas.read_file(path)
    attrs = ["RMC", "RMC_LIVE", "INHAB", "geometry"]
    gdf = gdf[attrs]

    gdf = gdf.replace({"": np.nan})
    for col in ["RMC", "RMC_LIVE"]:
        gdf[col] = gdf[col].apply(lambda x: str(x).replace(" ", ""))
    col_types = {attr: "float" for attr in attrs[:-1]}
    gdf = gdf.astype(col_types)

    mask = ((gdf["RMC_LIVE"] > 1000) & (gdf["RMC_LIVE"] > gdf["RMC"])) | gdf[
        "RMC_LIVE"
    ].isna()
    gdf["RMC_LIVE"] = np.where(mask, gdf["RMC"], gdf["RMC_LIVE"])

    avg_residents_in_apartment = (gdf.INHAB / gdf.RMC_LIVE).mean().round(2)
    gdf["inhab_calc"] = gdf["RMC_LIVE"] * avg_residents_in_apartment
    residents_in_house = gdf["inhab_calc"].mean()
    gdf["inhab_calc"] = gdf["inhab_calc"].fillna(residents_in_house)
    gdf["inhab_calc"] = gdf[["INHAB", "inhab_calc"]].max(axis=1)
    gdf["inhab_calc"] = gdf["inhab_calc"].astype(int)
    gdf = gdf[["inhab_calc", "geometry"]]
    return gdf


@task
def calculate_stores_area(fp) -> geopandas.GeoDataFrame:
    osm = pyrosm.OSM(fp)
    buildings = osm.get_buildings()
    buildings = buildings[["id", "building", "geometry"]]
    shops = osm.get_pois(custom_filter={"shop": ["supermarket", "convenience"]})
    shops = shops[["id", "shop", "name", "geometry"]]
    buildings.to_crs(Albers_Equal_Area_Russia, inplace=True)
    shops.to_crs(Albers_Equal_Area_Russia, inplace=True)

    shops["store_area"] = shops.area
    store_areas = buildings.sjoin(shops)
    store_type_coef = dict(
        supermarket=SUPERMARKET_AREA_PART, convenience=CONVENIENCE_AREA_PART
    )
    cond1 = np.where(
        store_areas.area <= SA_STORE_AREA_LIMIT,
        store_areas.area,
        store_areas["shop"].map(store_type_coef) * store_areas.area,
    )
    cond2 = np.where(store_areas["store_area"] != 0, store_areas["store_area"], cond1)
    store_areas["store_area"] = cond2.astype("int")
    return store_areas


@task
def func(residents, shops):
    residents.to_crs(Albers_Equal_Area_Russia, inplace=True)
    residents["wlk_dist_zone"] = residents.buffer(WALKING_DISTANCE)

    gdf = shops.drop(columns=["index_right"]).sjoin(
        residents.set_geometry("wlk_dist_zone").rename(
            columns={"geometry": "house_geo"}
        )
    )
    gdf["dist"] = gdf.distance(gdf["house_geo"])
    gdf["attract"] = gdf["store_area"] / gdf["dist"] ** 2
    gdf = gdf.assign(totattract=gdf.groupby("index_right")["attract"].transform("sum"))
    gdf["marketshare"] = gdf["attract"] / gdf["totattract"]
    return gdf


@flow(name="Retail Gravitation")
def main():
    with open("data/cities.json", encoding="utf-8") as f:
        cities = json.load(f)

    get_osm_data(cities["krasnodar"])
    gdf1 = process_apartment_buildings_data(
        "data/myhouse_RU-CITY-016_points_matched.geojson"
    )
    gdf2 = calculate_stores_area(
        "data/krasnodar/krasnodar-shops-buildings.osm.pbf", Albers_Equal_Area_Russia
    )
    print(func(gdf1, gdf2, Albers_Equal_Area_Russia))


if __name__ == "__main__":
    main()
