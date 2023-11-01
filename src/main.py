import json
import os

import geopandas
import numpy as np
import pyrosm
import s3fs
from dotenv import load_dotenv
from geopandas import GeoDataFrame
from prefect import flow, task

import constants as c
from crs import Albers_Equal_Area_Russia


@task(retries=3, retry_delay_seconds=5)
def get_osm_data(city: dict, local_prefix: str = "data") -> str:
    """Загружаем данные OpenStreetMap, обрезаем дамп по границе указанного города
    и оставляем только те объекты OSM, которые нас интересуют."""
    os.system(
        f"src/osm_get_data.sh {city['region']} {local_prefix} {city['name']} {city['osm_id']}"
    )
    return f"{local_prefix}/{city['name']}/{city['name']}-filtered.osm.pbf"


@task(log_prints=True)
def process_apartment_buildings_data(filename: str) -> GeoDataFrame:
    """Обрабатываем информацию по жилым многоквартирным домам для получения
    ориентировочной численности жителей."""
    gdf = geopandas.read_file(filename)
    gdf = gdf[["HOUSE_ID", "RMC", "RMC_LIVE", "INHAB", "geometry"]]

    gdf = gdf.replace({"": np.nan})
    for col in ["RMC", "RMC_LIVE"]:
        gdf[col] = gdf[col].apply(lambda x: str(x).replace(" ", ""))
    gdf = gdf.astype({"RMC": float, "RMC_LIVE": float, "INHAB": float})

    mask = ((gdf["RMC_LIVE"] > 1000) & (gdf["RMC_LIVE"] > gdf["RMC"])) | gdf[
        "RMC_LIVE"
    ].isna()
    gdf["RMC_LIVE"] = np.where(mask, gdf["RMC"], gdf["RMC_LIVE"])

    avg_residents_in_apartment = (gdf.INHAB / gdf.RMC_LIVE).mean().round(2)
    print(f"Среднее кол-во жителей в одной квартире - {avg_residents_in_apartment}")
    gdf["inhab_calc"] = gdf["RMC_LIVE"] * avg_residents_in_apartment
    residents_in_house = gdf["inhab_calc"].mean()
    gdf["inhab_calc"] = gdf["inhab_calc"].fillna(residents_in_house)
    gdf["INHAB"] = gdf[["INHAB", "inhab_calc"]].max(axis=1)
    gdf["INHAB"] = gdf["INHAB"].astype(int)
    gdf = gdf[["HOUSE_ID", "INHAB", "geometry"]]

    gdf.to_crs(Albers_Equal_Area_Russia, inplace=True)
    gdf["wlk_dist_zone"] = gdf.buffer(c.WALKING_DISTANCE)
    gdf = gdf.set_geometry("wlk_dist_zone")
    gdf.rename(columns={"geometry": "coords"}, inplace=True)
    return gdf


@task
def calculate_stores_area(filename: str) -> GeoDataFrame:
    """Вычисляем площадь каждого магазина города, как часть площади здания в котором
    он располагается. Логика определения площади следующая:
    1. Если в данных OSM геометрия магазина представлена полигоном, считаем, что
        магазин занимает всю влощадь полигона.
    2. Для магазинов, которые представлены точкой, находим геометрию здания, которая
        включает в себя эту точку. Вычисляем площадь здания.
    3. Если площадь здания небольшая (регулируется параметром SUPERMARKET_AREA_PART),
        принимаем площадь магазина равной площади здания.
    4. Для оставшихся магазинов принимаем площадь равной площади здания с поправкой на
        коэффициент доли площади магазина в здании:
        - SUPERMARKET_AREA_PART для супермаркетов;
        - CONVENIENCE_AREA_PART для небольших магазинов у дома."""
    osm = pyrosm.OSM(filename)
    buildings = osm.get_buildings()
    buildings = buildings[["id", "geometry"]]
    shops = osm.get_pois(custom_filter={"shop": ["supermarket", "convenience"]})
    shops = shops[["id", "shop", "name", "geometry"]]
    buildings.to_crs(Albers_Equal_Area_Russia, inplace=True)
    shops.to_crs(Albers_Equal_Area_Russia, inplace=True)

    shops["store_area"] = shops.area
    store_areas = buildings.sjoin(shops)
    store_areas.drop("index_right", axis=1, inplace=True)
    store_type_coef = dict(
        supermarket=c.SUPERMARKET_AREA_PART, convenience=c.CONVENIENCE_AREA_PART
    )
    cond1 = np.where(
        store_areas.area <= c.SA_STORE_AREA_LIMIT,
        store_areas.area,
        store_areas["shop"].map(store_type_coef) * store_areas.area,
    )
    cond2 = np.where(store_areas["store_area"] != 0, store_areas["store_area"], cond1)
    store_areas["store_area"] = cond2.astype("int")
    store_areas.rename(
        columns={"id_left": "building_id", "id_right": "shop_id"}, inplace=True
    )
    return store_areas


@flow(name="Retail Gravitation")
def main():
    """Основной поток получения данных из различных источников, последующей обработки
    и загрузки в хранилище."""
    load_dotenv()
    minio = s3fs.S3FileSystem(
        key=os.getenv("S3_KEY"),
        secret=os.getenv("S3_SECRET"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    BUCKET = "retail-gravitation"

    with minio.open(f"{BUCKET}/cities.json") as f:
        cities = json.load(f)
    city = cities["Краснодар"]
    osm_data = get_osm_data(city)
    with minio.open(f"{BUCKET}/{city['apartment_buildings_information']}") as f:
        number_of_residents = process_apartment_buildings_data(f)
    city_shops_data = calculate_stores_area(osm_data)

    number_of_residents.to_parquet(
        f"s3://{BUCKET}/{city['name']}-ab-residents.parquet", filesystem=minio
    )
    city_shops_data.to_parquet(
        f"s3://{BUCKET}/{city['name']}-shops.parquet", filesystem=minio
    )


if __name__ == "__main__":
    main.serve(name="test-dep")
