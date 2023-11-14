import json
import os

import folium
import geopandas
import pandas as pd
import s3fs
import streamlit as st
from dotenv import load_dotenv
from geolib import geohash as gh
from geopandas import GeoDataFrame
from streamlit_folium import st_folium

import src.huff_gravity_model as hgm

st.set_page_config(page_title="Retail Gravitation", page_icon="🛒", layout="wide")
st.title("🏪 Retail Gravitation Test App")

BUCKET = "retail-gravitation"


@st.cache_data
def get_city_bounds(osm_id: str) -> GeoDataFrame:
    url = f"https://polygons.openstreetmap.fr/get_geojson.py?id={osm_id}&params=0"
    bounds = geopandas.read_file(url)
    return bounds


@st.cache_data
def get_city_data(city: str, data_type: str, _s3: s3fs.S3FileSystem) -> GeoDataFrame:
    gdf = geopandas.read_parquet(
        f"s3://{BUCKET}/{city}-{data_type}.parquet", filesystem=_s3
    )
    return gdf


def main():
    if "markers" not in st.session_state:
        st.session_state["markers"] = {}
    if "store_number" not in st.session_state:
        st.session_state["store_number"] = 1

    with open("cities.json") as f:
        cities = json.load(f)
    city = st.selectbox("Выберите город:", cities)
    bounds = get_city_bounds(cities[city]["osm_id"])
    center = [bounds.centroid.y.iloc[0], bounds.centroid.x.iloc[0]]

    m = folium.Map(location=center, zoom_start=11)

    fg = folium.FeatureGroup(name="Markers")
    for marker in st.session_state["markers"]:
        lat, lng = gh.decode(marker)
        name = st.session_state["markers"][marker]["name"]
        fg.add_child(folium.Marker(location=[lat, lng], popup=name))

    folium.TileLayer("cartodb positron", show=False).add_to(m)
    folium.GeoJson(
        bounds,
        name="границы города",
        style_function=lambda feature: {
            "color": "black",  # цвет границ объекта
            "weight": 2,  # толщина линии
            # "dashArray": "5, 5", # тип линии пунктир
            "fillColor": "green",  # цвет заливки объекта
            "fill_opacity": 0.1,  # прозрачность
            "fill": True,  # будет ли заливка
        },
    ).add_to(m)
    folium.LayerControl().add_to(m)

    col1, col2 = st.columns(2)
    with col1:
        out = st_folium(m, width=725, feature_group_to_add=fg)
        # out

    with col2:
        df = pd.DataFrame(
            st.session_state["markers"].values(), index=st.session_state["markers"]
        )
        config = {
            "name": "Название магазина",
            "area": st.column_config.NumberColumn(
                "Площадь, м2", min_value=25, max_value=1000
            ),
        }
        st.data_editor(df, column_config=config, use_container_width=True, key="user_input")
        st.session_state["user_input"]

    store_name = f"Новый магазин {st.session_state['store_number']}"
    geohash = gh.encode(*out["last_clicked"].values(), 9)
    if geohash not in st.session_state["markers"]:
        st.session_state["markers"][geohash] = {"name": store_name, "area": 100}
        st.session_state["store_number"] += 1
        st.rerun()
    st.write(st.session_state.markers)

    load_dotenv()
    minio = s3fs.S3FileSystem(
        key=os.getenv("S3_KEY"),
        secret=os.getenv("S3_SECRET"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    population = get_city_data(cities[city]["name"], "ab-residents", minio)
    shops = get_city_data(cities[city]["name"], "shops", minio)

    if st.button("Go", type="primary"):
        all_shops = hgm.add_user_shops(shops, st.session_state.markers)
        huff_model = hgm.huff_gravity_model(population, all_shops)

        res = hgm.expected_number_of_consumers(huff_model)
        res


if __name__ == "__main__":
    main()
