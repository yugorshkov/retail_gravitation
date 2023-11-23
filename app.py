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
st.title("🏪 Retail Gravitation")
st.markdown("Сколько покупателей посетит Ваш магазин?")

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


@st.cache_resource
def get_remote_fs_session(
    s3_key=os.getenv("S3_KEY"),
    s3_secret=os.getenv("S3_SECRET"),
    s3_endpoint_url=os.getenv("S3_ENDPOINT_URL"),
):
    s3 = s3fs.S3FileSystem(
        key=s3_key,
        secret=s3_secret,
        endpoint_url=s3_endpoint_url,
    )
    return s3


def update_user_data():
    user_markers = list(st.session_state["markers"])
    edited_rows = st.session_state["user_input"]["edited_rows"]
    for row in edited_rows:
        k = user_markers[int(row)]
        st.session_state["markers"][k].update(edited_rows[row])


def main():
    if "markers" not in st.session_state:
        st.session_state["markers"] = {}
    if "store_number" not in st.session_state:
        st.session_state["store_number"] = 1
    load_dotenv()
    minio = get_remote_fs_session()
    with open("cities.json") as f:
        cities = json.load(f)
    col1, *unused_cols = st.columns(4)
    city = col1.selectbox("Выберите город:", ["Краснодар"])  # пока только Краснодаре
    bounds = get_city_bounds(cities[city]["osm_id"])
    center = [bounds.centroid.y.iloc[0], bounds.centroid.x.iloc[0]]

    m = folium.Map(location=center, zoom_start=11)
    fg = folium.FeatureGroup(name="Markers")
    for marker in st.session_state["markers"]:
        lat, lng = gh.decode(marker)
        name = st.session_state["markers"][marker]["name"]
        fg.add_child(folium.Marker(location=[lat, lng], popup=name, tooltip=name))
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

    col3, col4 = st.columns(2)
    with col3:
        out = st_folium(m, width=725, feature_group_to_add=fg)
    with col4:
        df = pd.DataFrame(st.session_state["markers"].values())
        config = {
            "name": st.column_config.TextColumn("Название магазина", max_chars=20),
            "store_area": st.column_config.NumberColumn(
                "Площадь, м2", min_value=25, max_value=1000
            ),
        }
        result = st.data_editor(
            df,
            use_container_width=True,
            hide_index=False,
            column_order=["name", "store_area"],
            column_config=config,
            key="user_input",
            on_change=update_user_data,
        )

        population = get_city_data(cities[city]["name"], "ab-residents", minio)
        shops = get_city_data(cities[city]["name"], "shops", minio)
        if st.button("Go"):
            all_shops = hgm.add_user_shops(shops, result)
            huff_model = hgm.huff_gravity_model(population, all_shops)

            res = hgm.expected_number_of_consumers(huff_model)
            res
    if out["last_clicked"]:
        lat, lng = out["last_clicked"]["lat"], out["last_clicked"]["lng"]
        geohash = gh.encode(lat, lng, 9)
        store_name = f"Новый магазин {st.session_state['store_number']}"
        if geohash not in st.session_state["markers"]:
            st.session_state["markers"][geohash] = {
                "name": store_name,
                "store_area": 100,
                "latitude": lat,
                "longitude": lng,
            }
            st.session_state["store_number"] += 1
            st.rerun()


if __name__ == "__main__":
    main()
