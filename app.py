import json
import os
import warnings

import folium
import geopandas
import s3fs
import streamlit as st
from dotenv import load_dotenv
from streamlit_folium import st_folium

import src.huff_gravity_model as hgm

warnings.filterwarnings("ignore")

st.set_page_config(page_title="Retail Gravitation", page_icon="🛒")
st.title("🏪 Retail Gravitation Test App")

load_dotenv()
minio = s3fs.S3FileSystem(
    key=os.getenv("S3_KEY"),
    secret=os.getenv("S3_SECRET"),
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
)
BUCKET = "retail-gravitation"

with minio.open(f"{BUCKET}/cities.json") as f:
    cities = json.load(f)
city = st.selectbox("Выберите город:", list(cities)[:1])  # добавить другие города
area = st.number_input(
    "Введите площадь магазина", min_value=25, max_value=5000, value=100
)

osm_id = cities[city]["osm_id"]
url = f"https://polygons.openstreetmap.fr/get_geojson.py?id={osm_id}&params=0"
boundary = geopandas.read_file(url)
CENTER_START = [boundary.centroid.y.iloc[0], boundary.centroid.x.iloc[0]]
minx, miny, maxx, maxy = boundary.bounds.values[0]

m = folium.Map(location=CENTER_START, zoom_start=10)
folium.TileLayer("cartodb positron", show=False).add_to(m)
folium.GeoJson(
    boundary,
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
m.add_child(folium.ClickForMarker())
st_data = st_folium(m, width=725)

st_data


lat, lng = st_data["last_clicked"]["lat"], st_data["last_clicked"]["lng"]
if "markers" not in st.session_state:
    st.session_state["markers"] = {}

if "store_number" not in st.session_state:
    st.session_state["store_number"] = 1
store_name = f"Новый магазин {st.session_state['store_number']}"
if f"{lng} {lat}" not in st.session_state["markers"]:
    st.session_state["markers"][f"{lng} {lat}"] = (store_name, area)
    st.session_state["store_number"] += 1
st.write(st.session_state.markers)

gdf1 = geopandas.read_parquet(
    f"s3://{BUCKET}/{cities[city]['name']}-ab-residents.parquet", filesystem=minio
)
gdf2 = geopandas.read_parquet(
    f"s3://{BUCKET}/{cities[city]['name']}-shops.parquet", filesystem=minio
)


if st.button("Go", type="primary"):
    all_shops = hgm.add_user_shops(gdf2, st.session_state.markers)
    huff_model = hgm.huff_gravity_model(gdf1, all_shops)

    res = hgm.expected_number_of_consumers(huff_model)
    res
