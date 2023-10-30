import json

import folium
import geopandas
import streamlit as st
from streamlit_folium import st_folium
from src.obj_strg_tools import S3

st.set_page_config(page_title="Retail Gravitation", page_icon="🛒")
st.title("🏪 Retail Gravitation Test App")

minio = S3(bucket_name="retail-gravitation")
cities = json.load(minio.get_object_from_storage("cities.json"))
city = st.selectbox("Выберите город:", list(cities)[:1]) # скорректировать при добавлении других городов
area = st.number_input(
    "Введите площадь магазина", min_value=25, max_value=5000, value=100
)

osm_id = cities[city]['osm_id']
url = f"https://polygons.openstreetmap.fr/get_geojson.py?id={osm_id}&params=0"
boundary = geopandas.read_file(url)
CENTER_START = [boundary.centroid.y, boundary.centroid.x]

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

number_of_residents = geopandas.read_parquet(minio.get_object_from_storage("krasnodar-ab-residents.parquet"))
number_of_residents