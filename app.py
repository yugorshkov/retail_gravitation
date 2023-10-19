import folium
import streamlit as st
from streamlit_folium import st_folium

st.write('Retail Gravitation Test App')

m = folium.Map(location=[45.035470, 38.975313], zoom_start=13)
m.add_child(folium.ClickForMarker())
st_data = st_folium(m, width=725)
st_data


lat, lng = st_data["last_clicked"]['lat'], st_data["last_clicked"]['lat']
if 'markers' not in st.session_state:
    st.session_state['markers'] = []

st.session_state['markers'].append([lat, lng])
st.write(st.session_state.markers)

