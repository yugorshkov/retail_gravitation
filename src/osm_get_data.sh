#!/bin/bash
region=$1
local_prefix=$2
city_name=$3
city_id=$4

echo Download OpenStreetMap data for ${region^} Federal District...
url="http://download.geofabrik.de/russia/$region-fed-district-latest.osm.pbf"
mkdir -p $local_prefix
wget $url -P $local_prefix -N

echo Creating extract for ${city_name^} city...
region_osm_data=$local_prefix/$region-fed-district-latest.osm.pbf
city_prefix=$local_prefix/$city_name
city_boundary=$city_prefix/$city_name-boundary.osm
city_osm_data=$city_prefix/$city_name.osm.pbf
mkdir -p $city_prefix
osmium getid -r -t $region_osm_data r$city_id -o $city_boundary
osmium extract -p $city_boundary $region_osm_data -o $city_osm_data

echo Filtering ${city_name^} OSM data by tags: shop, building...
osmium tags-filter $city_osm_data \
    shop \
    building \
    -o $city_prefix/$city_name-shops-buildings.osm.pbf
