from collections import namedtuple

City = namedtuple("City", ["name", "osm_id", "region"])
# проверить id городов
cities = [
        City("krasnodar", "r7373058", "south"),
        City("novorossiysk", "r1477110", "south"),
        City("armavir", "r3476238", "south"),
        City("rostov", "r1285772", "south"),
    ]
