import geopandas as gpd
from pathlib import Path

# raiz do projeto = pasta onde está o convert_geo.py
BASE_DIR = Path(__file__).resolve().parent

GEO_DIR = BASE_DIR / "assets" / "geo"

shp_path = GEO_DIR / "BR_UF_2023.shp"
geojson_path = GEO_DIR / "BR_UF_2023.geojson"

print("SHP:", shp_path)
print("Existe?", shp_path.exists())

gdf = gpd.read_file(shp_path)
gdf.to_file(geojson_path, driver="GeoJSON")

print("GeoJSON gerado em:", geojson_path)
