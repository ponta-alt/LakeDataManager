import itertools
import os
import zipfile
import requests
import ssl
from datetime import datetime
from typing import NamedTuple
from bs4 import BeautifulSoup
from pathlib import Path
from geopandas import gpd
import urllib3
from requests import adapters
from shapely.geometry.linestring import LineString
import re
import pykakasi
from shapely.ops import linemerge, polygonize, unary_union
from shapely.geometry import Polygon

def main():
    url = 'https://www.gsi.go.jp/kankyochiri/lakedatalist.html'

    parent_folder_name = 'lake-data'
    zip_folder_relative_path = os.path.join(parent_folder_name, 'zip')
    unzip_folder_relative_path = os.path.join(parent_folder_name, 'shp')
    geojson_folder_relative_path = os.path.join(parent_folder_name, 'geojson')

    ldm = LakeDataManager(url, zip_folder_relative_path, unzip_folder_relative_path, geojson_folder_relative_path)
    lake_data_list_to_download = ldm.fetch_lake_data_list()[0:3]
    ldm.download_zip(lake_data_list_to_download)
    ldm.unzip_shp()
    ldm.shp_to_geojson()


class LakeData(NamedTuple):
    name: str
    download_url: str
    is_mixed: bool


class LakeDataManager:
    def __init__(self, url, zip_folder_relative_path, unzip_folder_relative_path, geojson_folder_relative_path):
        self.url = url
        self.zip_folder_relative_path = zip_folder_relative_path
        self.unzip_folder_relative_path = unzip_folder_relative_path
        self.geojson_folder_relative_path = geojson_folder_relative_path

    def fetch_lake_data_list(self):
        print(f'{datetime.now().replace(microsecond=0)} {self.url} 接続中')
        response = CustomHttpAdapter.get_legacy_session().get(self.url)
        soup = BeautifulSoup(response.content, 'html.parser')
        lake_data_list = []
        for tr in soup.find('tbody').find_all('tr')[2:]:
            td_array = tr.find_all('td')
            names = re.sub(r'[\t\r\n　]', '', td_array[0].text).split('・')
            data_url = td_array[5].find('a', href=True).get('href')
            is_mixed = len(names) != 1  # 複数の湖や沼の名前が含まれているかどうか
            for name in names:
                lake_data_list.append(LakeData(name, data_url, is_mixed))
        return lake_data_list

    def download_zip(self, lake_data_list_to_download):
        if len(lake_data_list_to_download) == 0:
            print('ダウンロードなし')
            return
        print('ダウンロードリスト**********************')
        for i, lake_data in enumerate(lake_data_list_to_download):
            print(lake_data.name)
        print('*************************************')

        os.makedirs(self.zip_folder_relative_path, exist_ok=True)
        for i, lake_data in enumerate(lake_data_list_to_download):
            print(f'{datetime.now().replace(microsecond=0)} {lake_data.name} ダウンロード中')
            response = CustomHttpAdapter.get_legacy_session().get(lake_data.download_url)
            etc = '_etc' if lake_data.is_mixed else ''
            zip_file_path = os.path.join(self.zip_folder_relative_path, lake_data.name+etc+'.zip')
            with open(zip_file_path, 'wb') as file:
                file.write(response.content)
            print(f'{datetime.now().replace(microsecond=0)} {lake_data.name} ダウンロード完了 ({i+1}/{len(lake_data_list_to_download)})')

    def unzip_shp(self):
        ka_ka_si = pykakasi.kakasi()
        for zip_lake_name in os.listdir(self.zip_folder_relative_path):
            is_contains_other_files = '_etc' in zip_lake_name
            lake_name = os.path.splitext(zip_lake_name)[0].replace('_etc', '')
            unzip_folder_name_relative_path = os.path.join(self.unzip_folder_relative_path, lake_name)
            os.makedirs(unzip_folder_name_relative_path, exist_ok=True)
            zip_file_relative_path = os.path.join(self.zip_folder_relative_path, zip_lake_name)
            shp_roman_lake_name = 'シェープファイル_' + ka_ka_si.convert(lake_name)[0]['passport']
            with zipfile.ZipFile(str(zip_file_relative_path), 'r') as zip_ref:
                for member in zip_ref.infolist():
                    file_name_path = member.filename.encode('cp437').decode('cp932', errors='replace')
                    is_lake_line_data = '湖岸線ラインデータ' in file_name_path
                    is_name_in_path = lake_name in file_name_path or shp_roman_lake_name in file_name_path
                    is_not_folder = not member.is_dir()
                    if is_lake_line_data and is_not_folder and (is_name_in_path or not is_contains_other_files):
                        unzip_file_name_relative_path = os.path.join(str(unzip_folder_name_relative_path), Path(file_name_path).name)
                        with zip_ref.open(member) as source, open(unzip_file_name_relative_path, 'wb') as target:
                            target.write(source.read())

    def shp_to_geojson(self):
        print(f'{datetime.now().replace(microsecond=0)} GeoJson 出力中')
        unzip_folder_path = Path(self.unzip_folder_relative_path).resolve()
        shp_folder_path_list = [d for d in unzip_folder_path.iterdir() if d.is_dir()]
        geojson_line_string_folder_path = os.path.join(self.geojson_folder_relative_path, 'multi_line_string')
        geojson_polygon_folder_path = os.path.join(self.geojson_folder_relative_path, 'polygon')
        os.makedirs(geojson_line_string_folder_path, exist_ok=True)
        os.makedirs(geojson_polygon_folder_path, exist_ok=True)
        for shp_folder_path in shp_folder_path_list:
            shp_file_path_list = [f for f in Path(shp_folder_path).rglob('*') if f.suffix == '.shp']
            for shp_file_path in shp_file_path_list:
                gdf_multi_line_string = self._make_line_string(shp_file_path)
                polygons = [
                    Polygon(geom)
                    for geom in gdf_multi_line_string.geometry[0].geoms
                    if isinstance(geom, LineString)
                       and len(geom.coords) >= 4
                       and geom.coords[0] == geom.coords[-1]
                ]
                gdf_polygon = gpd.GeoDataFrame(geometry=polygons, crs=gdf_multi_line_string.crs)
                name = os.path.basename(shp_folder_path)
                geojson_line_string_file_path = os.path.join(geojson_line_string_folder_path, name+'.geojson')
                geojson_polygon_file_path = os.path.join(geojson_polygon_folder_path, name+'.geojson')
                if len(shp_file_path_list) != 1:
                    file_name = os.path.splitext(shp_file_path)[0]
                    geojson_line_string_file_path = os.path.splitext(geojson_line_string_file_path)[0]
                    geojson_polygon_file_path = os.path.splitext(geojson_polygon_folder_path)[0]
                    os.makedirs(geojson_line_string_file_path, exist_ok=True)
                    os.makedirs(geojson_polygon_file_path, exist_ok=True)
                    geojson_line_string_file_path += '/' + os.path.basename(file_name) + '.geojson'
                    geojson_polygon_file_path += '/' + os.path.basename(file_name) + '.geojson'

                gdf_multi_line_string.to_file(Path(geojson_line_string_file_path), driver='GeoJSON')
                gdf_polygon.to_file(Path(geojson_polygon_file_path), driver='GeoJSON')

        print(f'{datetime.now().replace(microsecond=0)} GeoJson 出力完了')

    @staticmethod
    def _make_line_string(shp_file_path):
        gdf = gpd.read_file(shp_file_path)
        gdf.to_crs(epsg=4326, inplace=True)
        self_contained_coordinates = []
        not_self_contained_coordinates = []
        for feature in gdf.geometry:
            start_point = feature.coords[0]
            end_point = feature.coords[-1]
            distance = LineString([start_point, end_point]).length
            coords = [list(coord) for coord in feature.coords]
            if len(feature.coords) < 4:
                continue
            if distance < 0.0003:
                self_contained_coordinates.append(coords)
            else:
                not_self_contained_coordinates.append(coords)

        result_coordinates = self_contained_coordinates
        unified_coordinates = []
        if not_self_contained_coordinates:
            unified_coordinates.append(not_self_contained_coordinates.pop(0))
            while len(not_self_contained_coordinates) != 0:
                min_dist_squared = float('inf')
                next_coords_index = int
                start_or_end_point = int
                for i, coords in enumerate(not_self_contained_coordinates):
                    point = [coords[0], coords[-1]]
                    for k, p in enumerate(point):
                        dist_squared = (unified_coordinates[-1][-1][0] - p[0]) ** 2 + (
                                unified_coordinates[-1][-1][1] - p[1]) ** 2
                        if dist_squared < min_dist_squared:
                            min_dist_squared = dist_squared
                            next_coords_index = i
                            start_or_end_point = k
                next_coords = not_self_contained_coordinates.pop(next_coords_index)
                if start_or_end_point == 1:
                    next_coords.reverse()
                unified_coordinates.append(next_coords)
            unified_coordinates[-1].append(unified_coordinates[0][0])
            result_coordinates = [list(itertools.chain.from_iterable(unified_coordinates))]
            for self_contained_coord in self_contained_coordinates:
                result_coordinates.append(self_contained_coord)
        feature_multi_line_string = [{'type': 'Feature', 'properties': {'ID': 1},
                                      'geometry': {'type': 'MultiLineString', 'coordinates': result_coordinates}}]
        gdf_multi_line_string = gpd.GeoDataFrame.from_features(features=feature_multi_line_string)
        gdf_multi_line_string.set_crs(epsg=4326, inplace=True)
        return gdf_multi_line_string


class CustomHttpAdapter (adapters.HTTPAdapter):  # 古い再ネゴシエーションエラーを消すため
    @staticmethod
    def get_legacy_session():
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.options |= ssl.ALERT_DESCRIPTION_NO_RENEGOTIATION  # これなしだとエラー出る
        session = requests.session()
        session.mount('https://', CustomHttpAdapter(ctx))
        return session

    def __init__(self, ssl_context=None):
        self.poolmanager = None
        self.ssl_context = ssl_context
        super().__init__()

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)


if __name__ == '__main__':
    main()
