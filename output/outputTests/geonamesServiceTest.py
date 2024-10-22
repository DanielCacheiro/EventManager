import unittest
from unittest.mock import patch, Mock
from geonamesService import (
    get_countries, get_states, get_provinces, obtener_codigos_postales,
    get_coords, get_geonames, get_coords_by_city, calcular_distancia, filtrar_coordenadas
)

class TestGeonamesService(unittest.TestCase):

    @patch('code.output.geonamesService.requests.get')
    def get_countries_returns_country_list(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'geonames': [
                {'countryCode': 'US', 'countryName': 'United States'},
                {'countryCode': 'CA', 'countryName': 'Canada'}
            ]
        }
        mock_get.return_value = mock_response

        result = get_countries()
        self.assertEqual(result, {'US': 'United States', 'CA': 'Canada'})

    @patch('code.output.geonamesService.requests.get')
    def get_states_returns_state_list(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'geonames': [
                {'geonameId': 1},
                {'geonameId': 2}
            ]
        }
        mock_get.return_value = mock_response

        result = get_states(123)
        self.assertEqual(result, {1, 2})

    @patch('code.output.geonamesService.requests.get')
    def get_provinces_returns_province_list(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'geonames': [
                {'name': 'Province1'},
                {'name': 'Province2'}
            ]
        }
        mock_get.return_value = mock_response

        result = get_provinces(123)
        self.assertEqual(result, [{'name': 'Province1'}, {'name': 'Province2'}])

    @patch('code.output.geonamesService.requests.get')
    def obtener_codigos_postales_returns_postal_codes(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'postalCodes': [
                {'lat': 1.0, 'lng': 1.0},
                {'lat': 2.0, 'lng': 2.0}
            ]
        }
        mock_get.return_value = mock_response

        result = obtener_codigos_postales('Province1')
        self.assertEqual(result, [(1.0, 1.0), (2.0, 2.0)])

    @patch('code.output.geonamesService.requests.get')
    def get_coords_returns_coordinates(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'geonames': [
                {'lat': 1.0, 'lng': 1.0}
            ]
        }
        mock_get.return_value = mock_response

        result = get_coords('Country1')
        self.assertEqual(result, [1.0, 1.0])

    @patch('code.output.geonamesService.requests.get')
    def get_geonames_returns_geoname_id(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'geonames': [
                {'geonameId': 123}
            ]
        }
        mock_get.return_value = mock_response

        result = get_geonames('Country1')
        self.assertEqual(result, 123)

    @patch('code.output.geonamesService.requests.get')
    def get_coords_by_city_returns_coordinates(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'geonames': [
                {'lat': 1.0, 'lng': 1.0}
            ]
        }
        mock_get.return_value = mock_response

        result = get_coords_by_city('City1')
        self.assertEqual(result, [1.0, 1.0])

    def calcular_distancia_returns_correct_distance(self):
        result = calcular_distancia(0, 0, 3, 4)
        self.assertEqual(result, 5.0)

    def filtrar_coordenadas_filters_correctly(self):
        coords = [(0, 0), (0.05, 0.05), (1, 1)]
        result = filtrar_coordenadas(coords, umbral=0.1)
        self.assertEqual(result, [(0, 0), (1, 1)])

if __name__ == '__main__':
    unittest.main()