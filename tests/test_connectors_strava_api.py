"""
Connectors Tests : Strava API

Author: Jairus Martinez
Date: 1/07/2024
"""
import os
import unittest
from unittest.mock import MagicMock, patch
import yaml
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parentdir) 
from src.commons.connectors import StravaAPIConnector

class TestStravaAPIConnector(unittest.TestCase):
    """Test suite for StravaAPI Connector"""
    def setUp(self):
        self.connector = StravaAPIConnector(
            strava_auth_url='auth_url',
            strava_activities_url='activities_url',
            strava_payload={
                'client_id': 'id', 
                'client_secret': 'secret', 
                'refresh_token': 'token', 
                'grant_type': 'type', 
                'f': 'format'}
        )

    @patch('src.commons.connectors.requests.post')
    def test_get_header_success(self, mock_post):
        """
        Test for the correct header return and correct call for get_header()
        """
        mock_response = mock_post.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {'access_token': 'dummy_token'}
        
        header = self.connector.get_header()

        mock_post.assert_called_once_with('auth_url', 
                                          data=self.connector.strava_payload, 
                                          verify=False, 
                                          timeout=(10, 10))
        self.assertEqual(header, {'Authorization': 'Bearer dummy_token'})

    @patch('src.commons.connectors.requests.post')
    def test_get_header_failure(self, mock_post):
        """
        Test to simulate error code 400
        """
        mock_response = mock_post.return_value
        mock_response.return_value.status_code = 400  
        header = self.connector.get_header()

        mock_post.assert_called_once_with('auth_url', 
                                          data=self.connector.strava_payload, 
                                          verify=False, 
                                          timeout=(10, 10))
        self.assertIsNone(header)

    @patch('src.commons.connectors.requests.get')
    def test_get_dataset(self, mock_get):
        """
        Tests for the correct call and return of get_dataset()/
        """
        mock_dataset = mock_get.return_value
        mock_dataset.json.return_value = [{'col_names': 'values'}]
        mock_header = {'Authorization': 'Bearer dummy_token'}

        dataset = self.connector.get_dataset(actv_per_page=10, request_page_number=1, header=mock_header)

        mock_get.assert_called_once_with('activities_url', 
                                         headers=mock_header, 
                                         params={'per_page': 10, 'page': 1}, 
                                         timeout=(10, 10))
        self.assertEqual(dataset, [{'col_names': 'values'}])

if __name__ == '__main__':
    unittest.main()