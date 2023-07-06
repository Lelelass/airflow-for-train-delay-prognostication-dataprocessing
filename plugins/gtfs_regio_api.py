import requests
from pathlib import Path
import datetime as dt 
from pytz import timezone
import time


AVAILABLE_RT_FEEDS = ('ServiceAlerts', 'TripUpdates', 'VehiclePositions')

class RegioFetcher():
    def __init__(self, operator:str, api_key:str, api_url:str):
        self.operator = operator
        self.api_key = api_key
        self.api_url = api_url

    def get_static(self, save_path:Path)->str:
        """Query the GTFS Regional api for data on a date of choice.
        Downloads raw static gtfs .zip in a target folder and returns
        the name of the saved file"""

        # Mark static files with a time stamp in filename
        day_str_stamp = dt.datetime.now(timezone('Europe/Stockholm')).strftime("%Y-%m-%d %H:%M")

        file_export_name = f"{self.operator}-{day_str_stamp}.zip"
        url = self.api_url
        query = f"/{self.operator}/{self.operator}.zip?key={self.api_key}"
        response = requests.get(url+query)

        with open(save_path.joinpath(file_export_name), 'wb') as f:
            f.write(response.content)
        
        print(f"static data saved at : {save_path.joinpath(file_export_name)}")


    def __repr__(self) -> str:
        return f"KoDaFetcher object for operator {self.operator}, static data fetcher"


class RegioFetcherRt(RegioFetcher):
    def __init__(self, operator:str, feed:str, api_key:str, api_url:str):
        super().__init__(operator, api_key, api_url)
        if feed in AVAILABLE_RT_FEEDS:
            self.feed = feed
        else:
            raise ValueError(f"{type} is not a valid dataset, should be one of {AVAILABLE_RT_FEEDS} ")

    def get_realtime(self, save_path:Path)->str:
        """Query the GTFS Regional api for data on a date and feed of choice.
        As the data needs to be prepared, checks the request status at 40 sec interval.
        When requests status code is 200, downloads raw real-time gtfs .zip in a target folder and returns
        the name of the saved file"""
        
        today_dt = dt.datetime.now(timezone('Europe/Stockholm'))
        date = today_dt.strftime("%Y-%m-%d %H:%M")

        file_export_name = f"{self.operator}-{self.feed}-{date}.pb"
        url = self.api_url
        query = f"/{self.operator}/{self.feed}.pb?key={self.api_key}"
        headers = {'Accept-Encoding': 'gzip'}
        while True:

            response = requests.get(url+query, headers=headers)
            print(f"api response status code: {response.status_code}")

            if response.status_code == 202:
                time.sleep(40)

            elif response.status_code == 200:       
                with open(save_path.joinpath(file_export_name), 'wb') as f:
                    f.write(response.content)
                break
            else:
                raise ValueError(f"Request not configured right, check field operator : {self.operator}")


    def __repr__(self) -> str:
        return f"KoDaFetcher object for operator {self.operator} and feed {self.feed}, real-time data fetcher"