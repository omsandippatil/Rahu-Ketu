import os
import json
import zipfile
import requests
import shutil
from io import BytesIO
from pathlib import Path


class FireDataFetcher:
    def __init__(self, config_file="config.json"):
        self.config = self._load_config(config_file)
        self._setup_directories()

    def _load_config(self, config_file):
        with open(config_file, 'r') as f:
            return json.load(f)

    def _setup_directories(self):
        for dir_path in self.config["directories"].values():
            Path(dir_path).mkdir(parents=True, exist_ok=True)

    def _download_zip(self, url):
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        return response.content

    def _extract_zip(self, zip_content, extract_path):
        with zipfile.ZipFile(BytesIO(zip_content)) as zip_ref:
            zip_ref.extractall(extract_path)

    def _find_and_move_country_file(self, year, temp_path, target_path):
        country = self.config["target_country"]
        pattern = self.config["file_pattern"].format(year=year, country=country)
        
        for root, dirs, files in os.walk(temp_path):
            for file in files:
                if file == pattern:
                    src = os.path.join(root, file)
                    dest = os.path.join(target_path, f"{year}", pattern)
                    Path(dest).parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(src, dest)
                    return True
        return False

    def _cleanup_temp(self, temp_path):
        if self.config["cleanup_temp"] and os.path.exists(temp_path):
            shutil.rmtree(temp_path)

    def fetch_year_data(self, year):
        base_url = self.config["base_url"]
        suffix = self.config["url_suffix"]
        url = f"{base_url}{year}{suffix}"
        
        temp_path = os.path.join(self.config["directories"]["temp_extract"], str(year))
        target_path = self.config["directories"]["fire_data"]
        
        try:
            print(f"Downloading {year} data...")
            zip_content = self._download_zip(url)
            
            print(f"Extracting {year} data...")
            self._extract_zip(zip_content, temp_path)
            
            if self._find_and_move_country_file(year, temp_path, target_path):
                print(f"Successfully processed {year} data")
            else:
                print(f"Warning: No {self.config['target_country']} file found for {year}")
            
            self._cleanup_temp(temp_path)
            
        except requests.RequestException as e:
            print(f"Download error for {year}: {e}")
        except zipfile.BadZipFile:
            print(f"Invalid ZIP file for {year}")
        except Exception as e:
            print(f"Unexpected error for {year}: {e}")

    def fetch_all_data(self):
        start_year = self.config["years"]["start"]
        end_year = self.config["years"]["end"]
        
        print(f"Fetching fire data from {start_year} to {end_year}")
        
        for year in range(start_year, end_year + 1):
            self.fetch_year_data(year)
        
        print("Data fetching completed")

    def list_downloaded_files(self):
        fire_data_path = self.config["directories"]["fire_data"]
        files = []
        
        for root, dirs, filenames in os.walk(fire_data_path):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        
        return sorted(files)


def main():
    fetcher = FireDataFetcher()
    
    choice = input("Enter 'all' to fetch all years, or specific year (e.g., 2023): ").strip()
    
    if choice.lower() == 'all':
        fetcher.fetch_all_data()
    else:
        try:
            year = int(choice)
            fetcher.fetch_year_data(year)
        except ValueError:
            print("Invalid input. Please enter 'all' or a valid year.")
    
    downloaded_files = fetcher.list_downloaded_files()
    print(f"\nDownloaded files ({len(downloaded_files)}):")
    for file in downloaded_files:
        print(f"  {file}")


if __name__ == "__main__":
    main()
