import requests
import json
import logging
import time
import os

class DataGouvFrScrapper:
    def __init__(self, dataset_id: str, timeout: int, max_retries: int, sleep_between_retries: float):
        self.dataset_id = dataset_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.sleep_between_retries = sleep_between_retries
        self.infos = None
        self.resources = []

    def get_api_url(self) -> str:
        return f"https://www.data.gouv.fr/api/1/datasets/{self.dataset_id}/"

    def _fetch_dataset_infos(self, force = False) -> None:
        if not force and self.infos is not None:
            return
        
        url = self.get_api_url()
        attempt = 0
        logging.info(f"Fetching dataset infos from {url}")
        while attempt < self.max_retries:
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                self.infos = response.json()
                return
            except requests.RequestException as e:
                logging.error(f"Attempt {attempt + 1} failed: {e}")
                attempt += 1
                if attempt < self.max_retries:
                    time.sleep(self.sleep_between_retries)
        raise Exception("Max retries exceeded")
    
    def fetch_resources(self, force = False) -> list:
        if not force and len(self.resources) > 0:
            return self.resources
        
        if self.infos is None:
            self._fetch_dataset_infos()

        self.resources = self.infos.get("resources", [])
        return self.resources
    
    def download_resources(self, download_path: str, force = False, filter=None) -> None:
        resources = self.fetch_resources(force=force)
        os.makedirs(download_path, exist_ok=True)

        for resource in resources:
            resource_url = resource.get("url")

            if filter and not filter(resource_url):
                logging.info(f"Resource \"{resource_url}\" filtered out, skipping download.")
                continue

            filename = os.path.join(download_path, resource_url.split("/")[-1])
            if os.path.exists(filename):
                logging.info(f"File \"{filename}\" already exists, skipping download.")
                continue

            logging.info(f"Fetching resource \"{resource_url}\"")
            attempt = 0
            while attempt < self.max_retries:
                try:
                    response = requests.get(resource_url, timeout=self.timeout)
                    response.raise_for_status()
                    with open(filename, "wb") as f:
                        f.write(response.content)
                    break
                except requests.RequestException as e:
                    logging.error(f"Attempt {attempt + 1} to download {resource_url} failed: {e}")
                    attempt += 1
                    if attempt < self.max_retries:
                        time.sleep(self.sleep_between_retries)
            else:
                logging.error(f"Failed to download resource {resource_url} after {self.max_retries} attempts.")
    
