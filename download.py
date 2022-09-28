import requests
import logging

def download_file(file):
    logger = logging.getLogger('main')
    logger.info(f'Starting file download {file}')
    res = requests.get(f'https://data.gharchive.org/{file}')
    logger.info("File download complete")
    return res