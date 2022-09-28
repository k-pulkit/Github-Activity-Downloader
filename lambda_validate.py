#!ghad-venv/bin/python
from lambda_function import lambda_handler
import logging, os

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("main")

    os.environ.setdefault('AWS_PROFILE', 'github-user')
    os.environ.setdefault('BUCKET_NAME', 'pknn-github')
    os.environ.setdefault('FILE_PREFIX', 'sandbox')
    os.environ.setdefault('BASELINE_FILE', '2022-09-29-1.json.gz')

    logger.info('Calling the lambda handler')
    res = lambda_handler(None, None)