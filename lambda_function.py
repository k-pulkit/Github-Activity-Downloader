import json
import logging
import os, sys
from download import download_file
from upload import upload_file
from file_yield import FileGenerator

def lambda_handler(event, context):

    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    except:
        logging.error("Config exists")

    # code here
    logger = logging.getLogger("main")
    file = os.environ.get('BASELINE_FILE')
    prefix = os.environ.get('FILE_PREFIX')

    # pass baseline file and bookmark key where bookmark is stored
    generator = FileGenerator(baseline_filename=file,  bookmark_key=f'{prefix}/bookmark')

    while True:
        curr = generator.get_bookmark()
        # download the file
        file_res = download_file(curr)
        if file_res.status_code == 404:
            logger.warning(f"File does not exist yet: {curr}. Exiting")
            break
        else: 
    # upload the file to S3
            res = upload_file(
                f'{prefix}/{curr}',
                bucket=os.environ.get('BUCKET_NAME'),
                content=file_res.content
            )
            generator.generate_next_file()
    