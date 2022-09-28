import logging
import os
import boto3
from botocore.errorfactory import ClientError
import datetime as dt
from datetime import timedelta as td

class FileGenerator(object):

    def __init__(self, baseline_filename, bookmark_key="sandbox/bookmark"):
        # create logger for the object
        self.logger = logging.getLogger("main")
        self.s3_client = boto3.client("s3")
        self.bucket_name = os.environ.get('BUCKET_NAME')
        self.baseline_filename = baseline_filename
        self.bookmark_key = bookmark_key
        self.bookmark = None

        # get bookmark from s3 location 
        bookmark = self.get_bookmark()
        if bookmark:
            self.bookmark = bookmark
        else:
            self.set_bookmark(self.baseline_filename)

    def get_bookmark(self):
        """This will get the latest bookmark, or create if a bookmark does not exists"""
        try:
            if self.bookmark:
                return self.bookmark
            else:
                self.logger.info(f"Reading bookmark from {self.bookmark_key}")
                s3_resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.bookmark_key)
                resp = s3_resp['Body'].read()
                self.bookmark = resp.decode('utf-8')
                self.logger.info(f"Current bookmark is {self.bookmark}")
                return self.bookmark
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.info("Bookmark does not exist")
                return None
            else:
                # If some other error then capture the same
                self.logger.exception("Error occured")

    def set_bookmark(self, bookmark_tobe):
        """This function is going to set the bookmark in the S3 sandbox prefix location"""
        self.s3_client.put_object(Bucket=self.bucket_name, Key=self.bookmark_key, Body=bookmark_tobe.encode('utf-8'))
        self.bookmark = bookmark_tobe
        self.logger.info(f'Created new bookmark {bookmark_tobe}')

    def generate_next_file(self):
        """This function will increment the time on file name and return a new one"""
        current_file_name = self.bookmark
        nex_d = lambda d: (dt.datetime.strptime(d, "%Y-%m-%d-%H") + td(hours=1)).strftime("%Y-%m-%d-%-H")
        self.logger.info(f"Current file name is {current_file_name}")
        d = current_file_name.split(".")[0]
        nd = nex_d(d)
        next_name = f"{nd}.json.gz"
        self.logger.info(f"Next file name is {next_name}")
        self.logger.info(f"Setting bookmark to {next_name}")
        self.set_bookmark(next_name)

if __name__ == '__main__':

    # Test the function 

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # set the env variables
    os.environ.setdefault('AWS_PROFILE', 'github-user')
    os.environ.setdefault('BUCKET_NAME', 'pknn-github')
    os.environ.setdefault('FILE_PREFIX', 'sandbox')

    file = os.environ.get('BASELINE_FILE')
    prefix = os.environ.get('FILE_PREFIX')
    bucket = os.environ.get('BUCKET_NAME')
    
    gen = FileGenerator(baseline_filename=file,  bookmark_key=f'{prefix}/test')
    gen.generate_next_file()
    print(gen.get_bookmark())