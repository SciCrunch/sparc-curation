import math
import base64
import pathlib
import boto3  # sigh
import requests
from config import Config
import json

log_file = open('progress_log.txt', 'a')

bp_list = []


def get_biolucida_token():
    url_bl_auth = f"{Config.BIOLUCIDA_ENDPOINT}/authenticate"
    response = requests.post(url_bl_auth,
                        data=dict(
                            username=Config.BIOLUCIDA_USERNAME,
                            password=Config.BIOLUCIDA_PASSWORD,
                            token=''))
    if response.status_code == requests.codes.ok:
        content = response.json()
        if content['status'] == 'success':
            return content['token']
    return None

def map_id(item, token, dataset_id, discover_id):
    print(item, token)
    url_bl_imagemap = f"{Config.BIOLUCIDA_ENDPOINT}/imagemap/add"
    resp = requests.post(url_bl_imagemap,
                          data=dict(
                            imageId=item['img_id'],
                            sourceId=item['package_id'],
                            blackfynn_datasetId=dataset_id,
                            discover_datasetId=discover_id
                            ),
                        headers=dict(token=token))
    print(resp)
    if resp.status_code == requests.codes.ok:
        content = resp.json()
        print(content)

    return item


def main():
    dataset_id = Config.DATASET_UUID
    discover_id = Config.DISCOVER_ID
    bp_list = []
    if dataset_id and discover_id:
      try:
        f = open('output_with_id.json', 'rb')
        with f:
          token = get_biolucida_token()
          data = json.load(f)
          for item in data:
            if item['status'] == 'successful' and 'img_id' in item and item['img_id']:
              map_id(item, token, dataset_id, discover_id)
          
      except OSError:
        print("No input file")
    else:
      print("Missing dataset uuid or discover id or both.")

if __name__ == "__main__":
    main()
