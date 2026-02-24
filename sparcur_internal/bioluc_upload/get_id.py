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

def get_biolucida_id(item, token, collection_id):
    print("original data:", item, token)
    col_id = collection_id
    if not col_id:
      col_id=item['collection_id']
    url_bl_colandbasename = f"{Config.BIOLUCIDA_ENDPOINT}/image/colandbasename"
    resp = requests.post(url_bl_colandbasename,
                        data=dict(
                            col_id=col_id,
                            basename=item['basename'],
                            ),
                        headers=dict(token=token))
    print(resp)
    if resp.status_code == requests.codes.ok:
        content = resp.json()
        print(content)
        if content['status'] == 'success' and 'image_id' in content:
            item['img_id'] = content['image_id']
            item['collection_id'] = col_id
    return item


def main():
    dataset_id = Config.DATASET_UUID  # f001
    collection_id = Config.COLLECTION_ID
    bp_list = []
    try:
      f = open('input.json', 'rb')
      with f:
        token = get_biolucida_token()
        data = json.load(f)
        for item in data:
          if item['status'] == 'successful':
            bp_list.append(get_biolucida_id(item, token, collection_id))
         
    except OSError:
      print("No input file")

    with open('output_with_id.json', 'w') as f:
        json.dump(bp_list, f)

if __name__ == "__main__":
    main()
