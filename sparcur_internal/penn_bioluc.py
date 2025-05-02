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

def initiate_biolucida_upload(filename, filesize, chunk_size, token):
    url_bl_uinit = f"{Config.BIOLUCIDA_ENDPOINT}/upload/init"
    response = requests.post(url_bl_uinit,
                        data=dict(
                            filename=filename,
                            filesize=filesize,
                            chunk_size=chunk_size),
                            headers=dict(token=token))
    if response.status_code == requests.codes.ok:
        content = response.json()
        if content['status'] == 'success':
            return content['upload_key'], content['total_chunks']
    return None


def cancel_biolucida_upload(upload_key):
    url_bl_ucancel = f"{Config.BIOLUCIDA_ENDPOINT}/upload/cancel"
    response = requests.post(url_bl_ucancel,
                        data=dict(
                            upload_key=upload_key
                        ))
    if response.status_code == requests.codes.ok:
        content = response.json()
        if content['status'] == 'success':
            return content['filepath'], content['files']
    return None

def finalise_biolucida_upload(upload_key, filename):
    url_bl_ufin = f"{Config.BIOLUCIDA_ENDPOINT}/upload/finish"
    response = requests.post(url_bl_ufin,
                    data=dict(upload_key=upload_key))
    output = {}
    if response.status_code == requests.codes.ok:
        log_file.write(f"Upload for {filename} completed\n")
        content = response.json()
        if content['status'] == 'success':
            content = response.json()
            if 'img_id' in content:
              log_file.write(f"Finish api biolucida id: {content['img_id']}\n")
              output['img_id']  = content['img_id']
            else:
              if 'collection_id' in content and 'basename' in content:
                log_file.write(f"Finish api biolucida id: {content['collection_id']}\n")
                output['collection_id'] = content['collection_id']
                output['basename'] = content['basename']
            return output
    else:
        log_file.write(f"Finish api for upload for {filename} failed\n")
    return None

def get_biolucida_id(filename):
    url_bl_search = f"{Config.BIOLUCIDA_ENDPOINT}/search/{filename}"
    resp = requests.get(url_bl_search)
    if resp.status_code == requests.codes.ok:
        content = resp.json()
        if content['status'] == 'success':
            images = content['images']
            for image in images:
                if image['original_name'] == filename:
                    return image['url'] #this is the id

    return None


def get_upload_key(resp):
    print(resp.headers, resp.text)
    return imageid


def upload_to_bl(dataset_id, published_id, package_id, s3url, filename, filesize, chunk_size=1048576):
    print(f"Uploading {published_id}, {s3url}, {filename}")
    log_file.write(f"Upload {published_id}, {dataset_id}, {package_id}, {s3url}, {filename}, {filesize}\n")
    # see https://documenter.getpostman.com/view/8986837/SWLh5mQL
    # see also https://github.com/nih-sparc/sparc-app/blob/0ca1c33e245b39b0f07485a990e3862af085013e/nuxt.config.js#L101
    BL_SERVER_URL = Config.BIOLUCIDA_ENDPOINT
     # filesize chunk_size filename -> upload_key
    # chunk_size is after decoded from base64
    # chunk_id means we can go in parallel in principle
    url_bl_ucont = f"{BL_SERVER_URL}/upload/continue" # upload_key upload_data chunk_id
    url_bl_ima = f"{BL_SERVER_URL}/imagemap/add"  # imageid sourceid blackfynn_datasetId discover_datasetId

    token = get_biolucida_token()
    item = {
        "package_id": package_id,
        "filename": filename,
        "discover_id": published_id,
        "status": "failed"
    }

    if token:
        upload_key, expect_chunks = initiate_biolucida_upload(filename, filesize, chunk_size, token)
        log_file.write(f"{upload_key}, {expect_chunks}\n")
        # see https://documenter.getpostman.com/view/8986837/SWLh5mQL

        if upload_key:
            resp_s3 = requests.get(s3url, stream=True)
            for i, chunk in enumerate(resp_s3.iter_content(chunk_size=chunk_size)):
                msg = f"Chunk {i} of {expect_chunks}: "
                log_file.write(msg)
                print(msg)
                b64chunk = base64.encodebytes(chunk)
                resp_cont = requests.post(url_bl_ucont,
                                        data=dict(
                                            upload_key=upload_key,
                                            upload_data=b64chunk,
                                            chunk_id=i))
                if resp_cont.status_code == requests.codes.ok:                           
                    content = resp_cont.json()
                    if content['status'] == 'success':
                        log_file.write("Successful\n")
                        print("Successful")
                    else:
                        log_file.write("Fail\n")
                        print("Fail")
                else:
                    log_file.write("Fail\n")
                    print("Fail")

            data = finalise_biolucida_upload(upload_key, filename)
            if data:
              item['status'] = "successful"
              for key in data:
                item[key] = data[key]
    bp_list.append(item)
    print(item['status'])
    

def kwargs_from_pathmeta(blob, pennsieve_session, published_id):
    dataset_id = 'N:' + blob['dataset_id']
    package_id = 'N:' + blob['remote_id']
    filename = blob['basename']
    filesize = blob['size_bytes']
    resp = pennsieve_session.get(blob['uri_api'])
    s3url = resp.json()['url']
    return dict(
        dataset_id=dataset_id,
        published_id=published_id,
        package_id=package_id,
        s3url=s3url,
        filename=filename,
        filesize=filesize
    )


def make_pennsieve_session():
    api_key = Config.PENNSIEVE_API_TOKEN
    api_secret = Config.PENNSIEVE_API_SECRET

    r = requests.get(f"{Config.PENNSIEVE_API_HOST}/authentication/cognito-config")
    r.raise_for_status()

    cognito_app_client_id = r.json()["tokenPool"]["appClientId"]
    cognito_region = r.json()["region"]

    cognito_idp_client = boto3.client(
        "cognito-idp",
        region_name=cognito_region,
        aws_access_key_id="",
        aws_secret_access_key="",
    )

    login_response = cognito_idp_client.initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": api_key, "PASSWORD": api_secret},
        ClientId=cognito_app_client_id,
    )

    api_token = login_response["AuthenticationResult"]["AccessToken"]

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {api_token}"})
    return session


def process_files(dataset_id, skipped, extensions=("jpx", "jp2"), bioluc_username=None):
    dataset_uuid = dataset_id.split(':')[-1]
    url_metadata = f"https://cassava.ucsd.edu/sparc/datasets/{dataset_uuid}/LATEST/curation-export.json"
    url_path_metadata = f"https://cassava.ucsd.edu/sparc/datasets/{dataset_uuid}/LATEST/path-metadata.json"

    # fetch metadata and path metadata
    metadata = requests.get(url_metadata).json()
    path_metadata = requests.get(url_path_metadata).json()
    published_id = metadata['meta'].get('id_published', None)

    pennsieve_session = make_pennsieve_session()

    # get jpx and jp2 files
    matches = []
    for blob in path_metadata['data']:
        bn = blob['basename']
        if bn.endswith('.jpx') or bn.endswith('.jp2'):
            matches.append(blob)

    wargs = []

    for match in matches:
        wargs.append(kwargs_from_pathmeta(match, pennsieve_session, published_id))

    for warg in wargs:
      try:
        if not warg['package_id'] in skipped:
          print('Required uploading', warg['filename'])
          upload_to_bl(**warg)
        else:
          print('uploaded', warg['filename'])
      except:
        item = {
          "package_id": warg['package_id'],
          "filename": warg['filename'],
          "discover_id": warg['published_id'],
          "status": "failed"
        }
        bp_list.append(item)


def main():
    dataset_id = "N:dataset:aa43eda8-b29a-4c25-9840-ecbd57598afc"  # f001
    skipped = []
    try:
      f = open('input.json', 'rb')
      with f:
        data = json.load(f)
        for item in data:
          if item['status'] == 'successful':
            bp_list.append(item)
            skipped.append(item['package_id'])
        print(skipped)
    except OSError:
      print("No input file")

    process_files(dataset_id, skipped)
    log_file.close()

    with open('output.json', 'w') as f:
        json.dump(bp_list, f)

if __name__ == "__main__":
    main()
