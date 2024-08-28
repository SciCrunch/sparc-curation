import math
import base64
import pathlib
import boto3  # sigh
import requests
from orthauth.stores import Secrets


def fun0(resp):
    print(resp.headers, resp.text)
    return token


def fun1(resp):
    print(resp.headers, resp.text)
    return upload_key


def fun2(resp):
    print(resp.headers, resp.text)
    return imageid


def upload_to_bl(dataset_id, published_id, package_id, s3url, filename, filesize,
                 secrets=None, username=None, BL_SERVER_URL="sparc.biolucida.net", chunk_size=4096):
    # see https://documenter.getpostman.com/view/8986837/SWLh5mQL
    # see also https://github.com/nih-sparc/sparc-app/blob/0ca1c33e245b39b0f07485a990e3862af085013e/nuxt.config.js#L101
    url_bl_auth = f"https://{BL_SERVER_URL}/api/v1/authenticate"  # username password token
    url_bl_uinit = f"https://{BL_SERVER_URL}/api/v1/upload/init" # filesize chunk_size filename -> upload_key
    # chunk_size is after decoded from base64
    # chunk_id means we can go in parallel in principle
    url_bl_ucont = f"https://{BL_SERVER_URL}/api/v1/upload/continue" # upload_key upload_data chunk_id
    url_bl_ufin = f"https://{BL_SERVER_URL}/api/v1/upload/finish"  # upload_key
    url_bl_ima = f"https://{BL_SERVER_URL}/api/v1/imagemap/add"  # imageid sourceid blackfynn_datasetId discover_datasetId
    
    password = secrets('biolucida', 'sparc', 'api', username, 'password')
    fake_token = 'derp-fake-token'
    resp_auth = requests.post(url_bl_auth,
                              data=dict(
                                  username=username,
                                  password=password,
                                  token=fake_token))
    token = fun0(resp_auth)

    resp_init = requests.post(url_bl_uinit,
                              data=dict(
                                  filename=filename,
                                  filesize=filesize,
                                  chunk_size=chunk_size),
                              headers=dict(token=token))
    upload_key = fun1(resp_init)

    resp_s3 = requests.get(s3url, stream=True)
    expect_chunks = math.ceil(filesize / chunk_size)
    for i, chunk in enumerate(resps3.iter_content(chunk_size=chunk_size)):
        b64chunk = base64.encode(chunk)
        resp_cont = requests.post(url_bl_ucont,
                                  data=dict(
                                      upload_key=upload_key,
                                      upload_data=b64chunk,
                                      chunk_id=i))
        print(resp_cont.text)

    resp_fin = requests.post(url_bl_ufin,
                             data=dict(upload_key=upload_key))

    imageid = fun2(resp_fin)  # ... uh no idea how we get this, hopefully it is in resp_fin ???
    resp_img = requests.post(url_bl_ima,
                             data=dict(
                                 imageId=imageid,
                                 sourceId=package_id,
                                 blackfynn_datasetId=dataset_id,
                                 discover_datasetId=id_published),
                             headers=dict(token=token))
    print(resp_img.text)


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


def make_pennsieve_session(secrets, organization_id):
    api_key = secrets('pennsieve', organization_id, 'key')
    api_secret = secrets('pennsieve', organization_id, 'secret')
    PENNSIEVE_URL = "https://api.pennsieve.io"

    r = requests.get(f"{PENNSIEVE_URL}/authentication/cognito-config")
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


def upload_dataset_files_to_bioluc(dataset_id, secrets=None, extensions=("jpx", "jp2"), bioluc_username=None):
    dataset_uuid = dataset_id.split(':')[-1]
    url_metadata = f"https://cassava.ucsd.edu/sparc/datasets/{dataset_uuid}/LATEST/curation-export.json"
    url_path_metadata = f"https://cassava.ucsd.edu/sparc/datasets/{dataset_uuid}/LATEST/path-metadata.json"

    # fetch metadata and path metadata
    metadata = requests.get(url_metadata).json()
    path_metadata = requests.get(url_path_metadata).json()
    published_id = metadata['meta'].get('id_published', None)
    organization_id = 'N:' + path_metadata['data'][0]['external_parent_id']

    pennsieve_session = make_pennsieve_session(secrets, organization_id)

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
        upload_to_bl(**warg, secrets=secrets, username=bioluc_username)

    # filter for just the jpx and jp2 files
    # get the package ids
    # loop over the package ids and
    # get the s3 key from pennsieve api
    # pull from the s3 address and upload the biolucida endpoint
    # get the image id from biolucida
    # post the package id to the biolucida image id so that it is mapped


def main():
    dataset_id = "N:dataset:aa43eda8-b29a-4c25-9840-ecbd57598afc"  # f001
    secrets = Secrets(pathlib.Path('~/ni/dev/secrets.sxpr').expanduser())
    upload_dataset_files_to_bioluc(dataset_id, secrets=secrets, bioluc_username='tgbugs')


if __name__ == "__main__":
    main()
