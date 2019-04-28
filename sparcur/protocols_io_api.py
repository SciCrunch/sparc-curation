import json
from urllib import parse
from pathlib import Path
from getpass import getpass
from argparse import Namespace
import robobrowser
from oauth2client import file, client
from pyontutils.config import devconfig


def get_auth_code(url):
    br = robobrowser.RoboBrowser()
    br.open(url)
    form = br.get_form(id=0)
    print(form)
    form['email'].value = input('Email: ')
    form['password'].value = getpass()
    br.submit_form(form)
    params = dict(parse.parse_qsl(parse.urlsplit(br.url).query))
    code = params['code']
    return code


class MyOA2WSF(client.OAuth2WebServerFlow):
    """ monkey patch to fix protocols.io non compliance with the oauth standard """
    def step1_get_authorize_url(self):
        value = super().step1_get_authorize_url()
        return value.replace('redirect_uri', 'redirect_url')


client.OAuth2WebServerFlow = MyOA2WSF


def run_flow(flow, storage):
    url = flow.step1_get_authorize_url()
    code = get_auth_code(url)
    try:
        credential = flow.step2_exchange(code)
    except client.FlowExchangeError as e:
        sys.exit('Authentication has failed: {0}'.format(e))

    storage.put(credential)
    credential.set_store(storage)
    print('Authentication successful.')

    return credential


def _store_file():
    try:
        return devconfig.secrets('protocols-io', 'api', 'store-file')
    except KeyError:
        return 'protocols-io-api-token-rw.json'


def get_protocols_io_auth(creds_file,
                          store_file=_store_file()):
    flags = Namespace(noauth_local_webserver=True,
                      logging_level='INFO')
    spath = Path(devconfig.secrets_file).parent
    sfile = spath / store_file
    store = file.Storage(sfile.as_posix())
    creds = store.get()
    SCOPES = 'readwrite'
    if not creds or creds.invalid:
        cfile = spath / creds_file
        with open(cfile) as f:
            redirect_uri, *_ = json.load(f)['installed']['redirect_uris']
        client.OOB_CALLBACK_URN = redirect_uri  # hack to get around google defaults
        flow = client.flow_from_clientsecrets(cfile.as_posix(),
                                              scope=SCOPES,
                                              redirect_uri=redirect_uri)

        creds = run_flow(flow, store)

    return creds
