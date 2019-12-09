import sys
import pickle
from urllib import parse
from getpass import getpass
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from .config import auth


class InstalledAppFlowConsole(InstalledAppFlow):
    def run_console_only(self,
                         uri_to_url=True,
                         **kwargs):
        kwargs.setdefault("prompt", "consent")

        try:
            self.redirect_uri = self.client_config['redirect_uris'][0]
        except IndexError as e:
            raise ValueError('No redirect uris were provided in your client config!') from e

        auth_url, _ = self.authorization_url(**kwargs)
        if uri_to_url:
            auth_url = auth_url.replace('redirect_uri', 'redirect_url')

        code = get_auth_code(auth_url)
        self.fetch_token(code=code, include_client_id=True)
        creds = self.credentials
        print('Authentication successful.')
        return creds


def get_auth_code(url):
    import robobrowser
    br = robobrowser.RoboBrowser()
    br.open(url)
    form = br.get_form(id=0)
    if form is None:
        raise ValueError('No form! Do you have the right client id?')
    print('If you registered using google please navigate to\n'
          'the url below and leave email and password blank.')
    print()
    print(url)
    print()
    print(form)
    print()
    print('protocols.io OAuth form')
    e = form['email'].value = input('Email: ')
    p = form['password'].value = getpass()
    if e and p:
        br.submit_form(form)
        params = dict(parse.parse_qsl(parse.urlsplit(br.url).query))

    elif (not e or not p) or 'code' not in params:
        print('If you are logging in via a 3rd party services\n'
              'please paste the redirect url in the prompt')
        manual_url = input('redirect url: ')
        params = dict(parse.parse_qsl(parse.urlsplit(manual_url).query))
        if 'code' not in params:
            print('No auth code provided. Exiting ...')
            sys.exit(10)

    code = params['code']
    return code


def get_protocols_io_auth(creds_file,
                          store_file=auth.get_path('protocols-io-api-store-file'),
                          SCOPES = 'readwrite'):

    if store_file.exists():
        with open(store_file, 'rb') as f:
            try:
                creds = pickle.load(f)
            except pickle.UnpicklingError as e:
                # FIXME need better way to trace errors in a way
                # that won't leak secrets by default
                log.error(f'problem in file at path for {_auth_var}')
                raise e
    else:
        creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlowConsole.from_client_secrets_file(creds_file.as_posix(), SCOPES)
            creds = flow.run_console_only()

        with open(store_file, 'wb') as f:
            pickle.dump(creds, f)

    return creds
