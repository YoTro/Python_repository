from __future__ import print_function
import httplib2
import os
import pickle
from apiclient import discovery
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
            
    return creds

def CUS_shippingfee(weight):
    """

    Creates a Sheets API service object and return a array of CN_TO_US shipping fee in this spreadsheet :
    https://docs.google.com/spreadsheets/d/1c7EkmwQNWVEwP2qgKERSx6tQnrb5tvqSidyY3m5BP_M/edit#gid=0it
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheetId = '1c7EkmwQNWVEwP2qgKERSx6tQnrb5tvqSidyY3m5BP_M'
    rangeName = ['xuni_shipping_fee!A2:B80','xuni_shipping_fee!H2:I80']
    result = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        for row in values:
            if weight<=2000:
                for i in xrange(len(row[0][0])):
                    if weight<=int(row[0][0][i])*1000:
                        return float(row[0][1][i])
                        break
            else:
                for i in xrange(len(row[1][0])):
                    if weight<=int(row[1][0][i])*1000:
                        return float(row[1][1][i])
                        break        



