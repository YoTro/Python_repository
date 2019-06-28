# -*- coding:UTF:8 -*-
# Author:Toryun
# Python version:2.7.13
# Date:18/3/25
# Function:Read and return the shipping fee of the fast yunexpress China-US  from Google Sheets
#	google-api-python-client
#	对Google系列的产品进行api访问控制，
#	Google sheet的最大单元格数：5000000
# 由于目前我不知道batchGet中range匹配多行的写法，所以暂时用get代替

from __future__ import print_function
import httplib2
import os
import pickle
from googleapiclient import discovery
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request



# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json




def CUS_shippingfee(weight):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    CLIENT_SECRET_FILE = 'credentials.json'
    APPLICATION_NAME = 'Google Sheets API Python Quickstart'
    creds = None
    p = 0
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
                CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
            


    """

    Creates a Sheets API service object and return a array of CN_TO_US shipping fee in this spreadsheet :
    https://docs.google.com/spreadsheets/d/1c7EkmwQNWVEwP2qgKERSx6tQnrb5tvqSidyY3m5BP_M/edit#gid=0it
    """

    service = discovery.build('sheets', 'v4',credentials=creds)

    spreadsheetId = '1c7EkmwQNWVEwP2qgKERSx6tQnrb5tvqSidyY3m5BP_M'
    if weight<=2000:
        rangeName = 'xuni_shipping_fee!A2:B80'
    else:
        rangeName = 'xuni_shipping_fee!H2:I80'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId,range=rangeName).execute()
    values = result.get('values', [])
    if not values:
        print('No data found.')
    else:
        
        for row in values:
            if weight<=float(row[0])*1000:
                p = float(row[1])
                break

    return p



