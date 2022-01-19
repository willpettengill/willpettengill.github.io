# python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range xdata --push true --csv data/test1.csv

from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from sqlalchemy import create_engine
import re
import pandas as pd
import argparse
from datetime import datetime, timedelta
import yaml
import os


def parse_dates(data): 
	dateparse = lambda x: pd.datetime.strptime(x, '%b %d, %Y')
	data['ds']=data['Date'].map(dateparse)
	return data

def pull_google_sheet(SPREADSHEET_ID, RANGE_NAME, path):
	creds=None
	if os.path.exists(path+'token.pickle'):
	    with open(path+'config/token.pickle', 'rb') as token:
	        creds = pickle.load(token)
	if not creds or not creds.valid:
	    if creds and creds.expired and creds.refresh_token:
	        creds.refresh(Request())
	    else:
	        flow = InstalledAppFlow.from_client_secrets_file(
	            path+'config/{}'.format(secret_file), SCOPES)
	        creds = flow.run_local_server(port=8000)
	    with open(path+'config/token.pickle', 'wb') as token:
	        pickle.dump(creds, token)
	service = build('sheets', 'v4', credentials=creds)
	sheet = service.spreadsheets()
	result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
	values = result.get('values', [])
	df = pd.DataFrame.from_records(values[1:], columns=values[0])
	return df

def push_google_sheet(SPREADSHEET_ID, RANGE_NAME, VALUES, path, APPEND=False):
	creds=None
	if os.path.exists(path+'/config/token.pickle'):
	    with open(path+'/config/token.pickle', 'rb') as token:
	        creds = pickle.load(token)
	if not creds or not creds.valid:
	    if creds and creds.expired and creds.refresh_token:
	        creds.refresh(Request())
	    else:
	        flow = InstalledAppFlow.from_client_secrets_file(
	            path+'config/{}'.format(secret_file), SCOPES)
	        creds = flow.run_local_server(port=8000)
	    with open(path+'config/token.pickle', 'wb') as token:
	        pickle.dump(creds, token)
	
	values = VALUES # list of cell values, one list per row
	body = {'values': values}
	service = build('sheets', 'v4', credentials=creds)
	
	if APPEND:
		result = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='RAW', body=body).execute()
	else:
		result = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='RAW', body=body).execute()
	print('{0} cells updated.'.format(result.get('updatedCells')))

	
if __name__ == '__main__':
	
	parser = argparse.ArgumentParser()
	parser.add_argument("--sheetid", help='from url slug e.g. 1J3tqmjylOTsITQS8qpeu9RyfB_ohj24Dr_ttqgUBbns')
	parser.add_argument("--range", help='tab name e.g. transactions')
	parser.add_argument("--table", help='snowflake table to write to')
	parser.add_argument("--push", help='push data to sheet')
	parser.add_argument("--append", help='if true append else overwrite')
	parser.add_argument("--csv", help='e.g. data/test1.csv')
	parser.add_argument("--header", help='include column names?')
	parser.add_argument("--top-level-dir", help='top_level_dir')
	args = parser.parse_args()
	yesterday = (pd.datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
	threedaysago = (pd.datetime.today() - timedelta(3)).strftime('%Y-%m-%d')
	secret_file = 'client_secret_486295390024-4t330pd7rpnsfv33nir2taosrv8cl1d1.apps.googleusercontent.com.json'
	SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
	SPREADSHEET_ID = args.sheetid or '1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA'
	RANGE_NAME = args.range or 'ddata'
	if args.top_level_dir:
		path = args.top_level_dir + '/willpettengill.github.io/datadog/'
	else:
		path = 	os.getenv('HOME') + '/willpettengill.github.io/datadog/'
	APPEND = False
	if args.append:
		APPEND = True if args.append.lower()=='true' else False
	if args.csv:
		file = args.csv or 'data/test1.csv' 
		read_file=pd.read_csv(file)
		y=read_file.to_records(index=False).astype(object, copy=False)
		y = [list(x) for x in y]
		if args.header:
			if args.header.lower() == 'true':
				y.insert(0,list(read_file.columns))

	if args.push:
		RANGE_NAME = args.range or 'rdata'
		values =[[4,5,6]]
		if y:
			values = y
		push_google_sheet(SPREADSHEET_ID, RANGE_NAME, values, path, APPEND)
	else:
		data = pull_google_sheet(SPREADSHEET_ID, RANGE_NAME, path)
		print(data)