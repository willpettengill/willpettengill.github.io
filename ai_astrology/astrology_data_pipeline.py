from __future__ import print_function
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import re

answerMapping={
'agree':1, 'disagree':0,
'true':1, 'false':0}

def answer_map(x):
	if type(x)==int:
		return x
	elif answerMapping.get(x.lower()) is not None:
		return answerMapping.get(x.lower())
	else:
		return x.lower()

SCOPE = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
SECRETS_FILE = "ML Horoscope-57bf7abc958a.json"
SPREADSHEET = "Machine Learning Horoscope (Responses)"

json_key = json.load(open(SECRETS_FILE))

credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/wpettengill/Desktop/willpettengill.github.io/ai_astrology/ML Horoscope-57bf7abc958a.json', SCOPE)

gc = gspread.authorize(credentials)

print("The following sheets are available")
for sheet in gc.openall():
    print("{} - {}".format(sheet.title, sheet.id))

workbook = gc.open(SPREADSHEET)
# Get the first sheet
sheet = workbook.sheet1
#all_keys = set().union(*(d.keys() for d in sheet.get_all_records()))
#del column_map['Email Address']
data = pd.DataFrame.from_records(sheet.get_all_records())
column_map = {k:re.sub(r'[^\w\s]','',k.lower().replace(' ','')) for k in data.columns.tolist()}
data = data.rename(columns=column_map)


data = data.set_index('emailaddress')
user_data = ['birthdate', 'birthtime', 'birthplacezipcode', 'timestamp']
udf = data[user_data]
df = data[[i for i in data.columns.tolist() if i not in user_data]]
df = df.applymap(lambda x: answer_map(x))

categoricals = {k.name: list(v) for k, v in df.columns.to_series().groupby(df.dtypes).groups.items()}

with open('categoricals.json', 'w') as fp:
    json.dump(categoricals, fp)
df.to_csv('survey.csv')
udf.to_csv('users.csv')


