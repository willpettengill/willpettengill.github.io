## cd '/Users/wpettengill/Desktop/willpettengill.github.io/ai_astrology'
from __future__ import print_function
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import re
import hashlib

answerMapping={
'agree':1, 'disagree':0,
'true':1, 'false':0}

def user_content(df, udf):
	for i in range(len(udf)):
		x = Dog(udf.birthdate[i], udf.birthtime[i], '02461')
		

def answer_map(x):
	if type(x)==int:
		return x
	elif answerMapping.get(x.lower()) is not None:
		return answerMapping.get(x.lower())
	else:
		return x.lower()

def zip_normalize(x):
	if len(x) == 4:
		x = '0'+ x
	else:
		pass	
	return x

def main():

	SCOPE = ['https://spreadsheets.google.com/feeds',
	         'https://www.googleapis.com/auth/drive']
	SECRETS_FILE = "ML Horoscope-57bf7abc958a.json"
	SPREADSHEET = "Machine Learning Horoscope (Responses)"

	json_key = json.load(open(SECRETS_FILE))

	credentials = ServiceAccountCredentials.from_json_keyfile_name('ML Horoscope-57bf7abc958a.json', SCOPE)

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
	#print(sheet.get_all_records())
	column_map = {k:re.sub(r'[^\w\s]','',k.lower().replace(' ','')) for k in data.columns.tolist()}
	data = data.rename(columns=column_map)
	data['emd5'] = data['emailaddress'].map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
	data['birthplacezipcode'] = data['birthplacezipcode'].astype(str)
	data['birthplacezipcode'] = data['birthplacezipcode'].map(lambda x: zip_normalize(x))
	data = data.set_index('emd5')
	user_data = ['birthdate', 'birthtime', 'birthplacezipcode', 'timestamp', 'emailaddress']
	udf = data[user_data]
	df = data[[i for i in data.columns.tolist() if i not in user_data]]
	df = df.applymap(lambda x: answer_map(x))

	categoricals = {k.name: list(v) for k, v in df.columns.to_series().groupby(df.dtypes).groups.items()}


	w = workbook.get_worksheet(2) #occult
	ww = pd.DataFrame.from_records(w.get_all_records(), index='Quality').reset_index()
	

	

	with open('categoricals.json', 'w') as fp:
	    json.dump(categoricals, fp)
	df.to_csv('survey.csv')
	udf.to_csv('users.csv')
	ww.to_csv('sun_qualities.csv')
	return df, udf, sheet, data, w, ww
	



if __name__ == '__main__':
	df, udf, sheet, data, w, ww = main()






	