# python osea_data_1.py --collections budverse-cans-heritage-edition,pepsi-mic-drop
import requests
import pandas as pd
import argparse
import datetime as dt
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 20)

def fieldsFromAPI(c):
	token_id = c.get('token_id') # e.g. 527 - specifies token
	permalink = c.get('permalink')
	last_sale_usd=c.get('last_sale').get('payment_token').get('usd_price') if c.get('last_sale') is not None else None
	last_sale_eth=c.get('last_sale').get('payment_token').get('eth_price') if c.get('last_sale') is not None else None
	top_bid=c.get('top_bid')
	num_sales=c.get('num_sales')
	asset_data = {'token_id': token_id, 'permalink': permalink, 'last_sale_usd': last_sale_usd, 'last_sale_eth': last_sale_eth, 'top_bid': top_bid, 'num_sales': num_sales}
	return asset_data

def dfFromCollection(collection, test):
	asset_list = []	
	offset=0
	pagination_flag=True
	while pagination_flag==True:
		print(offset)
		url = "https://api.opensea.io/api/v1/assets?order_direction=desc&offset={1}&collection={0}&limit=50".format(collection, offset)
		response = requests.request("GET", url)
		print(response.text)
		assets = dict(response.json()).get('assets')
		print(len(assets))
		for c in assets:
			asset_data = fieldsFromAPI(c)
			asset_list.append(asset_data)

		if response.text != '{"assets":[]}':
			offset += 50
		else:
			pagination_flag=False
		if test:
			break
	df = pd.DataFrame.from_records(asset_list, columns=['token_id', 'permalink', 'last_sale_usd', 'last_sale_eth', 'top_bid', 'num_sales']).sort_values(by='token_id', ascending=True)
	df = df.fillna(value=0)
	print(df)
	return df, assets

def StatsByCollection(collection, addToday=True, addCollection=True):
	url='https://api.opensea.io/api/v1/collection/{}/stats'.format(collection)
	response = requests.request("GET", url)
	stats = dict(response.json()).get("stats")
	if addToday:
		stats['ds'] = dt.date.today().strftime("%Y-%m-%d")
	if addCollection:
		stats['collection'] = collection
	df = pd.DataFrame.from_records([stats])
	print(df)
	return df

if __name__ == '__main__':
	
	parser = argparse.ArgumentParser()
	parser.add_argument("--collections", help='e.g. data/test1.csv')
	parser.add_argument("--outfolder", help='e.g. data/test1.csv')
	parser.add_argument("--test", help='if true use one row')
	parser.add_argument("--outfile_prefix", help='prefix for outfile')
	parser.add_argument("--endpoint", help='e.g. stats or token')
	args = parser.parse_args()
	collections = args.collections.split(',')
	
	if args.endpoint == 'token':
		for collection in collections:
			print(collection)
			#collection='budverse-cans-heritage-edition'
			#df, assets = dfFromCollection(collection, True)
			df, assets = dfFromCollection(collection, args.test)
			file_to_write = (args.outfolder or 'data') + '/' + (args.outfile_prefix or '') + collection + '.csv'
			df.to_csv(args.outfile, index=False, header=list(df.columns))

	if args.endpoint == 'stats':
		for collection in collections:
			df = StatsByCollection(collection, addToday=True, addCollection=True)
			file_to_write = (args.outfolder or 'data') + '/' + (args.outfile_prefix or '') + collection + '.csv'
			df.to_csv(file_to_write, index=False, header=list(df.columns))
