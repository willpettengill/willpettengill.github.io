# python osea_data_1.py --collections budverse-cans-heritage-edition,pepsi-mic-drop
import requests
import pandas as pd
import argparse
import datetime as dt
import time 

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 20)

def fieldsFromAPI(c):
	token_id = c.get('token_id', {}) # e.g. 527 - specifies token
	permalink = c.get('permalink', {})
	top_bid=c.get('top_bid', {})
	num_sales=c.get('num_sales', {})
	owner = c.get('owner', {}).get('address', {})
	collection_slug = c.get('collection', {}).get('slug', {})
	if c.get('sell_orders'):
		if c.get('sell_orders')[0].get('expiration_time',1)>time.time():
			for_sale=True
		else:
			for_sale=False
	else:
		for_sale=False
	try:
		last_sale_usd= c.get('last_sale').get('payment_token').get('usd_price') if (c.get('last_sale') or None) is not None else None
		last_sale_eth= c.get('last_sale').get('payment_token').get('eth_price') if (c.get('last_sale') or None) is not None else None
		last_sale_from_address = c.get('last_sale').get('transaction').get('from_account').get('address')
		last_sale_from_user = c.get('last_sale').get('transaction').get('from_account').get('user').get('username')
		last_sale_to_address = c.get('last_sale').get('transaction').get('to_account').get('address')
		last_sale_to_user = c.get('last_sale').get('transaction').get('to_account').get('user').get('username')
	except:
		last_sale_usd = None
		last_sale_eth = None
		last_sale_from_address = None
		last_sale_from_user = None
		last_sale_to_address = None
		last_sale_to_user = None
	traits = c.get('traits', {})
	last_sale_ds = c.get('created_date', {})
	contract_address = c.get('asset_contract', {}).get('address', {})
	datapull_ds = dt.date.today().strftime('%Y-%m-%d')
	asset_data = {'token_id': token_id, 'owner':owner, 'collection': collection_slug, 'contract_address':contract_address, 'datapull_ds': datapull_ds,  'permalink': permalink, 'last_sale_usd': last_sale_usd, 'last_sale_eth': last_sale_eth, 'top_bid': top_bid, 'num_sales': num_sales, 'last_sale_from_address': last_sale_from_address, 'last_sale_from_user': last_sale_from_user, 'last_sale_to_address': last_sale_to_address, 'last_sale_to_user': last_sale_to_user, 'traits': traits, 'last_sale_ds': last_sale_ds, 'for_sale':for_sale}
	return asset_data

def dfFromCollection(collection, headers, test):
	asset_list = []	
	offset=0
	pagination_flag=True
	while pagination_flag==True:
		url = "https://api.opensea.io/api/v1/assets?order_direction=desc&offset={1}&collection={0}&limit=50".format(collection, offset)
		response = requests.request("GET", url, headers=headers)
		print(response.text)
		assets = dict(response.json()).get('assets', {})
		for c in assets:
			asset_data = fieldsFromAPI(c)
			asset_list.append(asset_data)

		if response.text != '{"assets":[]}':
			offset += 50
		else:
			pagination_flag=False
		if test:
			break
	df = pd.DataFrame.from_records(asset_list, columns=asset_list[0].keys()).sort_values(by='token_id', ascending=True) # ['token_id', 'permalink', 'last_sale_usd', 'last_sale_eth', 'top_bid', 'num_sales']
	df = df.fillna(value=0)
	return df, assets

def StatsByCollection(collection, addToday=True, addCollection=True):
	url='https://api.opensea.io/api/v1/collection/{}/stats'.format(collection)
	response = requests.request("GET", url)
	stats = dict(response.json()).get("stats", {})
	if addToday:
		stats['ds'] = dt.date.today().strftime("%Y-%m-%d")
	if addCollection:
		stats['collection'] = collection
	df = pd.DataFrame.from_records([stats])
	return df

def ListingStatus(collection, api_key):
	url = "https://api.opensea.io/api/v1/events?collection_slug=budverse-cans-heritage-edition&event_type=successful&only_opensea=false&offset=0&limit=100"
	headers = {
	    "Accept": "application/json",
	    "X-API-KEY": api_key
	}
	response = requests.request("GET", url, headers=headers)
	
	return dict(response.json()).get("asset_events",{})

if __name__ == '__main__':
	
	parser = argparse.ArgumentParser()
	parser.add_argument("--collections", help='e.g. data/test1.csv')
	parser.add_argument("--outfolder", help='e.g. data/test1.csv')
	parser.add_argument("--test", help='if true use one row')
	parser.add_argument("--outfile_prefix", help='prefix for outfile')
	parser.add_argument("--endpoint", help='e.g. stats or token')
	args = parser.parse_args()
	collections = args.collections.split(',')
	api_key = '610c839f631b446f98c8ed1f2611d89e'
	headers = {
    "Accept": "application/json",
    "X-API-KEY": '610c839f631b446f98c8ed1f2611d89e'
	}

	if args.endpoint == 'token':
		for collection in collections:
			print(collection)
			df, assets = dfFromCollection(collection, headers, args.test)
			file_to_write = (args.outfolder or 'data') + '/' + (args.outfile_prefix or '') + collection + '.csv'
			df.to_csv(file_to_write, index=False, header=list(df.columns))

	if args.endpoint == 'stats':
		for collection in collections:
			df = StatsByCollection(collection, addToday=True, addCollection=True)
			file_to_write = (args.outfolder or 'data') + '/' + (args.outfile_prefix or '') + collection + '.csv'
			df.to_csv(file_to_write, index=False, header=list(df.columns))
