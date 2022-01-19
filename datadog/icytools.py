from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import requests
import pandas as pd
import argparse
import datetime as dt
from datetime import datetime, timedelta

def statsQuery(gte, lte, address):
  query = gql(
    '''
    query($address: String!, $gte: Date, $lte: Date) {
      contract(address: $address) {
        address
       
        ... on ERC721Contract {
          name
         
          stats(timeRange: { gte: $gte, lte: $lte }) {
            average
            ceiling
            floor
            totalSales
            volume
          }
        }
      }
    }
  ''')
  parameters = {"address": address, "gte":gte, "lte":lte}
  return query, parameters

def statsFromICY(query, parameters):
  transport = AIOHTTPTransport(url='https://developers.icy.tools/graphql', headers={'x-api-key': 'f59a44bd-c4a7-457f-8844-37f911577970'})
  client = Client(transport=transport, fetch_schema_from_transport=True)
  result = client.execute(query, variable_values=parameters)
  return result

def transactionQuery(after):
  if after:
    return gql(
      """
      query Vayner ($filter: LogsFilterInputType, $first: Int!, $after: String!){
 logs(first: $first, after: $after, filter: $filter) {
    edges {
      cursor
      node {
        contractAddress
        fromAddress
        toAddress
        estimatedConfirmedAt
        type
        token {tokenId}
       
        ... on OrderLog {
          marketplace
          priceInEth
        }
      }
    }
  }
}
  """
  )
  else:
    return gql(
      """
      query Vayner ($filter: LogsFilterInputType, $first: Int!){
 logs(first: $first, filter: $filter) {
    edges {
      cursor
      node {
        contractAddress
        fromAddress
        toAddress
        estimatedConfirmedAt
        type
        token {tokenId}
       
        ... on OrderLog {
          marketplace
          priceInEth
        }
      }
    }
  }
}
  """

      )
def transactionsFromICY(query, address_dict, after, contracts, estconfirmed):
  transport = AIOHTTPTransport(url='https://developers.icy.tools/graphql', headers={'x-api-key': 'f59a44bd-c4a7-457f-8844-37f911577970'})
  client = Client(transport=transport, fetch_schema_from_transport=True)
  params = { 
  "first":50,
  "filter": {
    "contractAddress": {
    "in": contracts},
    "type": "ORDER",
    "estimatedConfirmedAt": estconfirmed
        }
  }
  if after:
    params["after"] = after
  result = client.execute(query, variable_values=params)
  return result

def processStatResult(result, lte, lookback):
  x = {
  'datapull_ds':dt.date.today().strftime('%Y-%m-%d')
  ,'ds': lte
  , 'lookback': lookback
  , 'address': result.get('contract').get('address')
  , 'name': result.get('contract').get('name')
  }
  y = result.get('contract').get('stats')
  return {**x,**y}

def processTransactionResult(result):
  x = [i.get('node') for i in result.get('logs').get('edges')]
  for i in x:
    try:
      i['token'] = i.get('token').get('tokenId')
    except:
      i['token'] = -1
  return x

def processDF(df):
  df['collection'] = df['contractAddress'].map(address_dict)
  df.marketplace=df.marketplace.fillna('UNK')
  return df

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--endpoint", help='e.g. transactions, stats')
  parser.add_argument("--lookback", help='list of lookback windows e.g 1,7,30')
  parser.add_argument("--start", help='')
  parser.add_argument("--end", help='')
  parser.add_argument("--backfill", help='')
  args = parser.parse_args()
  address_dict = {"0xd6f4a923e2ecd9ab7391840ac78d04bfe40bd5e1":"budverse", "0xa67d63e68715dcf9b65e45e5118b5fcd1e554b5f":"pepsi-mic-drop"}
  contracts = ["0xd6f4a923e2ecd9ab7391840ac78d04bfe40bd5e1", "0xa67d63e68715dcf9b65e45e5118b5fcd1e554b5f"]
  if args.endpoint == 'transactions':    
    estconfirmed = {"gte":"2021-12-01T00:00:00.000Z"}
    record_list = []
    after = None
    continue_flag = True
    while continue_flag:
      query = transactionQuery(after)
      result = transactionsFromICY(query, address_dict, after, contracts, estconfirmed)
      data = processTransactionResult(result)
      if data != []:
        record_list += data
        after = result.get('logs').get('edges')[-1].get('cursor')
      else:
        continue_flag=False
    df = pd.DataFrame.from_records(record_list)
    df = processDF(df)
    df.to_csv('data/icy_transactions.csv', index=False, header=list(df.columns))

  if args.endpoint=='stats':
    
    start = args.start
    start_ = dt.datetime.strptime(start, '%Y-%m-%d')
    backfill = int(args.backfill) or 0
    dt_range = [start_ - timedelta(days=x) for x in range(backfill)]

    result_list = []
    for contract in contracts:
      for lookback in args.lookback.split(','):
        lookback = int(lookback)
        for d in dt_range:
          gte = (d - timedelta(days=lookback)).strftime('%Y-%m-%d')
          lte = d.strftime('%Y-%m-%d')
          query, parameters = statsQuery(gte, lte, contract)
          result = statsFromICY(query, parameters)
          print(result)
          try:
            data = processStatResult(result, lte, lookback)
            result_list.append(data)
          except TypeError:
            print(d,gte,lte)
    df = pd.DataFrame.from_records(result_list)
    df.to_csv('data/icy_stats.csv', index=False, header=list(df.columns))