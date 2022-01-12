from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import requests
import pandas as pd
import argparse

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


if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--outfolder", help='e.g. data/test1.csv')
  parser.add_argument("--test", help='if true use one row')
  parser.add_argument("--outfile_prefix", help='prefix for outfile')
  parser.add_argument("--collection", help='specify name of collection')
  args = parser.parse_args()
  address_dict = {"0xd6f4a923e2ecd9ab7391840ac78d04bfe40bd5e1":"budverse", "0xa67d63e68715dcf9b65e45e5118b5fcd1e554b5f":"pepsi-mic-drop"}
  contracts = ["0xd6f4a923e2ecd9ab7391840ac78d04bfe40bd5e1", "0xa67d63e68715dcf9b65e45e5118b5fcd1e554b5f"]
  estconfirmed = {"gte":"2021-12-01T00:00:00.000Z"}
  record_list = []
  after = None
  continue_flag = True
  while continue_flag:
    query = transactionQuery(after)
    result = transactionsFromICY(query, address_dict, after, contracts, estconfirmed)
    data = [i.get('node') for i in result.get('logs').get('edges')]
    if data != []:
      record_list += data
      after = result.get('logs').get('edges')[-1].get('cursor')
    else:
      continue_flag=False

  df = pd.DataFrame.from_records(record_list)
  df['collection'] = df['contractAddress'].map(address_dict)
  df.marketplace=df.marketplace.fillna('OPENSEA')
  print(df)
  df.to_csv('data/icy_transactions.csv', index=False, header=list(df.columns))
