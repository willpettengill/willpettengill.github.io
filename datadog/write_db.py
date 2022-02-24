import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector.constants import ClientFlag

config = {
    'user': 'root',
    'password': "",
    'host': '35.193.3.52',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': 'config/server-ca.pem',
    'ssl_cert': 'config/client-cert.pem',
    'ssl_key': 'config/client-key.pem',
    'database':'vnft',
    'connection_timeout':1000
}

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)

def runTests():
    timeout_query="SHOW VARIABLES LIKE '%timeout';"
    check_query="select 'token_metadata', count(*) from vnft.token_metadata union all select 'icy_transactions', count(*) from vnft.icy_transactions union all select 'icy_stats', count(*) from vnft.icy_stats"
    cursor.execute(check_query)
    for row in cursor:
        print(row)

def dropAndCreate(table):
    if table=='token_metadata':
        cursor.execute("DROP TABLE if exists token_metadata;")
        cnxn.commit()
        cursor.execute('''CREATE TABLE token_metadata (
        token INT,
        owner VARCHAR(755),
        collection VARCHAR(755),
        contractAddress VARCHAR(755),
        datapull_ds DATETIME,
        permalink VARCHAR(755),
        num_sales SMALLINT,
        traits VARCHAR(7755),
        priceInEth FLOAT,
        last_sold_price_eth FLOAT,
        notes VARCHAR(755),
        PRIMARY KEY (contractAddress(5), token)
        )
        ''')
        
    elif table=='icy_transactions':
        cursor.execute("DROP TABLE if exists icy_transactions;")
        cnxn.commit()
        cursor.execute('''CREATE TABLE icy_transactions (
        contractAddress VARCHAR(755),
        fromAddress VARCHAR(755),
        toAddress VARCHAR(755),
        estimatedConfirmedAt VARCHAR(755),
        type VARCHAR(755),
        token VARCHAR(755),
        transactionHash VARCHAR(755),
        marketplace VARCHAR(755),
        priceInEth FLOAT,
        icy_cursor VARCHAR(755),
        collection VARCHAR(755),
        notes VARCHAR(755),
        ds DATETIME,
        PRIMARY KEY (contractAddress(5),fromAddress(10),toAddress(10),transactionHash(10))
        ) 
        ''')
        
    elif table=='icy_stats':
        cursor.execute("DROP TABLE if exists icy_stats;")
        cnxn.commit()
        cursor.execute('''CREATE TABLE icy_stats (
        contractAddress VARCHAR(755),
        fromAddress VARCHAR(755),
        toAddress VARCHAR(755),
        estimatedConfirmedAt VARCHAR(755),
        type VARCHAR(755),
        token VARCHAR(755),
        transactionHash VARCHAR(755),
        marketplace VARCHAR(755),
        priceInEth FLOAT,
        icy_cursor VARCHAR(755),
        collection VARCHAR(755),
        notes VARCHAR(755),
        ds DATETIME,
        PRIMARY KEY (contractAddress(10),fromAddress(10),toAddress(10),transactionHash(10))
        ) 
        ''')
    cnxn.commit()

def AlterPrimaryKey(table):
    stmt = "ALTER TABLE icy_stats ADD PRIMARY KEY(column_list);"

def getInsertQuery(table):
    if table=='token_metadata':
        return ("INSERT INTO token_metadata (token, owner, collection, contractAddress, datapull_ds, permalink, num_sales, traits, priceInEth, last_sold_price_eth, notes) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    elif table=='icy_transactions':
        return ("INSERT INTO icy_transactions (contractAddress, fromAddress, toAddress, estimatedConfirmedAt, type, token, transactionHash, marketplace, priceInEth, icy_cursor, collection, notes, ds) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    elif table=='icy_stats':
        return ("INSERT INTO icy_stats (contractAddress, fromAddress, toAddress, estimatedConfirmedAt, type, token, transactionHash, marketplace, priceInEth, icy_cursor, collection, notes, ds) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

def getUpsertQuery(table):
    if table=='token_metadata':
        return ("REPLACE INTO token_metadata (token, owner, collection, contractAddress, datapull_ds, permalink, num_sales, traits, priceInEth, last_sold_price_eth, notes) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    elif table=='icy_transactions':
        return ("REPLACE INTO icy_transactions (contractAddress, fromAddress, toAddress, estimatedConfirmedAt, type, token, transactionHash, marketplace, priceInEth, icy_cursor, collection, notes, ds) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    elif table=='icy_stats':
        return ("REPLACE INTO icy_stats (contractAddress, fromAddress, toAddress, estimatedConfirmedAt, type, token, transactionHash, marketplace, priceInEth, icy_cursor, collection, notes, ds) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")


def writeDftoMySQL(query, data, n):
    list_df = [data[i:i+n] for i in range(0,data.shape[0],n)]
    for df in list_df:
        print(df)
        cursor.executemany(query, list(df.to_records(index=False)))
        cnxn.commit()  # and commit changes

def getExistingData(table):
    cursor.execute('select * from {}'.format(table))
    res = []
    for row in cursor:
        res.append(row)
    return pd.DataFrame.from_records(res)    

# now we establish our connection
cnxn = mysql.connector.connect(**config)
cnxn.set_converter_class(NumpyMySQLConverter)
cursor = cnxn.cursor()  # initialize connection cursor
# table dict
table_dict = {
    'icy_transactions':['contractAddress', 'fromAddress', 'toAddress','transactionHash'],
    'icy_stats':['contractAddress', 'fromAddress', 'toAddress','transactionHash'],
    'token_metadata':['contractAddress', 'token']
}

def cleanAndDedupe(ex, dx, table, table_dict):
    
    dx = dx.drop_duplicates(subset=table_dict.get(table))
    ex.columns=dx.columns
    ex = ex[1000:] # DELETE ---- [64549 rows x 13 columns]
    try:
        dx['estimatedConfirmedAt'].astype('object')
    except:
        pass
    fx = pd.concat([dx, ex])
    return fx.loc[~fx.duplicated(subset=table_dict.get(table))]

if __name__ == '__main__':
    
    for table in ['token_metadata']: # list(table_dict.keys()): # ['icy_transactions','icy_stats']:
        dx = pd.read_csv('data/'+table+'.csv')
        ex = getExistingData(table) # if seeding a new table from scratch use: ex = dx.loc[dx.ds=='a']
        data = cleanAndDedupe(ex, dx, table, table_dict)
        query = getInsertQuery(table)
        dropAndCreate(table)
        writeDftoMySQL(query, data, 2000)
    runTests()
    cursor.close()
    cnxn.close()
