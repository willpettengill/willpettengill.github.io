# transactions: ICY
python icytools.py --endpoint transactions

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range icy_transactions --push true --append false --header true --csv data/icy_transactions.csv

# tokens: Opensea 
python osea_data_1.py --endpoint token

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range token_metadata --push true --csv data/token_metadata.csv --header true

# stats: ICY
python icytools.py --lookback 1,7 --endpoint stats --start daily --backfill 5

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range icy_stats --push true --append false --header true --csv data/icy_stats.csv

