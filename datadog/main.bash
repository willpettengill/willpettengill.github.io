# transactions: ICY
python icytools.py --endpoint transactions

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range icy_transactions --push true --append false --header true --csv data/icy_transactions.csv

# tokens: Opensea 
python osea_data_1.py --outfolder data --outfile_prefix 'os_token_' --endpoint token --collections budverse-cans-heritage-edition,pepsi-mic-drop 

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range token_metadata --push true --csv data/os_token_budverse-cans-heritage-edition.csv --header true

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range token_metadata --push true --csv data/os_token_pepsi-mic-drop.csv --header false --append True

# stats: ICY
python icytools.py --lookback 1,7 --endpoint stats --start daily --backfill 3

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range icy_stats --push true --append false --header true --csv data/icy_stats.csv
