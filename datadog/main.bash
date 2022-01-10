# main

python osea_data_1.py --outfolder data --outfile_prefix 'os_token_' --endpoint token --collections budverse-cans-heritage-edition,pepsi-mic-drop

python osea_data_1.py --outfolder data --outfile_prefix 'os_stats_' --endpoint stats --collections budverse-cans-heritage-edition,pepsi-mic-drop

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range budverse-cans-heritage-edition --push true --csv data/budverse-cans-heritage-edition.csv --header true

python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range pepsi-mic-drop --push true --csv data/pepsi-mic-drop.csv --header true

python icytools.py 

# icy
python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range icy_transactions --push true --append false --header true --csv data/icy_transactions.csv

# stats
python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range os_stats_endpoint --push true --append true --csv data/os_stats_budverse-cans-heritage-edition.csv --header false
python etl_1.py --sheetid 1f0_gCDLQYOTC9sr9B6y94PpljRWlZ97wvboccTpnScA --range os_stats_endpoint --push true --append true --csv data/os_stats_pepsi-mic-drop.csv --header false
