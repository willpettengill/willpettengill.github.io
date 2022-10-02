import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from sklearn import preprocessing

def process_rec(rec):
	
	t1slash=rec[0].index('/')
	t2slash=rec[1].index('/')	
	try:
		print(rec)
		int(rec[0][-1])
	except:
		print(rec)		
	dd = {
	'line': rec[0][1]
	,'t1score': rec[0][-1]
	,'t2score': rec[1][-1]
	,'t1p1': '_'.join([rec[0][2].lower(),rec[0][3].lower()])
	,'t1p2': '_'.join([rec[0][t1slash+1].lower(),rec[0][t1slash+2].lower()])
	,'t2p1': '_'.join([rec[1][0].lower(),rec[1][1].lower()])
	,'t2p2': '_'.join([rec[1][t2slash+1].lower(),rec[1][t2slash+2].lower()])
	}
	print(dd)
	return(dd)

def winloss(row):
	t1=[int(i) for i in row['t1score'] if i not in ['(',')']]
	t2=[int(i) for i in row['t2score'] if i not in ['(',')']]
	if len(t1) == 2:
		diff = ((t1[0] - t2[0]) + (t1[1] - t2[1]))/2
	elif len(t1) == 3:
		diff = 	((t1[0] - t2[0]) + (t1[1] - t2[1]) + (t1[2] - t2[2]))/3
	else:
		diff = 0
	return diff

def scrapeFCTL(hrefs):
	games = []
	for urlstring in hrefs:
		url = 'https://fctlmen.tenniscores.com/'+urlstring
		response = requests.get(url)
		page = response.text
		soup = BeautifulSoup(page, 'lxml')
		tables = soup.find_all(class_="standings-table2")
		hds = soup.find(class_="datelocheader")
		teams = hds.contents[0][:hds.contents[0].find((':'))].split(' @ ')
		for table in tables:
			rec = [row.text.split() for row in table.find_all("tr")]
			try:
				dd=process_rec(rec)
				dd['team1']=teams[0].strip()
				dd['team2']=teams[1].strip()
				if int(dd.get('t1score'))>0:
					games.append(dd)
			except:
				print(rec)
	return games

def getHREFS(baseurl):
	baseresponse = requests.get(baseurl)
	basesoup = BeautifulSoup(baseresponse.text, 'lxml')
	basehrefs = basesoup.find_all(class_="lightbox-760-tall iframe link")
	hrefs = set([i.get('href') for i in basehrefs])
	return hrefs

def getDF(games):
	df = pd.DataFrame.from_records(games)
	df['line'] = df.line.astype(int)
	df['t1_points'] = df.apply(lambda row: winloss(row), axis=1)
	df['t2_points'] = df.t1_points*-1
	pdfs = []
	for col in ['t1p1', 't1p2']:
		xf = df.groupby(col).agg({'t1_points':np.sum, 'line':np.sum, 'team2':'count', 'team1':'max'})
		xf=xf.reset_index()
		xf.columns = ['player','points', 'line','match_cnt', 'team']
		pdfs.append(xf)
	for col in ['t2p1', 't2p2']:
		xf = df.groupby(col).agg({'t2_points':np.sum, 'line':np.mean, 'team1':'count', 'team2':'max'})
		xf=xf.reset_index()
		xf.columns = ['player','points', 'line','match_cnt', 'team']
		pdfs.append(xf)	

	pf = pd.concat(pdfs)
	pf = pf.groupby('player').agg({ 'points':'sum', 'line':'sum', 'match_cnt': 'sum', 'team':'max'}).reset_index()
	pf['avg_points'] = pf.points/pf.match_cnt
	pf['line_quality'] = 5-pf.line/pf.match_cnt
	pf['team_quality'] = pf.team.map(tf.rank())

	return df, pf

def getNNF(pf):
	nf=pf[['avg_points','line_quality','team_quality']].set_index(pf.player)
	x = nf.values #returns a numpy array
	min_max_scaler = preprocessing.MinMaxScaler()
	x_scaled = min_max_scaler.fit_transform(x)
	nnf = pd.DataFrame(x_scaled)
	nnf=nnf.set_index(pf.player).reset_index()
	nnf.columns = ['player','avg_points','line_quality','team_quality']

	pdd=pf[['player','team']].set_index('player')
	pdx=pf[['player','match_cnt']].set_index('player')
	nnf['team'] = nnf.player.map(pdd.team)
	nnf['match_cnt'] = nnf.player.map(pdx.match_cnt)
	nnf = nnf.loc[~nnf.player.str.contains('forfeit')]

	nnf['raw_score'] = nnf['avg_points']*.5+nnf['line_quality']*.25+nnf['team_quality']*.25
	nnf['power_rank'] = nnf.raw_score.rank()
	nnf=nnf.sort_values(by='power_rank', ascending=False).reset_index(drop=True)
	nnf['power_rank_pct'] = round(nnf.raw_score.rank(pct=True)*100,0)
	return nnf

baseurls = {
'div3open': 'https://fctlmen.tenniscores.com/?mod=nndz-TjJiOWtOR3QzTU4yakRrY1NjN1FMcGpx&did=nndz-WnlXNHc3MD0%3D',
'div2open': 'https://fctlmen.tenniscores.com/?mod=nndz-TjJiOWtOR3QzTU4yakRrY1NjN1FMcGpx&did=nndz-WnlXNHc3ND0%3D',
'div2fifty': 'https://fctlmen.tenniscores.com/?mod=nndz-TjJiOWtOR3QzTU4yakRrY1NjN1FMcGpx&did=nndz-WnlXNHc3Zz0%3D',
'div3fifty': 'https://fctlmen.tenniscores.com/?mod=nndz-TjJiOWtOR3QzTU4yakRrY1NjN1FMcGpx&did=nndz-WnlXNHdydz0%3D'
}


for league in ['div3open','div2open','div2fifty','div3fifty']:
	hrefs = getHREFS(baseurls[league])
	games = scrapeFCTL(hrefs)
	df, pf = getDF(games)
	tf = df.groupby("team1").sum()['t1_points'] + df.groupby("team2").sum()['t2_points']
	nnf=getNNF(pf)
	df.to_csv(league+'_games'+'.csv')
	pf.to_csv(league+'_players'+'.csv')
	nnf.to_csv(league+'_rankings'+'.csv')




# analysis
#nnf[:50]
#nnf.loc[nnf.team.str.contains('Rowayton')]



