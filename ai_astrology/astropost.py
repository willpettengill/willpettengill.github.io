from datetime import datetime
import pandas as pd
from collections import Counter
from markdown import markdown # pip install markdown==2.6.11
import requests # pip install requests
import jwt	# pip install pyjwt
from datetime import datetime as dt
import utils
import json
import random
# Admin API key goes here
def generate_post_content(cnt_vars, template_file):
	b=open(template_file,'r').read()
	c=b.format(*cnt_vars)
	return c

def write_post(html_str, slug, title, tags, updated_ts):
	a= {
      "slug": slug,
      "title": title,
      "mobiledoc": "{\"version\":\"0.3.1\",\"atoms\":[],\"cards\":[],\"markups\":[],\"sections\":[[1,\"p\",[[0,[],0,\"My post content. Work in progress...\"]]]]}",
      "html": html_str,
      "feature_image": "https://static.ghost.org/v3.0.0/images/welcome-to-ghost.png",
      "status": "published",
      "visibility": "public",
      "custom_excerpt": 'null',
      "codeinjection_head": 'null',
      "codeinjection_foot": 'null',
      "custom_template": 'null',
      "canonical_url": 'null',
      "send_email_when_published": 'false',
      "tags": tags,
      "updated_at": updated_ts
        }
	return a

#def main():

# Connect to Ghost
key ='5f6e69e8d028380039ec5a1e:ae140940a9490564516290add11970949bfe19c41f96b71253dc28838b8b9c4c'
api_url='https://williampettengill-2.ghost.io'+'/ghost/api/v3/admin/posts'
post_id= '/5fa351793f8e710039569573'
html_parameter='?source=html'
id, secret = key.split(':')
iat = int(dt.now().timestamp())
header = {'alg': 'HS256', 'typ': 'JWT', 'kid': id}
payload = {
    'iat': iat,
    'exp': iat + 5 * 60,
    'aud': '/v3/admin/'
}
token = jwt.encode(payload, bytes.fromhex(secret), algorithm='HS256', headers=header)
url = api_url+post_id+html_parameter
url = api_url+html_parameter
headers = {'Authorization': 'Ghost {}'.format(token.decode())}

# Import astrological data (daily & evergree)
quals = pd.read_csv('sun_qualities.csv').drop(['Unnamed: 0'], axis=1).set_index('Quality')

with open('today_data.txt') as file:
	td = json.load(file) # today's astrological data

def df_pct_col(df):
	total = df.cnt.sum()
	df['pct'] = df['cnt'].map(lambda x: str(round(x/total,2)*100)+' %' )
	return df

# Data and calculations
signs = ['Cancer', 'Scorpio', 'Pisces', 'Aries', 'Leo', 'Taurus', 'Virgo', 'Gemini', 'Libra', 'Aquarius']
signs = [i.lower() for i in signs]
bodies = list(td.keys())
sign_ele=dict(utils.sign_ele.items())

gender_counts={x:quals[y.get('sign')]['Duality'] for (x,y) in td.items()}
gnd_cnt = Counter(gender_counts.values())
gnd_common = gnd_cnt.most_common()
df5 = pd.DataFrame.from_records(gnd_common)
df5.columns=['duality','cnt']
df5=df_pct_col(df5)

name_sign={ td.get(i).get('name'):td.get(i).get('sign') for (i,x) in td.items()}
ns_counts = Counter(name_sign.values())
ns_common = ns_counts.most_common()
df4 = pd.DataFrame.from_records(ns_common)
df4.columns=['sign','cnt']
df4 = df_pct_col(df4)

name_element={i: sign_ele.get(x.get('sign')) for (i,x) in td.items()}
el_counts = Counter(name_element.values())
el_common = el_counts.most_common()
df3 = pd.DataFrame.from_records(el_common)
df3.columns=['element','cnt']
df3=df_pct_col(df3)

planets = ['sun', 'moon', 'mercury', 'venus', 'mars', 'jupiter', 'saturn', 'neptune', 'pluto']
planet_sign = [(i, td.get(i).get('sign')) for i in planets]
df2 = pd.DataFrame.from_records(planet_sign)
df2.columns=['planet','sign']

houses = ['house1','house2','house3','house4','house5','house6','house7','house8','house9','house10','house11','house12']
house_sign = [(i, td.get(i).get('sign'), td.get(i).get('qualities')) for i in houses]
df1 = pd.DataFrame.from_records(house_sign)
df1.columns = ['house','sign','governs']
df1['governs'] = df1['governs'].map(lambda x: ', '.join(random.sample(x, 3)))

# assemble content dataframe
html1=df1.to_html(index=False) # house/sign/governs
html2=df2.to_html(index=False) # planet/sign
html3=df3.to_html(index=False) # element/count/pct
html4=df4.to_html(index=False) # sign/count/pct
html5=df5.to_html(index=False) # gender
tbl = [html1,html2,html3,html4]

# assemble today's post content
today = datetime.today().strftime('%Y-%m-%d')
ds = dt.today().strftime("%a %b %d") # full string
updated_ts = str(pd.Timestamp.now()).replace(' ','T')+'Z'
sunsign = td.get('sun').get('sign')
moonsign = td.get('moon').get('sign')
topelement = df3.element[0].capitalize()
planetsign = html2
gender = html5

# post for today's main feed
cnt_vars = [today, sunsign, moonsign, topelement, html2]
md_text = generate_post_content(cnt_vars, 'post_example_main.md')
html = markdown(md_text, extensions=['tables'])
title = 'Daily Update: {0}'.format(ds)
slug = 'daily-update-{0}'.format(today)
tags = ['daily-update']
post=write_post(html, slug, title, tags, updated_ts)
# Ghost API Endpoint: Posts
r = requests.post(url, json={'posts': [post]}, headers=headers)
print(r)	
si_list=list(set(df2.sign.unique().tolist()+df1.sign.unique().tolist()))
# assemble sign-specific post content 
for sign in si_list[:1]:
	houses_ = df1.loc[df1.sign==sign].to_html(index=False)
	planets_ = df2.loc[df2.sign==sign].to_html(index=False)
	cnt_vars = [sign, today, planets_, houses_]
	md_text = generate_post_content(cnt_vars, 'post_example.md')
	html = markdown(md_text, extensions=['tables'])
	title = 'Today in {1}: {0}'.format(ds, sign)
	slug = 'today-in-{0}-{1}'.format(sign.lower(), today)
	tags = [sign.lower()]
	updated_ts = str(pd.Timestamp.now()).replace(' ','T')+'Z'
	post=write_post(html, slug, title, tags, updated_ts)

	# Ghost API Endpoint: Posts
	body = {'posts': [post]}
	r = requests.post(url, json=body, headers=headers)
	print(r)	



#main()	