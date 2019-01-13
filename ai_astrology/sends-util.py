from datetime import datetime as dt
import json

sends = json.load(open('sends.json'))
mod = []
for i in sends:
	i['ds']=dt.strptime(i['ds'],"%B %d, %Y").strftime("%Y-%m-%d")
	mod.append(i)
with open('sends.json', 'w') as fp:
		    json.dump(mod, fp)
