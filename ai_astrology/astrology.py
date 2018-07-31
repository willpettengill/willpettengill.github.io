import pandas as pd
import random
from flatlib.datetime import Datetime
from flatlib.geopos import GeoPos
from flatlib.chart import Chart
from flatlib import const
from datetime import datetime 
from uszipcode import ZipcodeSearchEngine

#from pyzipcode import ZipCodeDatabase


class Dog:

	def __init__(self, bdate, btime, bplacezip):
		
		self.planet_fields={}
		self.bdate=bdate
		#self.btime=btime
		self.bplacezip=bplacezip
		#self.date_obj = pd.to_datetime(self.bdate + ' ' + self.btime)
		self.date = Datetime(str(self.date_obj.date()).replace('-','/'), str(self.date_obj.time()),'+05:00')
		
		self.get_birthplace(bplacezip)
		#self.pull_chart(bdate,self.btime)
		self.pull_chart(bdate,btime) # removed str(self.date)) # dates_list = [dt.datetime.strptime(date, '"%Y-%m-%d"').date() for date in dates]
		self.house_qualities = json.load(open('house_qualities.json'))
		self.sign_qualities = json.load(open('sign_qualities.json'))
		self.planet_qualities = json.load(open('planet_qualities.json'))
		self.sun = {'data':self.generate_planet_data(self.chart.getObject(const.SUN)),'qualities':self.planet_qualities.get('sun')}
		self.moon = {'data':self.generate_planet_data(self.chart.getObject(const.MOON)),'qualities':self.planet_qualities.get('moon')}
		self.mercury = {'data':self.generate_planet_data(self.chart.getObject(const.MERCURY)),'qualities':self.planet_qualities.get('mercury')}
		self.venus = {'data':self.generate_planet_data(self.chart.getObject(const.VENUS)),'qualities':self.planet_qualities.get('venus')}
		self.mars = {'data':self.generate_planet_data(self.chart.getObject(const.MARS)),'qualities':self.planet_qualities.get('mars')}
		self.jupiter = {'data':self.generate_planet_data(self.chart.getObject(const.JUPITER)),'qualities':self.planet_qualities.get('jupiter')}
		self.saturn = {'data':self.generate_planet_data(self.chart.getObject(const.SATURN)),'qualities':self.planet_qualities.get('saturn')}
		self.neptune = {'data':self.generate_planet_data(self.chart.getObject(const.NEPTUNE)),'qualities':self.planet_qualities.get('neptune')}
		self.pluto = {'data':self.generate_planet_data(self.chart.getObject(const.PLUTO)),'qualities':self.planet_qualities.get('pluto')}
		self.chiron = {'data':self.generate_planet_data(self.chart.getObject(const.CHIRON)),'qualities':self.planet_qualities.get('chiron')}
		self.north_node = {'data':self.generate_planet_data(self.chart.getObject(const.NORTH_NODE)),'qualities':self.planet_qualities.get('north_node')}
		self.south_node = {'data':self.generate_planet_data(self.chart.getObject(const.SOUTH_NODE)),'qualities':self.planet_qualities.get('south_node')}
		self.syzygy = {'data':self.generate_planet_data(self.chart.getObject(const.SYZYGY)),'qualities':self.planet_qualities.get('syzygy')}
		self.pars_fortuna = {'data':self.generate_planet_data(self.chart.getObject(const.PARS_FORTUNA)),'qualities':self.planet_qualities.get('pars_fortuna')}
		self.asc = {'data':self.generate_planet_data(self.chart.get(const.ASC) ),'qualities':self.planet_qualities.get('asc')}
		self.house1 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE1)),'qualities':self.house_qualities.get('house1')}
		self.house2 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE2)),'qualities':self.house_qualities.get('house2')}
		self.house3 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE3)),'qualities':self.house_qualities.get('house3')}
		self.house4 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE4)),'qualities':self.house_qualities.get('house4')}
		self.house5 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE5)),'qualities':self.house_qualities.get('house5')}
		self.house6 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE6)),'qualities':self.house_qualities.get('house6')}
		self.house7 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE7)),'qualities':self.house_qualities.get('house7')}
		self.house8 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE8)),'qualities':self.house_qualities.get('house8')}
		self.house9 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE9)),'qualities':self.house_qualities.get('house9')}
		self.house10 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE10)),'qualities':self.house_qualities.get('house10')}
		self.house11 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE11)),'qualities':self.house_qualities.get('house11')}
		self.house12 = {'data':self.generate_planet_data(self.chart.get(const.HOUSE12)),'qualities':self.house_qualities.get('house12')}
		
    # creates a new empty list for each dog
	def normalize_btime(self, t):
		tl = t.split(' ')
		time = [int(i) for i in tl[0].split(':')]
		if tl[-1].find('PM') >= 0:
			time[0] = time[0]+12
		return ['+']+time

	def pull_chart(self, date, btime):
		print(date)
		b=self.normalize_btime(btime)
		c=[int(i) for i in date.split('/')[::-1]]
		offset= ['-',5,0,0] ## modify this for non eastern time zones
		c[1], c[2] = c[2],c[1]
		self.pos = GeoPos(self.zipcode_dict["SWBoundLatitude"], self.zipcode_dict["NEBoundLongitude"])
		self.new_date_obj = Datetime(c, b, offset)
		print(self.new_date_obj)
		self.chart = Chart(self.new_date_obj, self.pos, IDs=const.LIST_OBJECTS)

	def get_birthplace(self, bplacezip):
		search = ZipcodeSearchEngine()
		zipcode = search.by_zipcode(bplacezip)
		self.zipcode_dict=zipcode
	
#	def get_offset(self, zip):
#		zcdb = ZipCodeDatabase()
#		zipcode = zcdb[zip]

	def generate_planet_data(self, planet):
		fields = {}
		try:
			fields['name']=planet.name=planet.__str__()[1:planet.__str__().find(' ')]
		except:
			pass
		try:
			fields['sign']= planet.sign
		except:
			pass
		try:
			fields['isRetrograde']= planet.isRetrograde()
		except:
			pass
		try:
			fields['isFast']= planet.isFast()
		except:
			pass
		try:
			fields['isDirect']= planet.isDirect()
		except:
			pass
		try:
			fields['element']= planet.element()
		except:
			pass
		try:
			fields['gender']= planet.gender()
		except:
			pass
		try:
			fields['movement']= planet.movement()
		except:
			pass
		
		return fields


class Email1:

	def __init__(h):
		self.bdate=bdate
	

	


#if __name__ == "__main__":
