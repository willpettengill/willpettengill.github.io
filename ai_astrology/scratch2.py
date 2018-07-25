Willy = Dog(udf.birthdate[0], '2:19:00 AM', '02461')
Julia = Dog('03/03/1988', '7:47:00 PM', '20007')








Datetime(str(udf.birthdate[0]))
pd.to_datetime(udf.birthdate[0] + ' ' + udf.birthtime[0])

date = Datetime(pd.to_datetime(bdate + ' ' + btime).strftime('%Y-%m-%d-%H-%M-%S'))

bdate=udf.birthdate[0]
btime=udf.birthtime[0]
bplacezip='01776'
date_obj = pd.to_datetime(bdate + ' ' + btime)
date = Datetime(str(date_obj.date()).replace('-','/'), str(date_obj.time()),'+00:00')


def get_birthplace(bplacezip):
	search = ZipcodeSearchEngine()
	zipcode = search.by_zipcode(bplacezip)
	return zipcode

zips = get_birthplace(bplacezip)	


print(udf.birthdate[0])

chart = Chart(date, pos, IDs=const.LIST_OBJECTS)

lambda b:  [int(i) for i in  b[:-3].split(':')]


def replace_element(li, ch, num, repl):
    last_found = 0
    for _ in range(num):
        last_found = li.index(ch, last_found+1)
    li[li.index(ch, last_found)] = repl


    '/'.join(c.split('/')[::-1])

search = ZipcodeSearchEngine()
zipcode = search.by_zipcode('02461')
zipcode_dict=zipcode

new_date_obj = Datetime(c)

b=btime[:-3]
c=[int(i) for i in udf.birthdate[0].split('/')[::-1]]



self.pos = GeoPos(self.zipcode_dict["SWBoundLatitude"], self.zipcode_dict["NEBoundLongitude"])
print(self.pos.lat)
print(self.pos.lon)
self.new_date_obj = Datetime(c,b)

test=Datetime([2018,6,20])


c=[int(i) for i in udf.birthdate[0].split('/')[::-1]]
c[1], c[2] = c[2],c[1]
pos = GeoPos(zipcode_dict["SWBoundLatitude"], zipcode_dict["NEBoundLongitude"])
print(self.pos.lat)
print(self.pos.lon)
new_date_obj = Datetime(c)
print(self.new_date_obj.date)
print(self.new_date_obj.time)
print(self.new_date_obj.utcoffset)
chart = Chart(new_date_obj, pos, IDs=const.LIST_OBJECTS)
planet =  generate_planet_data(chart.getObject(const.JUPITER))


def generate_planet_data(planet):
	fields = {}
	try:
		planet.name=planet.__str__()[1:planet.__str__().find(' ')]
	except:
		pass
	try:
		fields[planet.name+'_sign']= planet.sign
	except:
		pass
	try:
		fields[planet.name+'_retrograde']= planet.isRetrograde()
	except:
		pass
	try:
		fields[planet.name+'_isFast']= planet.isFast()
	except:
		pass
	try:
		fields[planet.name+'_direct']: planet.isDirect()
	except:
		pass
	try:
		fields[planet.name+'_element']: planet.element()
	except:
		pass
	try:
		fields[planet.name+'_gender']: planet.gender()
	except:
		pass
	try:
		fields[planet.name+'_movement']: planet.movement()
	except:
		pass
	
	return fields
	