import random
from flatlib.datetime import Datetime
from flatlib.geopos import GeoPos
from flatlib.chart import Chart
from flatlib import const
from datetime import datetime 

user='willpettengill@gmail.com'
date = Datetime('1988/06/20', '4:20', '+00:00')
pos = GeoPos(42.3370, 71.2092)
chart = Chart(date, pos, IDs=const.LIST_OBJECTS)

house_qualities = json.load(open('house_qualities.json'))
sign_qualities = json.load(open('sign_qualities.json'))
planet_qualities = json.load(open('planet_qualities.json'))


sun = chart.getObject(const.SUN)
moon = chart.getObject(const.MOON)
mercury = chart.getObject(const.MERCURY)
venus = chart.getObject(const.VENUS)
mars = chart.getObject(const.MARS)
jupiter = chart.getObject(const.JUPITER)
saturn = chart.getObject(const.SATURN)
#uranus = chart.getObject(const.URANUS)
neptune = chart.getObject(const.NEPTUNE)
pluto = chart.getObject(const.PLUTO)
chiron = chart.getObject(const.CHIRON)
north_node = chart.getObject(const.NORTH_NODE)
south_node = chart.getObject(const.SOUTH_NODE)
syzygy = chart.getObject(const.SYZYGY)
pars_fortuna = chart.getObject(const.PARS_FORTUNA)
asc=chart.get(const.ASC).sign 
house1 = chart.get(const.HOUSE1).sign
house2 = chart.get(const.HOUSE2).sign
house3 = chart.get(const.HOUSE3).sign
house4 = chart.get(const.HOUSE4).sign
house5 = chart.get(const.HOUSE5).sign
house6 = chart.get(const.HOUSE6).sign
house7 = chart.get(const.HOUSE7).sign
house8 = chart.get(const.HOUSE8).sign
house9 = chart.get(const.HOUSE9).sign
house10 = chart.get(const.HOUSE10).sign
house11 = chart.get(const.HOUSE11).sign
house12 = chart.get(const.HOUSE12).sign






def generate_planet_data(planet, fields):
	planet.name=planet.__str__()[1:planet.__str__().find(' ')]

	planet_fields = {
	planet.name+'_sign': planet.sign,
	planet.name+'_retrograde': planet.isRetrograde(),
	planet.name+'_isFast': planet.isFast(),
	planet.name+'_direct': planet.isDirect(),
	planet.name+'_element': planet.element(),
	planet.name+'_gender': planet.gender(),
	planet.name+'_movement': planet.movement()
	}
	fields.update(planet_fields)


if __name__ == "__main__":

	planets = [sun, moon, mercury, venus, mars, jupiter, saturn, neptune, pluto]
	fields = {}

	for planet in planets:
		generate_planet_data(planet, fields)