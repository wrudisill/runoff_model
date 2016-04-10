#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import sqlite3 
import sys
import json 
import urllib 
import time
import logging


# url = "http://maps.googleapis.com/maps/api/geocode/json?address=googleplex&sensor=false"

theJson = "river.json"
dBase = sqlite3.connect('test.db')
cursor = dBase.cursor()

# cursor.execute('''CREATE TABLE rivers(ts DATE, id INTEGER, discharge INTEGER, name TEXT, lat TEXT, lon TEXT, gauge_time DATE)''')
# cursor.execute('''CREATE TABLE weather(ts DATE, id INTEGER, name TEXT, precip INTEGER,  forecastDate TEXT)''')


logging.basicConfig(filename='river.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.warning('started')


timestamp = int(time.time())



def getJ(file):

	Dict = {} ### array for storing station ids and names... seems kind of unnecessary to do it this way
	
	############ open local JSON file, return error if not 
	try: 
		with open(file) as F: 
			foo =  json.load(F)
			for i in foo:
				Dict[i["id"]] = i["name"]
			
	except (IOError, ValueError):
		logging.debug('Failed to load ' + theJson)
		exit()

	#############

	ids = ",".join(map(str, Dict.keys()))  ###get station ids, make into string	
	usgsURL = "http://waterservices.usgs.gov/nwis/iv/?format=json&sites="+ ids +"&parameterCd=00060,00061"



	try:
		response = urllib.urlopen(usgsURL)  
		data = json.loads(response.read())

		##############
		for i in data["value"]["timeSeries"]:

			name = Dict[i["sourceInfo"]["siteCode"][0]["value"]]
			stat_id = i["sourceInfo"]["siteCode"][0]["value"]

			lat = i["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"]
			lon = i["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"]
			gauge_time = i["values"][0]["value"][0]["dateTime"]
			discharge = i["values"][0]["value"][0]["value"]
			

			##### commit discharge data to riv table  
			cursor.execute('''INSERT INTO rivers(ts, id, discharge, name, lat, lon, gauge_time)
	                  VALUES(?,?,?,?,?,?,?)''', (timestamp, stat_id, discharge, name, lat, lon, gauge_time))
			#####

			coords = str(lat) + ',' + str(lon)
			dsURL = "https://api.forecast.io/forecast/fcc0b61a4a6d5351fd1ce1e11aa752a0/"+coords

			### commit weather data to weather table 
			try:
			    jsonString = urllib.urlopen(dsURL).read()
			    weather = json.loads(jsonString)

			    for j in weather['daily']['data']:
			    	
			    	precipIntensity = j['precipIntensity']
			    	forecastDate = j['time']

			    	# foo = time.ctime(forecastDate) #converts unix time to human readable
			    	
			    	cursor.execute('''INSERT INTO weather(ts, id, precip, name, forecastDate)
	                  VALUES(?,?,?,?,?)''', (timestamp, stat_id, precipIntensity, name, forecastDate))


			except (IOError, ValueError):
			    logging.warning("Connection failure to %s" % dsURL)
			    exit()

		dBase.commit()
		dBase.close()
		logging.warning('success')

	#####end try 

	except (IOError, ValueError):
	   logging.debug("Connection failure to %s" % usgsURL)
	   exit()



getJ(theJson)





