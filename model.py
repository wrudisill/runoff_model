
import sqlite3 
import sys
import json 
import urllib 
import time
import logging
import numpy as np
import matplotlib.pyplot as plt



dBase = sqlite3.connect('test.db')
cursor = dBase.cursor()


# sql = "SELECT precip FROM weather WHERE name=? ORDER BY forecastDate"

# cursor.execute(sql, [("Great Falls")])

# foo = cursor.fetchall()  # or use fetchone()


###Rational equation

#Q = discharge cfs
#Cf = Runoff coefficient adjustment factor to account for reduction of infiltration and otherlosses during high intensity storms
#C = Runoff coefficient to reflect the ratio of rainfall to surface runoff
#i = Rainfall intensity in inches per hour (in/hr)
#A = Drainage area in acres (ac)
#Q = Cf*C*i*A




def getWeather(ts,name): 
	Arr = []
	sql = "SELECT precip FROM weather WHERE ts = ? AND name =? ORDER BY forecastDate"
	for row in cursor.execute(sql, [ts, name]):
	    Arr.append(float(row[0]))


	foo = np.array(Arr)
	Arr = np.array(Arr)
	

	Arr = np.where(Arr != 0, 10.0**(Arr/10.0)/200.0**(5.0/8.0), 0) ### values in mm/hr
		#0.0393701 in = 1mm 	
	### rational coeff
	iArr = Arr*0.0393701 ## in in/hr
	C = .2 #for 'hilly' woodlands and forests
	Cf = 1 #don't totally get this one..
	A = 10000 # made up number ~ 150 sq mile
	###
	
	Q = iArr*C*Cf*A
	
	# plot = plt.scatter(foo, Q)
	# plt.show()

	return Q
	



def getDisch(ts,name):

	sql = "SELECT discharge FROM rivers WHERE ts = ? AND name =?"

	for row in cursor.execute(sql, [ts, name]):
	    q0 = float(row[0])

	fCast = np.zeros(8)
	fCast[0] = q0

	rRate = 5.0 ###runoff rate... random constant 5cfs/day

	for i in range(1,8):
		fCast[i] = fCast[i-1]-rRate

	return fCast


# Disch = getDisch(1460084041,"Linville")
# Rain = getWeather(1460084041,"Linville")
# Total = Disch + Rain
# print Total




def getTS(name):
	q0 = []
	sql = "SELECT discharge FROM rivers WHERE name =? ORDER BY ts"
	for row in cursor.execute(sql, [name]):
	    q0.append(float(row[0]))

	print q0


getTS("Linville")








