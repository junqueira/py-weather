#!/usr/bin/python
import threading
from datetime import datetime
import configuration as config
import os, time, json
import sys, math
import requests, httplib2
import pandas as pd
import pymongo


open_weather_API_endpoint = "http://api.openweathermap.org/"
base_url = 'http://api.openweathermap.org/data/2.5/weather'
api_key = '4d39e6ca908778de52dfbb2a9d9ee7d4'  #your API key: http://openweathermap.org/appid
freezing_temperature = 2
city_names = config.locations
frequency = config.refresh_frequency


def get_temperature(city):
	query = base_url + '?q=%s&units=metric&APPID=%s' % (city, api_key)
	try:
		response = requests.get(query)
		# print("[%s] %s" % (response.status_code, response.url))
		if response.status_code != 200:
			response = 'N/A'
			return response
		else:
			weather_data = response.json()
		return weather_data
	except requests.exceptions.RequestException as error:
		print(error)
		sys.exit(1)


def thread_for_5_days_forecast():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    alerts = {"rain":[],"snow":[],"freezing_temperature":[]}   
    for city in city_names:
        url = open_weather_API_endpoint+"/data/2.5/forecast?q="+city+"&appid="+api_key
        http_initializer = httplib2.Http()
        response, content = http_initializer.request(url,'GET')
        utf_decoded_content = content.decode('utf-8')
        json_object = json.loads(utf_decoded_content)

        #Mongodb database
        db = client.weather_data
        # put API data in database (pk: timestamp)
        for element in json_object["list"]:
            try:
                datetime = element['dt']
                del element['dt']
                db['{}'.format(city)].insert_one({'_id':datetime,"data":element})
            except pymongo.errors.DuplicateKeyError:
                continue

        for a in db['{}'.format(city)].find({}):
            temperature = (float(a["data"]["main"]["temp"]) - 273.15)*(9/5)+32 
            if temperature<freezing_temperature:
                alerts["freezing_temperature"].append("temperatura de congelamento "+ temperature +" in "+city+" on "+str(a["data"]["dt_txt"]).split(" ")[0]+" at "+str(a["data"]["dt_txt"]).split(" ")[1])
            elif a["data"]["weather"][0]["main"]=="Rain":
                alerts["rain"].append("Chuva esperada em "+city+" on "+str(a["data"]["dt_txt"]).split(" ")[0]+" at "+str(a["data"]["dt_txt"]).split(" ")[1])
            elif a["data"]["weather"][0]["main"]=="Snow":
                alerts["snow"].append("Neve esperada em "+city+" on "+str(a["data"]["dt_txt"]).split(" ")[0]+" at "+str(a["data"]["dt_txt"]).split(" ")[1])

    print("*********ALERTAS METEOROLÓGICOS********")
    if len(alerts["freezing_temperature"])>0:
        for i in alerts["freezing_temperature"]:
            print(i)
            
    if len(alerts["rain"])>0:
        for i in alerts["rain"]:
            print(i)
            
    if len(alerts["snow"])>0:
        for i in alerts["snow"]:
            print(i)

def main():
	name=[]
	temp=[]
	description=[]
	temp_min=[]
	temp_max=[]
	humidity=[]
	for city in city_names:
		location = get_temperature(city)
		name.append(location['name'])
		temp.append(str(math.ceil(location['main']['temp'])) + 'C')
		description.append(location["weather"][0]["description"])
		temp_min.append(str(location["main"]["temp_min"]))
		temp_max.append(str(location["main"]["temp_max"]))
		humidity.append(str(location["main"]["humidity"]))
		df_weather = pd.DataFrame(list(zip(
			name, temp, description, temp_min, temp_max, humidity)), 
			columns = ['name', 'temp', 'description', 'temp_min', 'temp_max', 'humidity']
		)
	# os.system('clear')
	myobj = datetime.now()
	print('Horas:', myobj.hour, ':', myobj.minute, ':', myobj.second, '\n')
	print(df_weather)



if __name__ == '__main__':
	main()
	while 1:
		try:
			t1 = threading.Thread(target = lambda : thread_for_5_days_forecast(), name='t1')
			t1.setDaemon(True)
			t1.start()
			t1.join()
			time.sleep(frequency) 

		except Exception as e:
			print(e)
