import json
import math
import os
import pickle
import pprint
import psycopg2
import re
import sqlite3
import time
import urllib2
import urlparse

from collections import defaultdict
from operator import itemgetter
#from scipy.stats import poisson Heroku wouldn't play nicely with scipy :(

DATABASE_NAME = 'crimes.db'
TABLE_NAME = 'crimes'
NUM_WEEKS = 60
EXAMPLE_PICKLE_FILE = 'pickles/sql_output.pkl'
KEYS = ["crime_id", "case_id", "date", "block", "crime_type", "beat", "district", 
	"year", "lat", "long"]




class Crime():
	def __init__(self, params):
		''' Initializes a Crime object. Params is a dictionary with the following keys:
		crime_id: unique crime identifier
		case_id: case identifier
		date: date string
		block: address string
		crime_type: (i.e. BATTERY, BURGLARY, HOMICIDE, etc)
		beat: Number indicating beat (patrol zone)
		district: Number indicating district (contains many beats)
		year: Year crime committed
		lat: latitude coordinate
		long: longitutde coordinate
		'''
		self.crime_id = params['crime_id']
		self.case_id = params['case_id']
		self.date = params['date']
		self.block = params['block']
		self.crime_type = params['crime_type']
		self.beat = params['beat']
		self.district = params['district']
		self.year = params['year']
		self.lat = params['lat']
		self.long = params['long']

	def get_hour(self):
		t = time.strptime(self.date, "%m/%d/%Y %I:%M:%S %p")
		hour = t.tm_hour
		return hour

class Intersection():
	def __init__(self, params):
		self.lat = params['lat']
		self.long = params['long']
		self.block = params['block']
		self.crimes = []
		self.num_311_service_requests = 0
		self.probability = 0

	def __hash__(self):
		return hash(self.block)



	def add_crime(self, crime_object):
		self.crimes.append(crime_object)




def init_database():
	urlparse.uses_netloc.append("postgres")
	if "DATABASE_URL" in os.environ: # production
		url = urlparse.urlparse(os.environ["DATABASE_URL"])

		conn = psycopg2.connect(
		    database=url.path[1:],
		    user=url.username,
		    password=url.password,
		    host=url.hostname,
		    port=url.port
		)
	else: # development
		conn = psycopg2.connect("dbname=crimes user=abeinstein")

	c = conn.cursor()
	return c



def get_data(beat):
	c = init_database()
	if (get_cached_data(beat)):
		# TODO
		pass
	else: 
		#sql_select = "SELECT * FROM %s WHERE beat=%d AND year=2013" % (TABLE_NAME, int(beat))

		crimes_in_beat = []
		intersections = {}
		c.execute("SELECT * FROM crimes WHERE beat=%s", (beat,))
		for row in c:
			crime_params = {}
			for i in range(len(row)):
				crime_params[KEYS[i]] = row[i]
			new_crime = Crime(crime_params)
			crimes_in_beat.append(new_crime)

			if new_crime.block not in intersections:
				int_params = {"lat": new_crime.lat, "long": new_crime.long, "block": new_crime.block}
				new_intersection = Intersection(int_params)
				new_intersection.add_crime(new_crime)
				intersections[new_crime.block] = new_intersection
			else:
				intersection = intersections[new_crime.block]
				intersection.add_crime(new_crime)


		intersections = process_open311_requests(intersections)
		probabilities = get_probabilities(crimes_in_beat, intersections)
		json_file = json.dumps(probabilities)
		return json_file


def get_probabilities(crimes, intersections):
	counts = get_counts(crimes, intersections)
	probs = init_probs_dict()
	for hour in counts:
		for inter in counts[hour]:
			total_count = counts[hour][inter]
			prob = calculate_probability(total_count, inter)
			probs[hour].append({"Probability": prob, "Latitude": inter.lat, "Longitude": inter.long})

	# Now, sort by decreasing probability
	for hour in probs:
		probs[hour] = sorted(probs[hour], key=itemgetter("Probability"), reverse=True)

	return probs

def get_counts(crimes, intersections):
	counts = init_hour_dict()
	for c in crimes:
		crime_hour = c.get_hour()
		# pos = (c.lat, c.long)
		inter = intersections[c.block]
		counts[crime_hour][inter] += 1
	return counts


def calculate_probability(total_count, inter):
	''' Calculates the probability of a crime happening on a given intersection and hour of day,
	from of crimes since 2008.
	Implicitly uses the Poisson distribution MLE, but will make more explicit and assign 
	a higher weight to more recent months soon.
	'''
	NUM_MONTHS = 60
	count_per_month = float(total_count) / NUM_MONTHS
	prob = poisson_sf(count_per_month, 1) # survival function (probability of at least 1 crime)


	# Simple weighting based on Broken Windows Theory
	num_open_reports = inter.num_311_service_requests
	if (num_open_reports > 3):
		prob *= 2
	elif (num_open_reports > 1):
		prob *= 1.5
	elif (num_open_reports == 0):
		prob *= 0.8


	
	return prob



def poisson_sf(mu, k):
	# ''' SciPy wasn't playing nicely with heroku, so this is my own implmentation of a poisson survival function.
	# This is 1 - cdf (used for the probability of >= 1 counts)
	# '''
	mu = float(mu) # just making sure!
	first_term = math.e ** (-mu)
	second_term = 0
	for i in range(int(math.ceil(k)) + 1):
		second_term += (mu ** i) / (math.factorial(i))

	return 1 - (first_term * second_term)


def process_open311_requests(intersections):
	requests = get_open311_requests()
	for req in requests:
		if "address" in req:
			block = parse_311_address(req["address"])
			if block in intersections:
				inter = intersections[block]
				inter.num_311_service_requests += 1
			else:
				params = {"lat": req["lat"], "long": req["long"], "block": block}
				new_intersection = Intersection(params)
				intersections[block] = new_intersection
		else:
			pass # Don't have enough information
	return intersections



def parse_311_address(full_address):
	exp = re.compile('^(\d+) ([^,]+),')
	m = exp.search(full_address)
	if (m):
		groups = m.groups()
		address_number = groups[0]
		address_street = groups[1]
		print address_number, address_street

		address_number = address_number[:-2] + "XX"
		while len(address_number) < 5:
			address_number = "0" + address_number

		return "%s %s" % (address_number, address_street)
	else:
		return ""


	
def get_open311_requests():
	requests = []
	try:
		pickle_file = open("311requests.pkl", 'rb')
		requests = pickle.load(pickle_file)
	except IOError:
		page_num = 1
		is_finished = False
		url = 'http://311api.cityofchicago.org/open311/v2/requests.json?start_date=2013-06-13&status=open&page_size=500&page=1'
		while not is_finished:
			f = urllib2.urlopen(url)
			json_string  = f.read()
			requests_object = json.loads(json_string)
			if len(requests_object) > 0:
				requests.extend(requests_object)
				page_num += 1
				url = url[:-1]
				url += str(page_num)
			else:
				is_finished = True

		pickle_file = open("311requests.pkl", 'wb')
		pickle.dump(requests, pickle_file)
	return requests



def init_probs_dict():
	probs = {}
	for i in range(24):
		probs[i] = []#defaultdict(list)
	return probs


def init_hour_dict():
	hours = {}
	for i in range(24):
		hours[i] = defaultdict(int)
	return hours



def get_cached_data(beat):
	'''Gets data from cache, if it exists'''
	return False



if __name__ == "__main__":
	pass
	#print get_open311_requests()
	#print parse_311_address("7120 W DIVERSEY AVE, CHICAGO, IL, 60707")
	#print get_data(2523)
	# #c = init_database()
	# # rows = []
	# # for row in c.execute("SELECT * FROM crimes WHERE beat=2523 AND YEAR=2013"):
	# # 	rows.append(row)
	# # pickle.dump(rows, open(EXAMPLE_PICKLE_FILE, 'wb'))
	# rows = pickle.load(open(EXAMPLE_PICKLE_FILE, 'rb'))
	# crimes_in_beat = []

	# for row in rows:
	# 	params = {}
	# 	for i in range(len(row)):
	# 		params[keys[i]] = row[i]
	# 	crimes_in_beat.append(Crime(params))

	# pprint.pprint(get_probabilities(crimes_in_beat))











