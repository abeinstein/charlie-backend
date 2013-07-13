import json
import pickle
import pprint
import sqlite3
import time
import os
import psycopg2
import urlparse
import urllib2
from collections import defaultdict
from operator import itemgetter
from scipy.stats import poisson

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
		c.execute("SELECT * FROM crimes WHERE beat=%s", (beat,))
		for row in c:
			params = {}
			for i in range(len(row)):
				params[KEYS[i]] = row[i]
			crimes_in_beat.append(Crime(params))

		# crimes_in_beat has all the crimes from that beat!
		probabilities = get_probabilities(crimes_in_beat)
		json_file = json.dumps(probabilities)
		return json_file




def get_probabilities(crimes):
	counts = get_counts(crimes)
	open311requests = get_open311_requests()
	probs = init_probs_dict()
	for hour in counts:
		for pos in counts[hour]:
			total_count = counts[hour][pos]


			prob = calculate_probability(total_count)
			probs[hour].append({"Probability": prob, "Latitude": pos[0], "Longitude": pos[1]})

	# Now, sort by decreasing probability
	for hour in probs:
		probs[hour] = sorted(probs[hour], key=itemgetter("Probability"), reverse=True)

	return probs


def calculate_probability(total_count, ):
	''' Calculates the probability of a crime happening on a given intersection and hour of day,
	from of crimes since 2008.
	Implicitly uses the Poisson distribution MLE, but will make more explicit and assign 
	a higher weight to more recent months soon.
	'''
	NUM_MONTHS = 60
	count_per_month = float(total_count) / NUM_MONTHS
	p_dist = poisson(count_per_month)
	prob = p_dist.sf(1)
	
	return prob
	
def get_open311_requests():
	f = urllib2.urlopen('http://www.python.org/')



# def get_counts(crimes):
# 	counts = {}
# 	for c in crimes:
# 		key = (c.lat, c.long)
# 		if key not in counts:
# 			counts[key] = init_hour_dict()
# 		crime_hour = c.hour()
# 		counts[key][crime_hour] += 1
# 	return counts

def get_counts(crimes):
	counts = init_hour_dict()
	for c in crimes:
		crime_hour = c.get_hour()
		pos = (c.lat, c.long)
		counts[crime_hour][pos] += 1
	return counts

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











