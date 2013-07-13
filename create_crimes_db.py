import csv
import sqlite3

CRIMES_FILE = "chicago_crimes.csv"
DATABASE_NAME = 'crimes_small.db'
SQL_CREATE = '''
CREATE TABLE crimes_small
(
crime_id INTEGER primary key,
case_id VARCHAR(255),
date VARCHAR(255),
block VARCHAR(255),
crime_type VARCHAR(255),
beat INTEGER,
district INTEGER,
year INTEGER,
lat REAL,
long REAL
);
''' 
def create_table(conn):
	''' Creates a database to hold all the crimes from 2001-2013.'''
	c = conn.cursor()
	c.execute(SQL_CREATE)
	conn.commit()
	conn.close()

def fill_table(conn, csv_file):
	''' Fills the database with relevant crime values.'''
	c = conn.cursor()
	reader = csv.reader(csv_file)
	header = reader.next()
	counter = 0
	for data in reader:
		print counter
		while (counter < 500):
			print data[2]
			try:
				values = (int(data[0]), data[1], data[2], data[3], data[5], 
					int(data[10]), int(data[11]), int(data[17]), float(data[19]), float(data[20]))
				c.execute("INSERT INTO crimes_small VALUES (?,?,?,?,?,?,?,?,?,?)", values)
				conn.commit()
				counter += 1
			except sqlite3.IntegrityError as e:
				pass
				#print e.args[0]
			except ValueError:
				pass # Empty record
		else:
			pass # Won't use values pre 2008
			
	
	conn.close()


if __name__ == "__main__":
	conn = sqlite3.connect(DATABASE_NAME)
	# UNCOMMENT TO INITIALIZE THE TABLE
	create_table(conn)
	with open(CRIMES_FILE, 'rb') as csv_file:
		fill_table(conn,csv_file)
