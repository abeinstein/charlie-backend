import csv
import sqlite3

CRIMES_FILE = "chicago_crimes.csv"
SQL_CREATE = '''
CREATE TABLE crimes
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
	for data in reader:
		if (int(data[17]) >= 2008):
			print data[2]
			try:
				# NOTE TO SELF: This is suceptible to a SQL injection attack. Use sqlite3's 
				# implementation instead. Won't have any forms though, so this is not a pressing 
				# issue
				values = (int(data[0]), data[1], data[2], data[3], data[5], 
					int(data[10]), int(data[11]), int(data[17]), float(data[19]), float(data[20]))
				c.execute('INSERT INTO crimes VALUES (?,?,?,?,?,?,?,?,?,?)', values)
				conn.commit()
			except sqlite3.IntegrityError as e:
				pass
				#print e.args[0]
			except ValueError:
				pass # Empty record
		else:
			pass # Won't use values pre 2008
			
	
	conn.close()


if __name__ == "__main__":
	conn = sqlite3.connect('crimes.db')
	# UNCOMMENT TO INITIALIZE THE TABLE
	#create_table(conn)
	with open(CRIMES_FILE, 'rb') as csv_file:
		fill_table(conn,csv_file)
