#final assignment for Erin Mahoney

import urllib.request as urllib
import json, codecs, time
import sqlite3
conn = sqlite3.connect('csc455.db')
c = conn.cursor()
c.execute ('drop table user')
c.execute('drop table tweets')
c.execute('drop table geo')

cu = '''CREATE TABLE user
(
	id TEXT,
	name TEXT,
	screen_name TEXT,
	description TEXT,
	friends_count INTEGER,

	CONSTRAINT user_pk
		PRIMARY KEY(id)
	)'''



cc = ''' CREATE TABLE tweets
(	
	geoID INT,
	created_at TEXT,
	id_str TEXT NOT NULL,
	source TEXT,
	in_reply_to_user_id INTEGER,
	in_reply_to_screen_name TEXT,
	in_reply_to_status_id INTEGER, 
	retweet_count INTEGER,
	contributors TEXT,
	text TEXT,
	userID TEXT,

	CONSTRAINT tweets_pk
		PRIMARY KEY(id_str)

	CONSTRAINT tweets_fk
		FOREIGN KEY(userID) references user(id)

	CONSTRAINT geo_fk
		FOREIGN KEY(geoID) REFERENCES geo(geo_id)

	)'''

#problem 1, part a

cg = ''' CREATE TABLE geo
(	geo_id INT NOT NULL,
	longitude INT,
	latitude INT,
	type TEXT,

	CONSTRAINT geo_pk
		PRIMARY KEY(geo_id)
)'''

c.execute(cu)
c.execute(cg)
c.execute(cc)




#Problem 1, Part b:
response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")

tempfile = codecs.open("tweetsfile.txt", "w", 'utf8')

for i in range(500000):
	st = time.time()
	str_response = response.readline().decode('utf8')
	tempfile.write(str_response)

tempfile.close()
final_time = time.time() - st
#my time for this was about 4 seconds

#part c
response = urllip.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")

tweets = []

tweet_keys = ['created_at', 'id_str', 'source', 'in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors', 'text']
user_keys = ['id_str','name', 'screen_name', 'description', 'friends_count' ] 

bad_tweets = ''

#I'm not sure if this is bad practice, but I created q as a primary key for geo and a foreign key for tweets
# I think this code would work for 500,000 tweets, but the run time's were too long
#the run time is horrifically long
def fromTheFile():
	st = time.time()
	bad_tweets = ''
	q = 1
	tweet_keys = ['created_at', 'id_str', 'source', 'in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors', 'text']
	user_keys = ['id_str','name', 'screen_name', 'description', 'friends_count' ] 


	for i in range(100000):
		str_response = response.readline().decode('utf8')
		try:
			jsonobject = json.loads(str_response)
			this_tweet = []
			user_tweet = []
			if jsonobject['geo'] not in ['null', "" ,[], None]:
				geo_tweet = [q, jsonobject['geo']['type'], jsonobject['geo']['coordinates'][0], jsonobject['geo']['coordinates'][1]]
				this_tweet.append(q)
			
				c.execute("INSERT INTO geo VALUES(?,?,?,?);", geo_tweet)
				q = q + 1
			else:
				this_tweet.append(None)

			for key in tweet_keys:
				if jsonobject[key] in ['', [], 'null']:
					this_tweet.append(None)
				else:
					this_tweet.append(jsonobject[key])
			for key in user_keys:
				if jsonobject['user'][key] in ['', [], 'null', None]:
					user_tweet.append(None)
				else:
					user_tweet.append(jsonobject['user'][key])
			if jsonobject['user']['id_str'] in ['null', '', [], None]:
				this_tweet.append(None)
			else:
				this_tweet.append(jsonobject['user']['id_str'])
			c.execute("INSERT or REPLACE INTO tweets VALUES(?,?,?,?,?,?,?,?,?,?,?);", this_tweet)
			c.execute("INSERT or REPLACE INTO user VALUES(?,?,?,?,?);", user_tweet)


		except(ValueError or UnicodeDecodeError):
			bad_tweets = str_response
	print(time.time() - st)

# part c 

response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
c.execute('drop table user')
c.execute('drop table tweets')
c.execute('drop table geo')

c.execute(cg)
c.execute(cc)
c.execute(cu)


tempfile.close()
tempfile  = codecs.open('tweetsfile.txt', 'r', 'utf8')

#this takes 0.63 seconds
def noBatching():
	q = 1
	st = time.time()
	for line in tempfile:
		str_response = tempfile.readline()
		try:
			jsonobject = json.loads(str_response)
			this_tweet = []
			user_tweet = []
			if jsonobject['geo'] not in ['null', "" ,[], None]:
				geo_tweet = [q, jsonobject['geo']['type'], jsonobject['geo']['coordinates'][0], jsonobject['geo']['coordinates'][1]]
				this_tweet.append(q)
			
				c.execute("INSERT INTO geo VALUES(?,?,?,?);", geo_tweet)
				q = q + 1
			else:
				this_tweet.append(None)

			for key in tweet_keys:
				if jsonobject[key] in ['', [], 'null']:
					this_tweet.append(None)
				else:
					this_tweet.append(jsonobject[key])
			for key in user_keys:
				if jsonobject['user'][key] in ['', [], 'null', None]:
					user_tweet.append(None)
				else:
					user_tweet.append(jsonobject['user'][key])
			if jsonobject['user']['id_str'] in ['null', '', [], None]:
				this_tweet.append(None)
			else:
				this_tweet.append(jsonobject['user']['id_str'])
			c.execute("INSERT or REPLACE INTO tweets VALUES(?,?,?,?,?,?,?,?,?,?,?);", this_tweet)
			c.execute("INSERT or REPLACE INTO user VALUES(?,?,?,?,?);", user_tweet)


		except(ValueError or UnicodeDecodeError):
			bad_tweets = str_response
	print(time.time() - st)
#part d

response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
c.execute('drop table user')
c.execute('drop table tweets')
c.execute('drop table geo')

c.execute(cg)
c.execute(cc)
c.execute(cu)


tempfile.close()
tempfile  = codecs.open('tweetsfile.txt', 'r', 'utf8')

allTweets = []
for line in tempfile:
	allTweets.append(line)

tempfile.close()

#this took 0.58 seconds 
def batchedLoading(tweet_list):

	st = time.time()

	tweet_keys = ['created_at', 'id_str', 'source', 'in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors', 'text']
	user_keys = ['id_str','name', 'screen_name', 'description', 'friends_count' ] 
	q = 1
	bad_tweets = ''

	try:
		batchedRows = 500
		batchedInsert_tweets = []
		batchedInsert_users = []
		batchedInsert_geo = []



		while len(tweet_list) > 0:

			this_tweet = []
			user_tweet = []

			line = tweet_list.pop(0)
			jsonobject = json.loads(line)

			if jsonobject['geo'] not in ['null', "" ,[], None]:
				geo_tweet = [q, jsonobject['geo']['type'], jsonobject['geo']['coordinates'][0], jsonobject['geo']['coordinates'][1]]
				this_tweet.append(q)
			
				batchedInsert_geo.append(geo_tweet)
				q = q + 1
			else:
				this_tweet.append(None)

			for key in tweet_keys:
				if jsonobject[key] in ['', [], 'null']:
					this_tweet.append(None)
				else:
					this_tweet.append(jsonobject[key])
			for key in user_keys:
				if jsonobject['user'][key] in ['', [], 'null', None]:
					user_tweet.append(None)
				else:
					user_tweet.append(jsonobject['user'][key])
			if jsonobject['user']['id_str'] in ['null', '', [], None]:
				this_tweet.append(None)
			else:
				this_tweet.append(jsonobject['user']['id_str'])
			batchedInsert_tweets.append(this_tweet)
			batchedInsert_users.append(user_tweet)

			if len(batchedInsert_tweets) > batchedRows or len(tweet_list) == 0:

				c.executemany("INSERT or REPLACE INTO tweets VALUES(?,?,?,?,?,?,?,?,?,?,?);", batchedInsert_tweets)
				c.executemany("INSERT or REPLACE INTO user VALUES(?,?,?,?,?);", batchedInsert_users)
				c.executemany("INSERT or REPLACE INTO geo VALUES(?,?,?,?);", batchedInsert_geo)

				batchedInsert_tweets = []
				batchedInsert_users = []
				batchedInsert_geo = []



	except(ValueError or UnicodeDecodeError):
		bad_tweets = str_response

	print(time.time() - st)


batchedLoading(allTweets)


#Problem 2

#part a
#i.
c.execute('select text from tweets where id_str like "%44%" or id_str like "%77%"').fetchall()
#ii.
c.execute('select count(in_reply_to_user_id) from (select distinct in_reply_to_user_id from tweets)').fetchall()
#iii.
c.execute('select text from tweets where length(text) = (select max(length(text)) from tweets)')

#iv.
c.execute('select avg(g.longitude), avg(g.latitude) from geo g inner join tweets t on g.geo_id = t.geoID inner join user u on t.userID = u.ID group by u.name')

#v.
#acording to the following code, it takes a little more than 10X as long to run the code 100 times as opposed to 10
def tentimes():
	st = time.time()
	for i in range(10):
		c.execute('select avg(g.longitude), avg(g.latitude) from geo g inner join tweets t on g.geo_id = t.geoID inner join user u on t.userID = u.ID group by u.name')
	print(str(time.time() - st))

def moretimes():
	st = time.time()
	for i in range(100):
		c.execute('select avg(g.longitude), avg(g.latitude) from geo g inner join tweets t on g.geo_id = t.geoID inner join user u on t.userID = u.ID group by u.name')
	print(str(time.time() - st))

#part b

tempfile.close()
tempfile= codecs.open('tweetsfile.txt', 'r', 'utf8')

id_list = []

for line in tempfile:
	json_object = json.loads(line)
	if '44' in json_object['id_str'] or '77' in jsonobject['id_str']:
		id_list.append(json_object['id_str'])

#the run time for this is over half a second whereas the runtime for the sql command is 6 1000ths of a second

reply_to_user_id_list = []
for line in tempfile:
	json_object = json.loads(line)
	if json_object['in_reply_to_user_id'] not in reply_to_user_id_list:
		reply_to_user_id_list.append(json_object['in_reply_to_user_id'])
	len(reply_to_user_id_list)

#this way takes 6/10ths of a second and with sql it takes 4/1000ths of a second

#problem 3

def generateInsert(table_name):

	fd = codecs.open('newfile.txt', 'w', 'utf8')

	for row in c.execute('select * from %s' %(table_name)):
		string_id = ''
		for i in str(row[0]):
			string_id = string_id + chr(ord('a') + int(i))

    	fd.write("INSERT INTO %s VALUES (%s, %s);" %(table_name, string_id, row[1:]))

    fd.close()

#my runtime for this code was 8/100ths of a second

tempfile.close()
tempfile = codecs.open('tweetsfile.txt', 'r', 'utf8')


#part b
def getFromFile():
	st = time.time()
	tempfile = codecs.open('tweetsfile.txt', 'r', 'utf8')
	for line in tempfile:
		try:
			json_object = json.loads(line)
			string_id = ''
			for i in str(json_object['user']['id_str']):
				string_id = string_id + chr(ord('a') + int(i))
			c.execute('insert into user values (%s,%s,%s,%,s%);' %(string_id, json_object['user']['name'], json_object['user']['screen_name'], json_object['user']['description'], json_object['user']['friends_count']))

		except(ValueError):
			bad_tweets = line
	print(time.time() - st)

#my run time for this was 6/10ths of a second, so this takes about 10x as long


def generatefile():

	st = time.time()

	fd = codecs.open('newfile.txt', 'w', 'utf8')

	#this is will be the value representing the unknown locations	
	fd.write('0, None, None, None |')


	for row in c.execute('select * from geo'):
		lat = round(float(row[2]), 4)
		lon = round(float(row[3]), 4)
		fd.write('%s, %s, %s, %s |' %(row[0], row[1], lat, lon))


	for row in c.execute('select * from tweets'):
		if row[0] == None:
			key = 0
		else:
			key = row[0]
		fd.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s |' %(key, row[1], row[2], row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]) )

	for row in c.execute('select * from user'):
		if row[2] != None and row[3] != None and row[1] != None and (row[1] in row[2] or row[1] in row[3]):
			name_in = True
		else:
			name_in = False
		fd.write('%s,%s,%s,%s,%s,%s |' %(row[0], row[1], row[2], row[3], row[4], name_in))
	print(time.time() - st)

		
    	
    fd.close()

#when I loaded 5000 tweets, 4895 had no location, which is almost exactly 98%


