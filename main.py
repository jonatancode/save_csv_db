#!/usr/python3

import csv
import pymongo
import sys
from optparse import OptionParser


def is_int(string):
	try:
		number_int =int(string)
		return True
	except:
		return False

def is_float(string):
	try:
		number_float = float(string)
		return True
	except:
		return False

def is_list(string):
	return "|" in string

def string_to_array(string, delimiter="|"):
	if delimiter == "|":
		return string.split(delimiter)

def type_value(string):
	if is_int(string):
		return "int"

	elif is_float(string):
		return "float"

	elif is_list(string):
		return "list"
	else:
		return "str"

def convert_value(type_value):
	def to_str(value):
		return str(value)

	def to_int(value):
		return int(value)

	def to_float(value):
		return float(value)

	def to_list(value, delimiter="|"):
		return value.split(delimiter)

	dic = {
		"str": to_str,
		"int": to_int,
		"float" : to_float,
		"list" : string_to_array
	}
	return dic[type_value]

def type_value_mongo(value):
	if value == "int":
		return "int"
	elif value == "list":
		return "array"
	elif value == "float":
		return 
	elif value == "str":
		return "string"
	else:
		return False

def connect(host="localhost", port=27017, name_db="test_csv_to_mongo"):
	try:
		connect = pymongo.MongoClient( host, port)
		db = connect[name_db]
		return db
	except:
		return False
	
def extract_info(file,delimiter):
	data_list = []
	object_csv = csv.reader(file, delimiter=delimiter)
	
	for x in object_csv:
		data_list.append(x)

	file.close()
	return data_list


def show_header(list_csv):
	header = data_list[0]
	firt_item = data_list[1]

	model = {}
	for x in range(len(header)):
		model.setdefault(header[x], convert_value(type_value(firts_item[x]))(firts_item[x]) )

	return model

def get_header(list_csv):
	return list_csv[0]

def insert_mongo(db, registro, header):
	try:
		data = {}
		for x in range(len(header)):
			data.setdefault(header[x], convert_value(type_value(registro[x]))(registro[x]) )
		#print(data)
		db.my_collection.insert_one(data)
		return True

	except :
		print(sys.exc_info()[0])
		print("[ERROR] ERROR INSERT MONGO")
	


# FILE_NAME = "csv/movie_metadata.csv"
# NAME_DB ="probandoprobado"
# DELIMITER = ","
# HOST= "localhost"
# PORT = 27017

def main():
	parser = OptionParser()
	parser.add_option("-l", "--host",help="host database", default="localhost")
	parser.add_option("-p", "--port",help="Port database", default=27017, type="int")
	parser.add_option("-n", "--namedb", help="Name db", default="ejemplo")
	parser.add_option("-f", "--filename", dest="filename", help="Path file csv", metavar="FILE")
	parser.add_option("-d", "--delimiter", dest="delimiter", help="delimiter file csv", default=",")
	(opts, args) = parser.parse_args()
	
	if opts.filename:
		try: 
			file = open(opts.filename, "rb")
		except:
			print("FICHERO no existe")
			exit()

	if opts.delimiter:
		print(opts.delimiter)
		DELIMITER = opts.delimiter

	if opts.host:
		print(opts.host)
		HOST = opts.host

	if opts.port:
		print(opts.port)
		PORT = opts.port

	if opts.namedb:
		print(opts.namedb)
		NAME_DB =opts.namedb

	# open file
	data_list = extract_info(file, DELIMITER)

	# #connect
	db = connect(HOST, PORT, NAME_DB)

	header = get_header(data_list)

	for item in data_list:
		insert_mongo(db, item, header)
		
	print("Fin del programa")

	

if __name__ == "__main__":
	main()