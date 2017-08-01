## This program reads, audit, clean and save cleaned data as .csv files.
## Some relevent codes were adopded from Data Wrangaling case studies.
## This program is written in Python3

## Importing required modules

import xml.etree.cElementTree as ET
import csv
from collections import defaultdict
import re

###############################################################################
## Essential data for future use

## List of expected street types
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
			"Trail", "Parkway", "Commons", "Highway", "Circle", "Way"]

## Street type discreepancies. This dictionary needs to update with the 
## discrepacies found.
mapping = { "St": "Street",
			"St." : "Street",
			"Ave" : "Avenue",
			"Rd." : "Road",
			"Rd" : "Road",
			"NE" : "North East",
			"SE" : "South East",
			"NW" : "North West",
			"SW" : "South West",
			"E" : "East",
			"S" : "South",
			"W" : "West",
			"N" : "North",
			"Cir" : "Circle",
			"Dr" : "Drive",
			"Blvd": "Boulevard"
			}

street_types = defaultdict(set)
zipcodes = set()
street_types2 = defaultdict(set)

## The downloaded osm file
#in_osm_file = 'Milledgeville_GA_sample_k10.osm'
in_osm_file = 'Milledgeville_GA.osm'

###############################################################################
## Get nodes data
def get_nodes(elem):
	nodes = {}
	node_tags = {}
	node_tag_lst = []
	
	nod_id = elem.attrib["id"]	
	nodes['nod_id'] = nod_id
	nodes["lon"] = elem.attrib["lon"]
	nodes["lat"] = elem.attrib["lat"]
	nodes["changeset"] = elem.attrib["changeset"]
	nodes["uid"] = elem.attrib["uid"]
	nodes["user"] = elem.attrib["user"]	
			
	## Get node tag infromation
	for tag in elem.iter("tag"):
		node_tags = {}
		node_tags["nod_id"] = nod_id
		tag_val = tag.attrib['v']
		node_tags["k"] = tag.attrib['k']
		
		## Auditing and Cleaning the street name
		if is_street_name(tag):
			audit_street_type2(street_types2, tag_val)
			tag_val = update_street_name(tag_val, mapping)
		if is_zipcode(tag):
			audit_zipcodes(zipcodes, tag_val)
			tag_val = update_zip(tag_val)
		
		node_tags["v"] = tag_val
		node_tag_lst.append(node_tags)

	return nodes, node_tag_lst
	
###############################################################################
## Get ways data
def get_ways(elem):
	ways = {}
	way_tag_lst = []
	way_nd_lst = []
	
	way_id = elem.attrib["id"]	
	ways['way_id'] = way_id
	ways["changeset"] = elem.attrib["changeset"]
	ways["uid"] = elem.attrib["uid"]
	ways["user"] = elem.attrib["user"]			
			
	## Get node tag infromation
	for tag in elem.iter("tag"):
		way_tags = {}
		way_tags["way_id"] = way_id
		tag_val = tag.attrib['v']
		
		## Auditing and Cleaning the street name
		if is_street_name(tag):
			audit_street_type2(street_types2, tag_val)
			tag_val = update_street_name(tag_val, mapping)
		if is_zipcode(tag):
			audit_zipcodes(zipcodes, tag_val)
			tag_val = update_zip(tag_val)
			
		way_tags["v"] = tag_val
		way_tags["k"] = tag.attrib['k']
		way_tag_lst.append(way_tags)		
		
	for tag in elem.iter("nd"):
		way_nds = {}
		way_nds["way_id"] = way_id
		way_nds["ref"] = tag.attrib['ref']
		way_nd_lst.append(way_nds)

	return ways, way_tag_lst, way_nd_lst

###############################################################################
## This function reads the osm data iteratively
def iter_read_osm(in_osm_file):
	osm_file = open(in_osm_file, "r", encoding="utf8")
	
	## create a handle that can itterate over xml file
	xml_dat = ET.iterparse(osm_file, events=("start","end"))
	
	is_new_node = True
	is_new_nod_tag = True
	is_new_way = True
	is_new_way_tag = True
	is_new_way_nod =True	
	
	nodes_lst = []
	node_tag_main_lst = []
	ways_lst = []
	way_tag_main_lst = []
	way_nod_lst = []
	
	line_count = 0
	can_write = False
	## Iterate ove elements elemnts	
	for event, elem in xml_dat:
		try:
			line_count += 1			
			if line_count == 5000 or (event == "end" and elem.tag == "osm"):
				#print(line_count, event, elem.tag)
				can_write = True
				line_count = 0
					
			if event == "start":
				## Seperate elements in to their own set
				## Get node information
				if elem.tag == "node":
					nodes, node_tag_lst = get_nodes(elem)
					nodes_lst.append(nodes)
					
					#for node_tags in node_tag_lst:
					node_tag_main_lst = node_tag_main_lst + node_tag_lst
				
		
				## Get node information
				if elem.tag == "way":
					ways, way_tag_lst, way_nd_lst = get_ways(elem)
					ways_lst.append(ways)
					#for way_tags in way_tag_lst:
					way_tag_main_lst = way_tag_main_lst + way_tag_lst
					way_nod_lst = way_nod_lst + way_nd_lst
						
					#for way_nds in way_nd_lst:
		
						
				if can_write:
					#for nod in nodes_lst:
					if len(nodes_lst) > 0:
						write_csv('nodes.csv', is_new_node, nodes_lst)				
						is_new_node = False
						nodes_lst = []
					
					if len(node_tag_main_lst) > 0:
						write_csv('node_tags.csv', is_new_nod_tag, node_tag_main_lst)				
						is_new_nod_tag = False
						node_tag_main_lst = []
					
					if len(ways_lst) > 0:
						write_csv('ways.csv', is_new_way, ways_lst)
						is_new_way = False
						ways_lst = []
					
					if len(way_tag_main_lst) > 0:
						write_csv('way_tags.csv', is_new_way_tag, way_tag_main_lst)
						is_new_way_tag = False
						way_tag_main_lst = []
					
					if len(way_nod_lst) > 0:
						write_csv('way_nodes.csv', is_new_way_nod, way_nod_lst)
						is_new_way_nod = False
						way_nod_lst = []
					
					can_write = False
		except:
			print(elem.tag)
			continue
	osm_file.close()

###############################################################################
## Write the dictionary values to csv
def write_csv(file_nm, is_new, dic):
	
	if len(dic) > 0:
	
		if is_new:
			#print (is_new)
			writer = csv.DictWriter(open(file_nm, 'w', encoding="utf8"), fieldnames = dic[0].keys(), lineterminator='\n')
			writer.writeheader()
			for lin in dic:
				try:
					writer.writerow(lin)
				except:
					print(lin)
					continue
		elif is_new == False:
			writer = csv.DictWriter(open(file_nm, 'a', encoding="utf8"), fieldnames = dic[0].keys(), lineterminator='\n')
			for lin in dic:
				try:
					writer.writerow(lin)
				except:
					print(lin)
					continue
###############################################################################
## Check whether the tag value is street name or not
def is_street_name(elem):
	return (elem.attrib['k'] == "addr:street")
	
###############################################################################
## Check whether the tag value is zipcode or not
def is_zipcode(elem):
	return (elem.attrib['k'] == "addr:postcode")
	
###############################################################################
## This function update the street_types dictionary if any discrepancy is there
## in street names.
def audit_street_type2(street_types2, street_name):
	## Split the street names
	street_splts = street_name.split()
	
	## Get the last name of the streets. Generally it is the street type
	str_type = street_splts[len(street_splts) -1]
	if str_type not in expected and str_type not in mapping.keys():
		## Get unique data only
		street_types2[str_type] = street_name


###############################################################################
## This function update the zipcode dictionary if any discrepancy is there
## in street names.
def audit_zipcodes(zipcodes, zipcode):
	## Get unique data only
	zipcode = update_zip(zipcode)
	zipcodes.add(zipcode)
	
###############################################################################
## Clean the erooneous zip codes
def update_zip(zipc):
	if len(zipc) == 5 and int(zipc):
		retu = zipc
	else:
		zips = zipc.split(' ')
		retu = zips[2]
		print(retu)
	return retu		
	
###############################################################################
## updating the street names to standard format
def update_street_name(name, mapping):
	nme = str()
	splitted_name = name.split()
	#print(splitted_name)
	for i, nam in enumerate(splitted_name):
		if nam in mapping.keys():
			splitted_name[i] = mapping[nam]
	for nm in splitted_name:
		nme += nm+' '
	
	name = nme.rstrip()
	return name

###############################################################################
## The main function
def main():
	## Cleaning and editing the data
	iter_read_osm(in_osm_file)
	
	## If still some errors exist, following message will guide where to address
	print("Please modify 'mapping' dictionary to include these non standart street types as needed: ", street_types2)
	print("Please check errors of this zipcodes: ", zipcodes)

if __name__ == '__main__':
	main()