import sys, os
import commands
#import cx_Oracle
from xml.dom import minidom

class pyELSSI :

	def eventLookup(self, runnr, eventnr, list_of_attributes, stream='null', verbose=False):
		try:
			cert = os.environ['X509_USER_PROXY']
		except KeyError:
			print 'You have to provide a valid proxy! (voms-proxy-init)'
		if type(list_of_attributes).__name__!='list':
			raise TypeError("Please provide a list of attributes as input. Example: ['streamAOD_ref', 'streamESD_ref', 'streamRAW_ref']")
		runnr_s = str(runnr)
		eventnr_s = str(eventnr)
		guidList = []
		connList = []
		string_of_attributes = ''
		num = len(list_of_attributes)
		for x in range (0,num-1):
			string_of_attributes = string_of_attributes + list_of_attributes[x] + ','
		string_of_attributes = string_of_attributes + list_of_attributes[num-1]
		collList = []
		cert = os.environ['X509_USER_PROXY']
		my_curl_command = "curl --silent --key " + cert + " --sslv3 --cert " + cert + " -k --url \"https://lxvm0341.cern.ch/tagservices/dev/evinek/event_lookup/catalog_lookup_bulk.php?runnr=" + runnr_s + "&stream=" + stream + "\""
		#print my_curl_command
		s,o = commands.getstatusoutput(my_curl_command)
		html_result2 = o
		if (html_result2 != ''):
				try:
					xmldoc2 = minidom.parseString(html_result2)
					trlist2 = xmldoc2.getElementsByTagName('tr')
					nr_tr2 = trlist2.length
					table2 = xmldoc2.childNodes[0]
					for x in range (0, nr_tr2): # start with 0 here because there is no header!
						row2 = table2.childNodes[x]
						myList2 = []
						tdlist2 = row2.getElementsByTagName('td')
						nr_td2 = tdlist2.length
						for y in range (0, nr_td2):
							myList2.append(row2.childNodes[y].firstChild.data.encode())
						myTuple2 = tuple(myList2)
						connList.append(myTuple2)
				except:
					print 'The result of the TAG catalog query could not be parsed.'
		else:
			print 'The curl request did not get any result back (the returned html table is empty).'

		for item in connList:
			collection_name = item[1]
			connection = item[0]
			#print collection_name
			#cert = os.environ['X509_USER_PROXY']
			#curl_command = "curl -i --key " + cert + " --sslv3 --cert " + cert + " -k --url \'https://lxvm0341.cern.ch/tagservices/dev/fjriegas/eventselect/eventselect.php?query=select%20" + string_of_attributes + "%20from%20" + collection_name + "%20%20where%20RunNumber=" + runnr_s + "%20and%20EventNumber=" + eventnr_s + "&collection=" + collection_name + "&connect=" + connection + "'"""
			curl_command = "curl --silent --key " + cert + " --sslv3 --cert " + cert + " -k --url \"https://lxvm0341.cern.ch/tagservices/dev/fjriegas/eventselect/eventselect_bulk.php?select=" + string_of_attributes + "&%20where=RunNumber=" + runnr_s + "and%20EventNumber=" + eventnr_s + "&collection_list=" + collection_name + "&connect=" + connection + "\""

			if verbose:
				print curl_command
			s,o = commands.getstatusoutput(curl_command)
			html_result = o
			if verbose:
				print html_result
			if (html_result != ''):
				try:
					xmldoc = minidom.parseString(html_result)
					trlist = xmldoc.getElementsByTagName('tr')
					nr_tr = trlist.length
					table = xmldoc.childNodes[0]
					for x in range (1, nr_tr): # start with 1 here because of the header!!
						row = table.childNodes[x]
						myList = []
						tdlist = row.getElementsByTagName('td')
						nr_td = tdlist.length
						for y in range (0, nr_td):
							myList.append(row.childNodes[y].firstChild.data.encode())
						myTuple = tuple(myList)
						#myTuple2 = runnr, eventnr, connection,  myTuple
						#guidList.append(myTuple2)
						guidList.append(myTuple)
						
				except:
					print 'The result of the call to eventselect.php could not be parsed.'
			else:
				print 'The curl request did not get any result back (the returned html table is empty).'
		if len(guidList) == 0:
			print "No results found for runnumber=" + runnr_s + " and eventnumber=" + eventnr_s + " and stream=" + stream
		else:
			guidList.sort()
			last = guidList[-1]
			for i in range(len(guidList)-2, -1, -1):
				if last == guidList[i]:
					del guidList[i]
				else:
					last = guidList[i]

			#print "Results for runnumber=" + runnr_s + " and eventnumber=" + eventnr_s + ": "
		return guidList
