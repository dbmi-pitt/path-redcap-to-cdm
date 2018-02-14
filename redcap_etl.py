'''
Created on Feb 7, 2018



@author: chb69
'''

from Oracle import Oracle
from SqlServer import SqlServer
import datetime
import time
import sys
import traceback
import copy
import requests.packages.urllib3
import ConfigParser
import os
import logging
import ast 
from redcapProject import RedcapProject
from distutils.version import StrictVersion
from PyCap.redcap import Project, RedcapError


#initialize a dictionary with all the variables relating to the DATABASE section
#This allows the code to loop through the section while loading the variables into the dictionary
dictDatabaseSettings = {'dbms' : None, 'host' : None, 'port' : None, 'sid': None, 'dbname' : None, 'dbuser' : None, 'dbpassword' : None, 
                        'event_mapping_table' : None, 'answer_mapping_table' : None, 'patient_mapping_table' : None, 
                        'pro_cm_table' : None}

#initialize a dictionary with all the variables relating to the REDCAP section
#This allows the code to loop through the section while loading the variables into the dictionary
dictRedcapSettings = {'api_url' : None, 'redcap_project_info' : None, 'verify_ssl' : True, 'lazy' : False, 
                      'redcap_version' : None, 'ignore_fields' : None}

# The object representing a database connection
dbconn = None

dictPatientMap = {}
dictEventMap = {}

# IMPORTANT!! This variable represents the "cutoff" between the NewRedcapXMLParser and the OldRedcapXMLParser
# classes.  If the code encounters a version of REDCap less than the redcap_new_api_version, it will use the 
# OldRedcapXMLParser to extract the data.  Otherwise, it will use the NewRedcapXMLParser.  Also note,
# the version (6.11.2) is an approximate estimate.  We don't know exactly when REDCap changed their API, 
# but this document may have relevant information on page 5: https://www.bu.edu/ctsi/files/2016/03/Version-6.11.2.docx 
# The document includes the following item:
#     "Change: The API method 'Export Instrument-Event Mappings' now returns a different structure if exporting as JSON or XML" 
redcap_new_api_version = "6.11.2"

def loadConfigFile():
    """Open the configuration file and load the contents
    
    Opens the configuration file (the code assumed the file to be called 'config.ini' and 
    exist in the top level directory with the python code).  This method loads the configuration data into
    two dictionaries: a database dictionary and a REDCap dictionary.
    
    Returns:
        Creates two dictionaries loaded with database settings and REDCap settings
        
    Any errors stop the code's execution  
    """
    config = ConfigParser.ConfigParser()
    try:
        logging.info("Starting loading config.ini")
        config.read('config.ini')
        for k in dictDatabaseSettings.keys():
            dictDatabaseSettings[k] = config.get('DATABASE', k)
        for k in dictRedcapSettings.keys():
            dictRedcapSettings[k] = config.get('REDCAP', k)
        #parse some of the complicated variables from strings to valid python types (ex lists, dict, etc.)
        dictRedcapSettings['redcap_project_info'] = ast.literal_eval(dictRedcapSettings['redcap_project_info'])
        dictRedcapSettings['ignore_fields'] = ast.literal_eval(dictRedcapSettings['ignore_fields'])
        
        checkConfigSettings(dictDatabaseSettings, dictRedcapSettings)
        
        #explicitly set some of the values to their correct datatypes
        dictRedcapSettings['verify_ssl'] = bool(dictRedcapSettings['verify_ssl'])
        dictRedcapSettings['lazy'] = bool(dictRedcapSettings['lazy'])
        logging.info("Finished loading config.ini")
    except OSError as err:
        msg = "OS error.  Check config.ini file to make sure it exists and is readable: {0}".format(err)
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)
    except ConfigParser.NoSectionError as noSectError:
        msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(noSectError)
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)
    except ConfigParser.NoOptionError as noOptError:
        msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(noOptError)
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)
    except SyntaxError as syntaxError:
        msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(syntaxError)
        msg = msg + "  Cannot read line: {0}".format(syntaxError.text)
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)        
    except AttributeError as attrError:
        msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(attrError)
        msg = msg + "  Cannot read line: {0}".format(attrError.text)
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)        
    except:
        msg = "Unexpected error:", sys.exc_info()[0]
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)

def checkConfigSettings(dictDatabaseSettings, dictRedcapSettings):
    #first check database settings
    generalSettingsList = ['dbms', 'host', 'dbuser', 'dbpassword', 'event_mapping_table', 'answer_mapping_table', 'patient_mapping_table', 'pro_cm_table', 'port']
    for gs in generalSettingsList:
        if dictDatabaseSettings[gs] == None or len(dictDatabaseSettings[gs]) == 0:
            raise Exception('Missing {0} entry in config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format(gs,gs))

    #check Oracle specific settings
    if dictDatabaseSettings['dbms'] == 'Oracle':
        if dictDatabaseSettings['sid'] == None or len(dictDatabaseSettings['sid']) == 0:
            raise Exception('Missing {0} entry in config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format('sid','sid'))
    else:
        if dictDatabaseSettings['dbname'] == None or len(dictDatabaseSettings['dbname']) == 0:
            raise Exception('Missing {0} entry in config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format('dbname','dbname'))
            
    generalRedcapList = ['api_url', 'redcap_version', 'verify_ssl', 'lazy', 'ignore_fields', 'redcap_project_info' ]
    for gs in generalRedcapList:
        if dictRedcapSettings[gs] == None or len(dictRedcapSettings[gs]) == 0:
            raise Exception('Missing {0} entry in config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format(gs,gs))
        
    for info in dictRedcapSettings['redcap_project_info']:
        for k in info.keys():
            if info[k] == None or len(info[k]) == 0:
                raise Exception('Found blank entry for the {0} variable in the REDCap project definition of the config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format(k,k))

    for info in dictRedcapSettings['ignore_fields']:
        for k in info:
            if k == None or len(k) == 0:
                raise Exception('Found blank entry for the {0} variable in the REDCap project ignore_fields entry of the config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format(k,k))
                
        

def loadSupportingProjectData(dbobj, project_info):
    """Load mapping tables from database.

    Retrieves database records allowing the code to
    map from REDCap identifiers like patients ids and event ids
    to CDM concepts like patid and event id

    Args:
        dbobj: A database object allowing code to open and cursor and execute queries.
        project_info: An associative array containing REDCap project information
    """    
    loadPatientMapping(dbobj, project_info['site_project_id'])
    loadEventMappingDatabase(dbobj, project_info['path_project_id'])

def loadPatientMapping(dbobj, projectid):
    """Create an in memory mapping of patients in a project.

    Queries the database and builds a simple map from the REDCap record_id
    to the EMR patient_num.  The map is the dictPatientMap

    Args:
        dbobj: A database object allowing code to open and cursor and execute queries.
        project_info: An associative array containing REDCap project information
        
    Raises:
        Exception: An error occurred accessing database table.
    """    
    msg = "Loading patient list for project_info {0}".format(projectid)
    logging.info(msg)
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT record_id, patient_num FROM {patient_mapping_table} WHERE project_id = '{project_id}'".format(patient_mapping_table=dictDatabaseSettings['patient_mapping_table'],project_id=projectid))
        rows = db_cursor.fetchall()
        #TODO: check for duplicate record_ids or patient_nums
        for row in rows:
            record_id = str(row[0])
            patient_num = str(row[1])
            dictPatientMap[record_id] = patient_num
        msg = "Finished loading patient list for project_info {0}".format(projectid)
        logging.info(msg)
    except Exception as e:
        msg = "Error retrieving patient mapping for project_info {0}.  Error: {1}".format(projectid, e)
        logging.error(msg)

def loadEventMappingDatabase(dbobj, projectid):
    """Create an in memory mapping of events in a project.

    Queries the database and builds a simple map from the REDCap unique_event_name
    an associative array containing the arm_num and modifier_cd.  The map is the dictEventMap

    Args:
        dbobj: A database object allowing code to open and cursor and execute queries.
        project_info: An associative array containing REDCap project information
        
    Raises:
        Exception: An error occurred accessing database table.
    """    
    msg = "Loading event mapping for projectid {0}".format(projectid)
    logging.info(msg)
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT unique_event_name, arm_num, modifier_cd FROM {event_mapping_table} WHERE project_id = '{project_id}'".format(event_mapping_table=dictDatabaseSettings['event_mapping_table'],project_id=projectid))
        rows = db_cursor.fetchall()
        #TODO: check for duplicates
        for row in rows:
            unique_event_name = str(row[0])
            arm_num = str(row[1])
            modifier_cd = str(row[2])
            dictEventMap[unique_event_name] = {'arm_num' : arm_num, 'modifier_cd' : modifier_cd}
        msg = "Finished loading event for project_info {0}".format(projectid)
        logging.info(msg)
        return dictEventMap
    except Exception as e:
        msg = "Error retrieving patient mapping for projectid {0}.  Error: {1}".format(projectid, e)
        logging.error(msg)
    

def loadCodesByFormDatabase(dbobj, projectid, formname):
    """Create an in memory mapping of events in a project.

    Queries the database and builds a simple map from the REDCap unique_event_name
    an associative array containing the arm_num and modifier_cd.  The map is the dictEventMap

    Args:
        dbobj: A database object allowing code to open and cursor and execute queries.
        project_info: An associative array containing REDCap project information
        
    Raises:
        Exception: An error occurred accessing database table.
    """ 
    dictFieldData = {}   
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT DISTINCT field_name, concept_cd, answer_text FROM {redcap_mapping_table} WHERE project_id = '{project_id}' AND form_name = '{form_name}' AND (concept_cd LIKE '%NORESPONSE' or concept_cd LIKE '%ANSWERED')".format(redcap_mapping_table=dictDatabaseSettings['answer_mapping_table'],project_id=projectid,form_name=formname))
        rows = db_cursor.fetchall()
        for row in rows:
            # build a dictionary entry where the dictionary key is a field_name and the value is a concept_cd
            # replace 'ANSWERED' with 'NORESPONSE'
            concept_cd = str(row[1])
            concept_cd = concept_cd.replace('ANSWERED', 'NORESPONSE')
            field_name = str(row[0])
            answer_text = str(row[2])
            # the key needs to be a combination of field_name:number from answer_text
            dictKey = field_name + ':' + answer_text.split(',')[0]
            dictFieldData[dictKey] = concept_cd
        return dictFieldData
    except Exception as e:
        msg = "Error retrieving answer mapping form list for project_info {0} formname {1}.  Error: {2}".format(projectid, formname, e)
        logging.error(msg)

# extract a list of all the forms for a given project_info using the REDCAP_ANSWER_MAPPING table
def loadFormListDatabase(dbobj, project_id):
    listProjectForms = []
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT DISTINCT form_name FROM {redcap_mapping_table} WHERE project_id = '{project_id}'".format(redcap_mapping_table=dictDatabaseSettings['answer_mapping_table'],project_id=project_id))
        rows = db_cursor.fetchall()
        for row in rows:
            listProjectForms.append(str(row[0])) 
    except Exception as e:
        print "Error retrieving form list for project_id {0}.  Error {1}".format(project_id, e)
    return listProjectForms

def truncatePROTable(dbobj):
    """Truncate the PRO_CM table before loading data.

    Truncates the PRO_CM table before running the loading process.

    Args:
        dbobj: A database object allowing code to open and cursor and execute queries.
        
    Raises:
        Exception: An error occurred accessing database table.  Any exception halts the 
        program's execution.
    """    

    try:
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        msg = "Truncating the {0} table".format(dictDatabaseSettings['pro_cm_table'])
        logging.info(msg)
        db_cursor.execute("TRUNCATE TABLE {0}".format(dictDatabaseSettings['pro_cm_table']))
        logging.info("Finished truncating the {0} table".format(dictDatabaseSettings['pro_cm_table']))
    except:
        msg = "Unexpected error:", sys.exc_info()[0]
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)

    

def etlTest(project_info):
    # get metadata connection
    #demo_con = demo_db_obj.getConnection()
    #meta_con = meta_db_obj.getConnection()
    #demo_cursor = demo_con.cursor()
    #meta_cursor = meta_con.cursor()
    
    #print "Deleting old REDCap data from the {0} table".format(visit_dimension_table)
    #demo_cursor.execute("DELETE {0} WHERE SOURCESYSTEM_CD = '{1}'".format(visit_dimension_table, source_system_cd))
    #print "Deleting old REDCap data from the {0} table".format(observation_fact_table)
    #demo_cursor.execute("DELETE {0} WHERE SOURCESYSTEM_CD = '{1}'".format(observation_fact_table, source_system_cd))
    #print "Truncating the {0} table".format(encounter_mapping_table)
    #demo_cursor.execute("TRUNCATE TABLE {0}".format(encounter_mapping_table))
    from PyCap.redcap import Project, RedcapError
    import xml.etree.ElementTree as ET

    #for project_info in redcap_project_info:
    missing_patient_count = 0 
    api_url = 'https://www.ctsiredcap.pitt.edu/redcap/api/'
    api_key = 'F2DD0FEF0C86AFF9DEE0C293B3BF9D45'           
    #project = Project(api_url, api_key, True, False)
    project = Project(dictRedcapSettings['api_url'], project_info['api_key'], verify_ssl= dictRedcapSettings['verify_ssl'], lazy= dictRedcapSettings['lazy'])

    # get form_event_mapping from REDCAP API
    fem = project.export_fem(format="xml")
    items = ET.fromstring(fem.encode('utf-8'))
    print "===>Starting ETL for {0}".format(project_info['project_name'])
    #etl_project(project, items, project_info['path_project_id'], project_info['site_project_id'], demo_cursor, meta_cursor)
    print "===>Finished ETL for {0}".format(project_info['project_name'])
    print "Found {missing_patients} REDCap patients missing from the REDCAP_PATIENT_MAPPING table".format(missing_patients=missing_patient_count)

    try:
        #print "Writing {0} results.".format(project_info['project_name'])
        #demo_con.commit()
        pass
    except Exception as e:
        print "Encountered an error writing results."
        print e
        tb = traceback.format_exc()
        print tb


    #demo_con.close()
    #meta_con.close()

    # text_file = open("data/ipf_records.csv", "w")
    # text_file.write(data.encode('UTF-8'))
    # text_file.close()
    print "ETL Completed."

def etlProject(dbobj, project_info):
    loadSupportingProjectData(dbobj, project_info)
    rcProject = RedcapProject(logging, dictRedcapSettings['api_url'], project_info['api_key'],
                                  project_info['project_name'], verify_ssl= dictRedcapSettings['verify_ssl'], lazy= dictRedcapSettings['lazy'])

    msg = "Querying REDCap found these events {0} for project {1}".format(rcProject.getEvents(), project_info['project_name'])
    logging.debug(msg)
    
    dictEvents = loadEventMappingDatabase(dbobj, project_info['path_project_id'])
    msg = "Querying database found these events {0} for project {1}".format(dictEvents.keys(), project_info['project_name'])
    logging.debug(msg)
    
    msg = "Querying REDCap found these forms {0} for project {1}".format(sorted(rcProject.getForms()), project_info['project_name'])
    logging.debug(msg)
    
    listForms = loadFormListDatabase(dbobj, project_info['path_project_id'])
    msg = "Querying database found these forms {0} for project {1}".format(sorted(listForms), project_info['project_name'])
    logging.debug(msg)



if __name__ == "__main__":
    # use this command to disable the InsecurePlatformWarning (see http://stackoverflow.com/questions/29099404/ssl-insecureplatform-error-when-using-requests-package)
    requests.packages.urllib3.disable_warnings()
    logging.basicConfig(filename='redcap.log.' + str(os.getpid()),level=logging.DEBUG)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)

    today = datetime.date.today()
    logging.info('Starting REDCap ETL to CDM Process: ' + today.strftime('%m/%d/%Y %I:%M'))

    loadConfigFile()
    if dictDatabaseSettings['dbms'] == "Oracle":
        dbobj = Oracle(dictDatabaseSettings['host'], dictDatabaseSettings['port'], dictDatabaseSettings['sid'], dictDatabaseSettings['dbuser'], dictDatabaseSettings['dbpassword'])
    else:
        dbobj = SqlServer(dictDatabaseSettings['host'], dictDatabaseSettings['port'], dictDatabaseSettings['dbname'], dictDatabaseSettings['dbuser'], dictDatabaseSettings['dbpassword'])
    truncatePROTable(dbobj)
    redcap_project_info = dictRedcapSettings['redcap_project_info']
    
    
    for project_info in redcap_project_info:
        logging.info('Starting ETL for Project: {0}'.format(project_info['project_name']))
        etlProject(dbobj, project_info)
    

