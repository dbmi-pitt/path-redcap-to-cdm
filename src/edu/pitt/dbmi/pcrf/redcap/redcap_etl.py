'''
Created on Feb 7, 2018

Note: This uses the python documentation styleguide from Google: https://google.github.io/styleguide/pyguide.html?showone=Comments#Comments
example:

def fetch_bigtable_rows(big_table, keys, other_silly_variable=None):
    """Fetches rows from a Bigtable.

    Retrieves rows pertaining to the given keys from the Table instance
    represented by big_table.  Silly things may happen if
    other_silly_variable is not None.

    Args:
        big_table: An open Bigtable Table instance.
        keys: A sequence of strings representing the key of each table row
            to fetch.
        other_silly_variable: Another optional variable, that has a much
            longer name than the other args, and which does nothing.

    Returns:
        A dict mapping keys to the corresponding table row data
        fetched. Each row is represented as a tuple of strings. For
        example:

        {'Serak': ('Rigel VII', 'Preparer'),
         'Zim': ('Irk', 'Invader'),
         'Lrrr': ('Omicron Persei 8', 'Emperor')}

        If a key from the keys argument is missing from the dictionary,
        then that row was not found in the table.

    Raises:
        IOError: An error occurred accessing the bigtable.Table object.
    """
    pass


@author: chb69
'''

import cx_Oracle
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
    except:
        msg = "Unexpected error:", sys.exc_info()[0]
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)

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
    loadEventMapping(dbobj, project_info['path_project_id'])

def loadPatientMapping(dbobj, projectid):
    """Create an in memory mapping of patients in a project.

    Queries the database and builds a simple map from the REDCap record_id
    to the EMR patient_num.  The map is the dictPatientMap

    Args:
        dbobj: A database object allowing code to open and cursor and execute queries.
        project_info: An associative array containing REDCap project information
        
    Raises:
        IOError: An error occurred accessing the bigtable.Table object.
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

def loadEventMapping(dbobj, projectid):
    msg = "Loading event mapping for project_info {0}".format(projectid)
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
    except Exception as e:
        msg = "Error retrieving patient mapping for project_info {0}.  Error: {1}".format(projectid, e)
        logging.error(msg)
    

def loadCodesByForm(dbobj, projectid, formname):
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT DISTINCT field_name, concept_cd FROM {redcap_mapping_table} WHERE project_id = '{project_id}' AND form_name = '{form_name}' AND (concept_cd LIKE '%NORESPONSE' or concept_cd LIKE '%ANSWERED')".format(redcap_mapping_table=answer_mapping_table,project_id=projectid,form_name=formname))
        rows = meta_cursor.fetchall()
        for row in rows:
            # build a dictionary entry where the dictionary key is a field_name and the value is a concept_cd
            # replace 'ANSWERED' with 'NORESPONSE'
            concept_cd = str(row[1])
            concept_cd = concept_cd.replace('ANSWERED', 'NORESPONSE')
            form_field_dict[str(row[0])] = concept_cd
    except Exception as e:
        msg = "Error retrieving answer mapping form list for project_info {0} formname {1}.  Error: {2}".format(projectid, formname, e)
        logging.error(msg)

def truncatePROTable(dbobj):
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

def etlProject(dbobj, project_info):
    loadSupportingProjectData(dbobj, project_info)

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


