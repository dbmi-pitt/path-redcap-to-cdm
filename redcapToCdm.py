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
from dateutil.parser import *
from dateutil.tz import *


#initialize a dictionary with all the variables relating to the DATABASE section
#This allows the code to loop through the section while loading the variables into the dictionary
dictDatabaseSettings = {'dbms' : None, 'host' : None, 'port' : None, 'sid': None, 'dbname' : None, 'dbuser' : None, 'dbpassword' : None, 
                        'event_mapping_table' : None, 'answer_mapping_table' : None, 'patient_mapping_table' : None, 
                        'pro_cm_table' : None}

#initialize a dictionary with all the variables relating to the REDCAP section
#This allows the code to loop through the section while loading the variables into the dictionary
dictRedcapSettings = {'api_url' : None, 'redcap_project_info' : None, 'verify_ssl' : True, 'lazy' : False, 
                      'redcap_version' : None, 'load_text_fields' : False, 'logging_level' : 'INFO'}

# a dictionary to manage the data types found in the pro_cm table
dictPROCMTableDataTypes = {'pro_date' : 'date', 'pro_response_num' : 'number', 'pro_measure_score' : 'number', 'pro_measure_theta' : 'number',
                           'pro_measure_scaled_tscore' : 'number', 'pro_measure_standard_error' : 'number', 'pro_measure_count_scored' : 'number', 
                           'pro_response_date': 'date'}

# The object representing a database connection
dbconn = None

dictPatientMap = {}
dictEventMap = {}

# these are globals used when writing data to the CDM
pro_cm_id = 0
pro_measure_seq = 0

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
        
        checkConfigSettings(dictDatabaseSettings, dictRedcapSettings)
        
        #explicitly set some of the values to their correct datatypes
        dictRedcapSettings['verify_ssl'] = bool(dictRedcapSettings['verify_ssl'])
        dictRedcapSettings['lazy'] = bool(dictRedcapSettings['lazy'])
        dictRedcapSettings['load_text_fields'] = bool(dictRedcapSettings['load_text_fields'])
        level = logging.getLevelName(str(dictRedcapSettings['logging_level']))
        if level == None:
            level = logging.INFO
        logging.info("Setting logging level to {0}".format(logging.getLevelName(level)))
        logging.getLogger().setLevel(level)
                     
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
            
    generalRedcapList = ['api_url', 'redcap_version', 'verify_ssl', 'lazy', 'load_text_fields', 'redcap_project_info', 'logging_level' ]
    for gs in generalRedcapList:
        if dictRedcapSettings[gs] == None or len(dictRedcapSettings[gs]) == 0:
            raise Exception('Missing {0} entry in config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format(gs,gs))
        
    for info in dictRedcapSettings['redcap_project_info']:
        for k in info.keys():
            if info[k] == None or len(info[k]) == 0:
                raise Exception('Found blank entry for the {0} variable in the REDCap project definition of the config.ini file.  Please reference the config.ini.example file and set the {1} entry in your config.ini file'.format(k,k))

                
        

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
    

def loadCodesByFormDatabase(dbobj, projectid, formname, redcapProject):
    """Create an in memory mapping of events in a project.

    Queries the database and builds a simple map from the REDCap unique_event_name
    an associative array containing the arm_num and modifier_cd.  The map is the dictEventMap
    NOTE: To distinguish between different answer options, the KEY in this dictionary is the
    fieldname a colon plus the answer option (ex:id_06:2)

    Args:
        dbobj: A database object allowing code to open and cursor and execute queries.
        project_info: An associative array containing REDCap project information
        
    Raises:
        Exception: An error occurred accessing database table.
    """ 
    
    #TODO: extract the question text from REDCap
    dictFieldData = {}   
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT field_name, concept_cd, answer_text, pro_item_loinc, pro_measure_loinc, is_date_field FROM {redcap_mapping_table} WHERE project_id = '{project_id}' AND form_name = '{form_name}'".format(redcap_mapping_table=dictDatabaseSettings['answer_mapping_table'],project_id=projectid,form_name=formname))
        rows = db_cursor.fetchall()
        redcapFields = redcapProject.getFormFields(formname)
        for row in rows:
            # build a dictionary entry where the dictionary key is a field_name and the value is a concept_cd
            # replace 'ANSWERED' with 'NORESPONSE'            
            field_name = str(row[0])
            if redcapFields.has_key(field_name) == False:
                msg = "REDCAP_ANSWER_MAPPING error.  Unable to find a field named {0} in REDCap formname {1} for project {2}.".format(field_name, formname, projectid)
                logging.info(msg)
                continue
            redcapFieldInfo = redcapFields[field_name]
            field_type = redcapFieldInfo['field_type']
            field_label = redcapFieldInfo['field_label']
            concept_cd = str(row[1])
            concept_cd = concept_cd.replace('ANSWERED', 'NORESPONSE')
            answer_text = str(row[2])
            if row[3] != None:
                loinc_item_code = str(row[3])
            else:
                loinc_item_code = None
            if row[4] != None:
                loinc_measure_code = str(row[4])
            else:
                loinc_measure_code = None
            if row[5] != None:
                is_date_field = str(row[5]) == '1'                    
            else:
                is_date_field = False
            dictKey = field_name
            # for radio or checkboxes, the key needs to be a combination of field_name:number from answer_text
            if field_type == 'radio' or field_type == 'checkbox' or field_type == 'yesno':
                if answer_text == 'No Response':
                    dictKey = field_name + ':NORESPONSE'
                else:
                    if field_type == 'yesno':
                        strAnswerNum = '0'
                        if answer_text == 'yes':
                            strAnswerNum = '1'
                        dictKey = field_name + ':' + strAnswerNum
                    else:
                        dictKey = field_name + ':' + answer_text.split(',')[0]
                #dictKey = field_name + ':' + strAnswerNum
            dictFieldData[dictKey] = {'field_name':field_name, 'field_type':field_type,'field_label':field_label,'concept_code':concept_cd,'answer_text':answer_text,'loinc_item_code':loinc_item_code,'loinc_measure_code':loinc_measure_code, 'is_date_field':is_date_field}
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
        msg = "Error retrieving form list for project_id {0}.  Error {1}".format(project_id, e)
        logging.error(msg)
    return listProjectForms

# extract a list of all the fields for a given form and project_info using the REDCAP_ANSWER_MAPPING table
def loadFieldListDatabase(dbobj, project_id, formname):
    listFormField = []
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT DISTINCT field_name FROM {redcap_mapping_table} WHERE project_id = '{project_id}' AND form_name = '{formname}'".format(redcap_mapping_table=dictDatabaseSettings['answer_mapping_table'],project_id=project_id, formname=formname))
        rows = db_cursor.fetchall()
        for row in rows:
            listFormField.append(str(row[0])) 
    except Exception as e:
        msg = "Error retrieving field list for project_id {0} form {1}.  Error {2}".format(project_id, formname, e)
        logging.error(msg)
    return listFormField

# extract a list of all the REDCap record_ids for given project using the REDCAP_ANSWER_MAPPING table
def loadRecordListDatabase(dbobj, project_id):
    listRecordId = []
    try:        
        dbconn = dbobj.getConnection()
        db_cursor = dbconn.cursor()
        db_cursor.execute("SELECT DISTINCT record_id FROM {patient_mapping_table} WHERE project_id = '{project_id}'".format(patient_mapping_table=dictDatabaseSettings['patient_mapping_table'],project_id=project_id))
        rows = db_cursor.fetchall()
        for row in rows:
            listRecordId.append(str(row[0])) 
    except Exception as e:
        msg = "Error retrieving patient list for project_id {0}.  Error {1}".format(project_id, e)
        logging.error(msg)
    return listRecordId


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
    except Exception as e:
        msg = "Unexpected error: {0}".format(e)
        logging.critical(msg)
        print msg + "  Program stopped."
        exit(0)

    

def testConfiguration(project_info):
    msg = "Querying REDCap found these events {0} for project {1}".format(rcProject.getEvents(), project_info['project_name'])
    logging.debug(msg)
    
    dictEvents = loadEventMappingDatabase(dbobj, project_info['path_project_id'])
    msg = "Querying database found these events {0} for project {1}".format(dictEvents.keys(), project_info['project_name'])
    logging.debug(msg)
    
    msg = "Querying REDCap found these forms {0} for project {1}".format(sorted(rcProject.getForms()), project_info['project_name'])
    logging.debug(msg)

    msg = "Querying REDCap found these forms IN EVENTS {0} for project {1}".format(sorted(rcProject.getFormsInEvents()), project_info['project_name'])
    logging.debug(msg)
    
    listForms = loadFormListDatabase(dbobj, project_info['path_project_id'])
    msg = "Querying database found these forms {0} for project {1}".format(sorted(listForms), project_info['project_name'])
    logging.debug(msg)

def loadResponses(dbobj, listResponses, dictFormAnswerMap):
    """
    Load the responses extracted from REDCap by converting them to CDM compliant structures
    """
    for response in listResponses:
        singleFormData = transformSinglePatientFormData(dbobj, response, dictFormAnswerMap)
        writeCDMData(dbobj, singleFormData)

def writeCDMData(dbobj, listCDMData):
    for dataItem in listCDMData:
        sqlStmt = "INSERT INTO {0} (".format(dictDatabaseSettings['pro_cm_table'])
        for key in dataItem.keys():
            sqlStmt = sqlStmt + key + ", "
        # remove trailing comma and space and add next part of insert statement
        sqlStmt = sqlStmt[:len(sqlStmt)-2] + ") VALUES ("
        for key in dataItem.keys():
            if dataItem[key] == None:
                sqlStmt = sqlStmt + "NULL, "
            else:
                if dictPROCMTableDataTypes.has_key(key):
                    #Take specific action if the field is found in the PRO CM table dictionary
                    if dictPROCMTableDataTypes[key] == 'number': 
                        sqlStmt = sqlStmt + str(dataItem[key]) + ", "
                    elif dictPROCMTableDataTypes[key] == 'date': 
                        #TODO check if I need to add different code for SQL Server
                        #first, check if the data contains a ":" indicating a time
                        if str(dataItem[key]).find(":"):
                            sqlStmt = sqlStmt + "TO_DATE('{0}', 'YYYY/MM/DD HH24:MI:SS'), ".format(str(dataItem[key]))
                        else:
                            sqlStmt = sqlStmt + "TO_DATE('{0}', 'YYYY/MM/DD'), ".format(str(dataItem[key]))
                else:
                    # By default, treat any other column as a string
                    sqlStmt = sqlStmt + "'" + str(dataItem[key]) + "', "

        # remove trailing comma and space and add next part of insert statement
        sqlStmt = sqlStmt[:len(sqlStmt)-2] + ")"
        try:
            dbconn = dbobj.getConnection()
            db_cursor = dbconn.cursor()
            db_cursor.execute(sqlStmt)
            dbconn.commit()
        except Exception as e:
            msg = "Error executing SQL {0}.  Error {1}".format(sqlStmt, e)
            logging.error(msg)
            
        #print sqlStmt

def transformThisField(fieldItem, dictAnswerMap):
    """ 
    Return a flag determining if the current field should be processed or skipped.
    """
    # 'special' was a field type invented by this codebase so skip it
    if fieldItem['field_type'] == 'special':
        return False
    if fieldItem['field_type'] == 'radio' or fieldItem['field_type'] == 'checkbox' or fieldItem['field_type'] == 'yesno':
        # return the boolean indicating if the checkbox/radio/yesno field and its value are in the answer map
        if fieldItem.has_key('field_value') == False:
            return False
        return dictAnswerMap.has_key(fieldItem['field_name'] + ':' + fieldItem['field_value'])
    """Handle text fields:
    This takes some explaining.  There are boolean values that dictate whether or not
    text fields should be processed.  However, these booleans also need to recognize whether or not
    the text files in in the dictAnswerMap.   
    Case 1: return False if the current field is not in the dictAnswerMap
    Case 2: return True for date text fields if the field exists in dictAnswerMap.  These are always transformed
    Case 3: return True for text fields if the config.ini file says to load text fields and the field exists in dictAnswerMap
    """
    if fieldItem['field_type'] == 'text':
        if dictAnswerMap.has_key(fieldItem['field_name']) == False:
            return False
        currField = dictAnswerMap[fieldItem['field_name']]
        if currField['is_date_field'] == True:
            return True
        if dictRedcapSettings['load_text_fields'] == True:
            return True        
    return False

def cleanText(inputString):
    """Clean up the text.  Remove quotes and shorten the string so it fits into the database"""
    if inputString == None or len(inputString) == 0:
        return ''
    if inputString.find("'") > -1:
        return inputString.replace("'", "''")
    appendMsg = "...[Text is too long, you need to open the original survey to see the entire text]"
    if len(inputString) > 1024 - len(appendMsg):
        inputString[:1024 - len(appendMsg)] + appendMsg
    return inputString

def extractDate(inputString):
    # first trim this badboy.  If the string is empty, return None
    if inputString == None or len(inputString) == 0:
        return None
    retString = inputString.strip()
    if len(retString) == 0:
        return None
    try:
        return parse(retString)
    except Exception as e:
        msg = "Encountered error converting REDCap text date {0} into a valid date.  This text field will not be loaded into the CDM table.  Error {1}".format(inputString, e)
        logging.info(msg)
        return None        
    return None

def getConceptCode(fieldItem):
    pass

def transformSinglePatientFormData(dbobj, dictFormResponse, dictAnswerMap):
    """
    Transform the responses for a single patient and single form
    into a data structure formatted for CDM
    """
    listCDMData = []
    dictCDMRecord = {}
    patient_num = getPatientNum(dbobj, dictFormResponse['record_id'])
    pro_type = getPROType(dictFormResponse['form_name'])
    event_code = dictFormResponse['redcap_event_name']

    #hardcoded values
    pro_mode = 'NI'
    pro_cat = 'NI'
    pro_method = 'NI'

    pro_datetimestamp = stringToDateTime(dictFormResponse['form_timestamp'])
    #pro_time = stringToDateTime(dictFormResponse['form_timestamp'])[1]
    
    global pro_measure_seq
    pro_measure_seq = pro_measure_seq + 1
    
    dictFieldData = dictFormResponse['field_data']
    
    #TODO sync the key with loadCodesByFormDatabase 
    # I need to add the text answer to the fieldname.  Also need to handle freetext  Maybe fieldname:TEXTANSWERED
    for fieldItem in dictFieldData:
        #skip data that does not have a valid answer map entry:
        if transformThisField(fieldItem, dictAnswerMap) == False:
            continue
        
        dictCDMRecord = {}
        dictCDMRecord['patid'] = patient_num
        dictCDMRecord['pro_date'] = pro_datetimestamp.strftime('%Y/%m/%d')
        dictCDMRecord['pro_time'] = pro_datetimestamp.strftime('%H:%M')
        dictCDMRecord['pro_measure_seq'] = pro_measure_seq
        dictCDMRecord['event_code'] = event_code
        dictCDMRecord['pro_type'] = pro_type

        #hardcoded values
        dictCDMRecord['pro_mode'] = pro_mode
        dictCDMRecord['pro_method'] = pro_method
        dictCDMRecord['pro_cat'] = pro_cat
        
        global pro_cm_id
        pro_cm_id = pro_cm_id + 1
        dictCDMRecord['pro_cm_id'] = pro_cm_id
        dictCDMRecord['pro_item_name'] = fieldItem['field_name']
        #TODO: create some calls:
        #LOINCMEasure = getPROMeasureLOINC(formname)
        #dictCDMRecord['pro_measure_loinc'] = LOINCMeasure 
        #LOINCMEasureName = getPROMeasureName(formname)
        #dictCDMRecord['pro_measure_name'] = LOINCMEasureName 
        
        if fieldItem['field_type'] == 'radio' or fieldItem['field_type'] == 'checkbox' or fieldItem['field_type'] == 'yesno':
            #TODO: assemble the answer option
            if fieldItem.has_key('field_value') == False:
                fieldItem['field_value'] = 'NORESPONSE'
            if dictAnswerMap.has_key(fieldItem['field_name'] + ':' + fieldItem['field_value']):
                answerMap = dictAnswerMap[fieldItem['field_name'] + ':' + fieldItem['field_value']]
                dictCDMRecord['pro_item_loinc'] = answerMap['loinc_item_code']
                dictCDMRecord['pro_measure_loinc'] = answerMap['loinc_measure_code']
                dictCDMRecord['pro_response_text'] = cleanText(answerMap['answer_text'])
                dictCDMRecord['path_code'] = answerMap['concept_code']
                if fieldItem['field_value'] != 'NORESPONSE':
                    dictCDMRecord['pro_response_num'] = cleanText(fieldItem['field_value'])
                
        elif (fieldItem['field_type'] == 'text' or fieldItem['field_type'] == 'calc') and dictAnswerMap.has_key(fieldItem['field_name']):
            answerMap = dictAnswerMap[fieldItem['field_name']]
            dictCDMRecord['pro_item_loinc'] = answerMap['loinc_item_code']
            dictCDMRecord['pro_measure_loinc'] = answerMap['loinc_measure_code']
            # handle situation where the participant did respond
            if fieldItem.has_key('field_value') and fieldItem['field_value'] != 'NORESPONSE' and fieldItem['field_value'] != 'No Response':
                currentField = dictAnswerMap[fieldItem['field_name']]
                dictCDMRecord['path_code'] = str(answerMap['concept_code']).replace('NORESPONSE', 'ANSWERED')
                if currentField['is_date_field'] == False:
                    dictCDMRecord['pro_response_text'] = cleanText(fieldItem['field_value'])
                else:
                    dictCDMRecord['pro_response_date'] = extractDate(fieldItem['field_value'])
            # handle situation where the participant did not respond
            elif fieldItem.has_key('field_value') == False:
                dictCDMRecord['path_code'] = str(answerMap['concept_code'])
            #if fieldItem['field_value'] != 'NORESPONSE':
            #    dictCDMRecord['pro_response_num'] = cleanText(fieldItem['field_value'])
        else:
            print "found an item of type {0}.  I don't know what to do with it.  Look at this stuff too {2}".format(fieldItem['field_type'], fieldItem)
        if dictCDMRecord.has_key('pro_response_text') and dictCDMRecord.has_key('pro_response_num'):
            if dictCDMRecord['pro_response_text'] == 'No Response' and dictCDMRecord['pro_response_num'] != None:
                print "here line 513 {0}".format(dictCDMRecord)
        try:
            if dictCDMRecord.has_key('pro_response_num') and dictCDMRecord['pro_response_num'] != None:
                f = float(dictCDMRecord['pro_response_num'])
        except Exception as e:
            msg = "Error converting pro_response_num {0} to a floating point number.  For REDCap record id {1} PATID {2} form name {3} event name {4}  Error {1}".format(dictCDMRecord['pro_response_num'],dictFormResponse['record_id'], patient_num, dictFormResponse['form_name'],event_code, e)
            logging.error(msg)
        listCDMData.append(dictCDMRecord)
    return listCDMData

def getCodeForDataItem(fieldname, fielddata):
    pass

def stringToDateTime(timestamp):
    """Convert a REDCap date time string to a python datetime object.
    If the timestamp is not a valid date time, return today's date time.

    Args:
        timestamp: A string representing the REDCap date time format (ex: 5-14-2015  2:52:44 PM).
        
    Returns:
        A python datetime object.  If one cannot be extracted from the timestamp, today's timestamp is returned.
        The CDM specification requires a date in the PRO_CM table.
    """    
    dtReturn = datetime.datetime.now()
    if timestamp == None or len(timestamp) == 0:
        return dtReturn
    try:
        dtReturn = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        msg = "Error converting REDCap timestamp {0}.  Error {1}".format(timestamp, e)
        logging.error(msg)
        return datetime.datetime.now()
    
    return dtReturn

def getPatientNum(dbobj, record_id):
    if dictPatientMap.has_key(record_id):
        return dictPatientMap[record_id]
    else:
        raise Exception("Cannot find valid patient num for REDCap patient record_id: {0}".format(record_id))

def getPROType(formname):
    if formname == 'promis29':
        return 'PM'
    elif str(formname).startswith('neuroqol'):
        return 'NQ'
    else:
        return 'OT'

def etlProject(dbobj, project_info):
    loadSupportingProjectData(dbobj, project_info)
    listRecordId = loadRecordListDatabase(dbobj, project_info['site_project_id'])
    rcProject = RedcapProject(logging, dictRedcapSettings['api_url'], project_info['api_key'],
                                  project_info['project_name'], verify_ssl= dictRedcapSettings['verify_ssl'], lazy= dictRedcapSettings['lazy'])
    # load all the forms from the database.  Why the database and not REDCap?  Because REDCap will have a superset of forms.  The only forms we
    # care about is the subset of forms found in the database that we can map.
    listdbForms = loadFormListDatabase(dbobj, project_info['path_project_id'])
    for form in listdbForms:
        form_field_list = loadFieldListDatabase(dbobj,project_info['path_project_id'], form) 
        records = rcProject.getPatientData(listRecords=listRecordId, formname=form, listFields=form_field_list, export_survey_fields=True, format="xml")
        dictFormMapping = loadCodesByFormDatabase(dbobj, project_info['path_project_id'], form, rcProject)
        loadResponses(dbobj, records, dictFormMapping)
        #records = ET.fromstring(records.encode('utf-8'))
        #print records
        
    




if __name__ == "__main__":
    # use this command to disable the InsecurePlatformWarning (see http://stackoverflow.com/questions/29099404/ssl-insecureplatform-error-when-using-requests-package)
    requests.packages.urllib3.disable_warnings()
    logging.basicConfig(filename='redcap.log.' + str(os.getpid()),level=logging.DEBUG, format='%(asctime)s: %(levelname)s - %(message)s')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)

    # suppress the extraneous HTTP messages from the logs
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    today = datetime.datetime.now()
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
        logging.info('Finished ETL for Project: {0}'.format(project_info['project_name']))
    

