'''
Created on Mar 5, 2018

@author: chb69
'''


from Oracle import Oracle
from SqlServer import SqlServer
import datetime
import time
import sys
import traceback
import copy
import ConfigParser
import os
import logging
import ast 
import unittest
from dateutil.parser import *
from dateutil.tz import *


class TestRedcapLoad(unittest.TestCase):
    #initialize a dictionary with all the variables relating to the DATABASE section
    #This allows the code to loop through the section while loading the variables into the dictionary
    dictDatabaseSettings = {'dbms' : None, 'host' : None, 'port' : None, 'sid': None, 'dbname' : None, 'dbuser' : None, 'dbpassword' : None, 
                            'event_mapping_table' : None, 'answer_mapping_table' : None, 'patient_mapping_table' : None, 
                            'pro_cm_table' : None}
    
    # The object representing a database connection
    dbobj = None

    def setUp(self):
        self.loadConfigFile()
        self.dbobj = Oracle(self.dictDatabaseSettings['host'], self.dictDatabaseSettings['port'], self.dictDatabaseSettings['sid'],self.dictDatabaseSettings['dbuser'], self.dictDatabaseSettings['dbpassword'])

    
    
    def loadConfigFile(self):
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
            #print "Starting loading config.ini"
            config.read('config.ini')
            for k in self.dictDatabaseSettings.keys():
                self.dictDatabaseSettings[k] = config.get('DATABASE', k)
                                     
            #print "Finished loading config.ini"
        except OSError as err:
            msg = "OS error.  Check config.ini file to make sure it exists and is readable: {0}".format(err)
            print msg + "  Program stopped."
            exit(0)
        except ConfigParser.NoSectionError as noSectError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(noSectError)
            print msg + "  Program stopped."
            exit(0)
        except ConfigParser.NoOptionError as noOptError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(noOptError)
            print msg + "  Program stopped."
            exit(0)
        except SyntaxError as syntaxError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(syntaxError)
            msg = msg + "  Cannot read line: {0}".format(syntaxError.text)
            print msg + "  Program stopped."
            exit(0)        
        except AttributeError as attrError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(attrError)
            msg = msg + "  Cannot read line: {0}".format(attrError.text)
            print msg + "  Program stopped."
            exit(0)        
        except:
            msg = "Unexpected error:", sys.exc_info()[0]
            print msg + "  Program stopped."
            exit(0)

    def testNewDataVersusOld(self):
        """ Compare the data found in an old CDM 3.1 PRO_CM table with the data loaded
        into the CDM 4.0 PRO_CM table.  The record counts should be equal.
        """
        # bad developer...bad bad
        # hardcoded old table name
        pro_cm_31_table = 'pro_cm_old_data'
        sqlNew = """select count(*) from {0}
                where (path_code like 'PRO:1000_f7_q5%'
                or path_code like 'PRO:1001_f8_q4%'
                or path_code like 'PRO:1002_f6_q5%'
                or path_code like 'PRO:1000_f7_q12%'
                or path_code like 'PRO:1001_f8_q11%'
                or path_code like 'PRO:1002_f6_q12%'
                or path_code like 'PRO:1000_f7_q14%'
                or path_code like 'PRO:1001_f8_q13%'
                or path_code like 'PRO:1002_f6_q14%'
                or path_code like 'PRO:1000_f7_q26%'
                or path_code like 'PRO:1001_f8_q25%'
                or path_code like 'PRO:1002_f6_q26%'
                or path_code like 'PRO:1000_f7_q9%'
                or path_code like 'PRO:1001_f8_q8%'
                or path_code like 'PRO:1002_f6_q9%'
                or path_code like 'PRO:1000_f7_q20%'
                or path_code like 'PRO:1001_f8_q19%'
                or path_code like 'PRO:1002_f6_q20%')
                and PATH_CODE not like '%NORESPONSE'""".format(self.dictDatabaseSettings['pro_cm_table'])
        sqlOld = "select count(*) from {0}".format(pro_cm_31_table)
        #set these two variables to different values by default.  That way, the assert is a valid test
        iNewCount = 0
        iOldCount = -1
        try:        
            dbconn = self.dbobj.getConnection()
            db_cursor = dbconn.cursor()
            db_cursor.execute(sqlNew)
            row = db_cursor.fetchone()
            iNewCount = int(row[0]) 
            db_cursor.execute(sqlOld)
            row = db_cursor.fetchone()
            iOldCount = int(row[0]) 
            self.assertEqual(iNewCount, iOldCount, "incorrect number of old records {0} compared to new records {1}".format(iOldCount, iNewCount))
        except Exception as e:
            msg = "Error comparing old PRO_CM counts with new PRO_CM counts.  Error {1}".format(e)
            print msg

    def genericNullTest(self, column_name):
        sqlTest = """select count(*) from {0}
                where {1} IS NULL""".format(self.dictDatabaseSettings['pro_cm_table'], column_name)
        try:        
            dbconn = self.dbobj.getConnection()
            db_cursor = dbconn.cursor()
            db_cursor.execute(sqlTest)
            row = db_cursor.fetchone()
            iTestCount = int(row[0]) 
            self.assertEqual(iTestCount, 0, "Found {0} records with NULL {1}".format(iTestCount, column_name))
        except Exception as e:
            msg = "Error checking NULL {0}.  Error {1}".format(column_name, e)
            print msg

    def testNoResponse(self):
        """
        Check to make sure no rows contain both response data AND a NORESPONSE code
        """
        
        sqlTest = """select count(*) from {0}
            where PATH_CODE like '%NORESPONSE'
            and (PRO_RESPONSE_TEXT IS NOT NULL
            OR PRO_RESPONSE_NUM IS NOT NULL
            OR PRO_RESPONSE_DATE IS NOT NULL)""".format(self.dictDatabaseSettings['pro_cm_table'])
        try:        
            dbconn = self.dbobj.getConnection()
            db_cursor = dbconn.cursor()
            db_cursor.execute(sqlTest)
            row = db_cursor.fetchone()
            iTestCount = int(row[0]) 
            self.assertEqual(iTestCount, 0, "Found {0} records response values but a PATH_CODE set to NORESPONSE".format(iTestCount))
        except Exception as e:
            msg = "Error checking NULL {0}.  Error {1}".format(column_name, e)
            print msg

    def testNullPathCode(self):
        self.genericNullTest("PATH_CODE")       

    def testNullProItemName(self):
        self.genericNullTest("PRO_ITEM_NAME")       

    def testNullProDate(self):
        self.genericNullTest("PRO_DATE")       

    def testNullProTime(self):
        self.genericNullTest("PRO_TIME")       

    def testNullProMethod(self):
        self.genericNullTest("PRO_METHOD")       

    def testNullProMode(self):
        self.genericNullTest("PRO_MODE")       

    def testNullProCat(self):
        self.genericNullTest("PRO_CAT")       

    def testNullEventCode(self):
        self.genericNullTest("EVENT_CODE")       

    def testNullProType(self):
        self.genericNullTest("PRO_TYPE")       

    def testProMeasureSeq(self):
        self.genericNullTest("PRO_MEASURE_SEQ")       

    def testPatId(self):
        self.genericNullTest("PATID")       
    
        

if __name__ == "__main__":
    unittest.main()
