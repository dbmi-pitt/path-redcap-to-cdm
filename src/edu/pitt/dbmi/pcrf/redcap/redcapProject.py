'''
Created on Feb 8, 2018

@author: chb69
'''

from PyCap.redcap import Project, RedcapError
import xml.etree.ElementTree as ET
import requests.packages.urllib3

class RedcapProject(object):
    """Main class for interacting with a single REDCap project"""

    def __init__(self, logging, url, token, name='', verify_ssl=True, lazy=False, redcap_version):
        self.token = token
        self.name = name
        self.url = url
        self.verify = verify_ssl
        self.redcap_version = redcap_version
        self.logging = logging

    def getEvents(self):
        pass
    
    def getForms(self):
        pass
    
    def getFormFields(self, formname):
        pass
    
    def getFieldInfo(self, fieldname):
        pass
