'''
Created on Feb 8, 2018

@author: chb69
'''

from PyCap.redcap import Project, RedcapError
import xml.etree.ElementTree as ET
import requests.packages.urllib3
import traceback
from distutils.version import StrictVersion
import re


class RedcapProject(object):
    """Main class for interacting with a single REDCap project"""

    # IMPORTANT!! This variable represents the "cutoff" between the NewRedcapXMLParser and the OldRedcapXMLParser
    # classes.  If the code encounters a version of REDCap less than the redcap_new_api_version, it will use the 
    # OldRedcapXMLParser to extract the data.  Otherwise, it will use the NewRedcapXMLParser.  Also note,
    # the version (6.11.2) is an approximate estimate.  We don't know exactly when REDCap changed their API, 
    # but this document may have relevant information on page 5: https://www.bu.edu/ctsi/files/2016/03/Version-6.11.2.docx 
    # The document includes the following item:
    #     "Change: The API method 'Export Instrument-Event Mappings' now returns a different structure if exporting as JSON or XML" 
    redcap_new_api_version = '6.11.2'

    def __init__(self, logging, url, token, name='', verify_ssl=True, lazy=False):
        if token == None or len(token) == 0:
            raise RedcapError('Cannot connect to REDCap project if token is empty')
        self.token = token
        self.name = name
        self.url = url
        if url == None or len(url) == 0:
            raise RedcapError('Cannot connect to REDCap project if url is empty')
        self.verify_ssl = bool(verify_ssl)
        self.lazy = bool(lazy)
        self.logging = logging
        self.project_conn = None
        self.checkProjectConnection()
        self.eventList = None
        self.formList = None

    def checkProjectConnection(self):
        """
        Attempt to establish a connection to the REDCap server API using the parameters set
        in the constructor.
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing the connection information for a single REDCap project
    
        Catches
        -------
            Exception: An error occurred accessing the event information on the REDCap server.

        """
        try:
            if self.project_conn == None:
                self.project_conn = Project(self.url, self.token, self.name, self.verify_ssl, self.lazy)
                self.project_conn.configure()
                # set the version based on the data returned from REDCap
                self.redcap_version = str(self.project_conn.redcap_version)
        except RedcapError as rce:
            msg = "Error connecting to REDCap for project {0}.  Error: {1}".format(self.name, rce)
            self.logging.error(msg)

    def getEvents(self):
        """
        Return a unique list of all the events with forms associated with them in the project_info.
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        eventList :
            a list of the all the events with forms associated with them in a project_info
            
        Catches
        -------
            Exception: An error occurred accessing the event information on the REDCap server.

        """
        if self.eventList == None:
            try:
                if StrictVersion(self.redcap_version) >= StrictVersion(self.redcap_new_api_version):
                    self.eventList = self.extract_events_new()
                else:
                    self.eventList = self.extract_events_old()        
            except Exception as e:
                msg = "Encountered an error extracting events for project {0}.  Error: {1}".format(self.name, e)
                self.logging.error(msg)
                tb = traceback.format_exc()
                print tb
                self.logging.debug(tb)
        return self.eventList
    
    def extract_events_new(self):
        """
        Return a unique list of all the events with forms associated with them in the project_info.  This
        code targets a "newer" version of REDCap API.  The method for extracting this data changed between older
        and newer versions of REDCap. 
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        eventList :
            a list of the all the events with forms associated with them in a project_info
            
        Catches
        -------
        Exception: 
            An error occurred accessing the event information on the REDCap server.

        """
        eventList = []
        try:
            project_fem = self.project_conn.export_fem(format="xml")
            items = ET.fromstring(project_fem.encode('utf-8'))
            for i, item in enumerate(items):
                arm_num = item[0].text
                unique_event_name = item[1].text
                if eventList.count(unique_event_name) == 0:
                    eventList.append(unique_event_name)
        except Exception as e:
            msg = "Encountered an error extracting forms from events for project {0}.  Error: {1}".format(self.name, e)
            self.logging.error(msg)
            tb = traceback.format_exc()
            print tb
            self.logging.debug(tb)
        return eventList
    
    def extract_events_old(self):
        """
        Return a unique list of all the events with forms associated with them in the project_info.  This
        code targets a "older" version of REDCap API.  The method for extracting this data changed between older
        and newer versions of REDCap. 
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        eventList :
            a list of the all the events with forms associated with them in a project_info
            
        Catches
        -------
        Exception: 
            An error occurred accessing the event information on the REDCap server.

        """
        eventList = []
        try:
            project_fem = self.project_conn.export_fem(format="xml")
            items = ET.fromstring(project_fem.encode('utf-8'))
            # loop form_event_mapping
            for arm in items:
                arm_num = arm.find('number').text
                #print "Extracting data for arm number: {0}".format(arm_num)
                for event in arm.findall('event'):
                    unique_event_name = event.find('unique_event_name').text
                    if eventList.count(unique_event_name) == 0:
                        eventList.append(unique_event_name)
        except Exception as e:
            msg = "Encountered an error extracting events for project {0}.  Error: {1}".format(self.name, e)
            self.logging.error(msg)
            tb = traceback.format_exc()
            print tb
            self.logging.debug(tb)
        return eventList
    
    def getForms(self):
        """
        Return a unique list of all the forms in the project_info.  This list retrieves forms 
        independent of events.
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        form_list :
            a list of the all the forms in a project_info

        Catches
        -------
        Exception: 
            An error occurred accessing the event information on the REDCap server.
        """
        if self.formList == None:
            try:
                metadata_results = self.project_conn.get_metadata(format="xml")
                items = ET.fromstring(metadata_results.encode('utf-8'))
                form_list = []
                for item in items:
                    children = item._children
                    for child_item in children:
                        if child_item.tag == 'form_name':
                            if form_list.count(child_item.text) == 0:
                                form_list.append(child_item.text)
                self.formList = form_list
            except Exception as e:
                msg = "Encountered an error extracting events for project {0}.  Error: {1}".format(self.name, e)
                self.logging.error(msg)
                tb = traceback.format_exc()
                print tb
                self.logging.debug(tb)
        return self.formList
    
    def getFormFields(self, formname):
        """
        Return a dictionary using the fields in a form as keys and the value is the "field_type" (ex: radio, text, checkbox, etc.).
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        form_list :
            a dictionary of the all the forms in a project as the keys and the field type as the values

        Catches
        -------
        Exception: 
            An error occurred accessing the event information on the REDCap server.
        """
        
        #TODO make this more of a singleton pattern.  Track the "current_form_name" in the class
        # then check if the incoming formname is the same.  If so, return the current array.  If not,
        # execute the lookup
        if formname == None or len(formname) == 0:
            raise RedcapError('Cannot load fields if formname is empty')
        form_field_list = {}
        # I need to explicitly extract the field names for a given form
        #project_conn = Project(self.url, self.token, verify_ssl=self.verify_ssl, lazy=self.lazy)
        form_metadata = self.project_conn.export_metadata(forms=[formname],format='xml')
        
        # Loop the rows to extract the field list
        field_records = ET.fromstring(form_metadata.encode('utf-8'))
        
        for field in field_records:
            if (field.find('field_name') != None) & (field.find('form_name') != None):
                if (field.find('form_name').text == formname):
                    form_field_list[field.find('field_name').text] = {'field_type' : field.find('field_type').text, 'field_label' : field.find('field_label').text }
        # add record_id field to all data requests.  record_id is the primary key for the patient.
        try:
            form_field_list.has_key('record_id')
        except ValueError as e:
            form_field_list['record_id'] = {'field_type' : 'special', 'field_label' : None }
    
        # add form specific timestamp record to the field list
        form_field_list[formname + '_timestamp'] = {'field_type' : 'special', 'field_label' : None }
        return form_field_list
    
    def getFormsInEvents(self):
        """
        Return a unique list of all the forms assigned to an event in the project.
    
        Parameters
        ----------
        project: self
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        event_forms :
            a list of the all the forms assigned to an event in a project
        """
        event_forms = []
        try:
            if StrictVersion(self.redcap_version) >= StrictVersion(self.redcap_new_api_version):
                return self.extract_forms_from_events_new()
            else:
                return self.extract_forms_from_events_old()        
        except Exception as e:
            print "Encountered an error extracting forms from events."
            print e
            tb = traceback.format_exc()
            print tb
        return event_forms
    
    
    def extract_forms_from_events_new(self):
        """
        Return a unique list of all the forms associated with events in the project_info.  This
        code targets a "new" version of REDCap API.  The method for extracting this data changed between older
        and newer versions of REDCap. 
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        event_forms :
            a list of the all the forms associated with events in a project_info
            
        Catches
        -------
        Exception: 
            An error occurred accessing the event information on the REDCap server.

        """
        project_fem = self.project_conn.export_fem(format="xml")
        items = ET.fromstring(project_fem.encode('utf-8'))
        event_forms = []
        try:
            for i, item in enumerate(items):
                arm_num = item[0].text
                unique_event_name = item[1].text
                form_name = item[2].text
    
                if event_forms.count(form_name) == 0:
                    event_forms.append(form_name)
        except Exception as e:
            msg = "Encountered an error extracting forms for events for project {0}.  Error: {1}".format(self.name, e)
            self.logging.error(msg)
            tb = traceback.format_exc()
            print tb
            self.logging.debug(tb)
        return event_forms
    
    def extract_forms_from_events_old(self):
        """
        Return a unique list of all the forms associated with events in the project_info.  This
        code targets a "older" version of REDCap API.  The method for extracting this data changed between older
        and newer versions of REDCap. 
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
    
        Returns
        -------
        event_forms :
            a list of the all the forms associated with events in a project_info
            
        Catches
        -------
        Exception: 
            An error occurred accessing the event information on the REDCap server.

        """
        project_fem = self.project_connt.export_fem(format="xml")
        items = ET.fromstring(project_fem.encode('utf-8'))
        event_forms = []
        try:
            # loop form_event_mapping
            for arm in items:
                arm_num = arm.find('number').text
                #print "Extracting data for arm number: {0}".format(arm_num)
                for event in arm.findall('event'):
                    unique_event_name = event.find('unique_event_name').text
                    #print "Extracting data for event: {0}".format(unique_event_name)
                    for form in event.findall('form'):
                        # append form_name
                        if event_forms.count(form.text) == 0:
                            event_forms.append(form.text)
        except Exception as e:
            msg = "Encountered an error extracting forms for events for project {0}.  Error: {1}".format(self.name, e)
            self.logging.error(msg)
            tb = traceback.format_exc()
            print tb
            self.logging.debug(tb)
        return event_forms


    def getPatientData(self, listRecords, formname, listFields, export_survey_fields=True, format="xml"):
        """
        Return an array of the data for all the patient responses to a single form.  The array contains dictionaries.
        Each dictionary will contain 3 default entries: 
        * record_id- the patient's record id
        * redcap_event_name- the name of the event associated with the form
        * form_timestamp- the timestamp the form was completed (if available)
        * form_name- the name of the form
        The dictionary will also include data from the other form fields.  In most cases the dictionary entries will have a
        single value.  For checkbox data, the entry will have a list.
    
        Parameters
        ----------
        self: object
            A RedcapProject object representing an API connection to a single REDCap project
        listRecords: list
            An array of patient data for a particular form.  Each element represents a single patient's responses to the form
        formname: string
            The name of the survey form providing the data
        listFields: list
            An array of the fields to find in each of the patient's data.  Not all the fields are exported so this represents
            the subset of fields needed for data extraction.
        export_survey_fields: boolean
            A boolean passed to the REDCap API call
        format: string
            A string representing the output format to use when extracting the data.  This is also passed to the REDCap API call.
    
        Returns
        -------
        patient_data : list
            An array of the patient data extracted from the form.  Each element of the array is a dictionary
            
        Catches
        -------
        Exception: 
            An error occurred accessing the event information on the REDCap server.

        """
        records = self.project_conn.export_records(records=listRecords, forms=[formname], fields=None, export_survey_fields=True, format="xml")
        #print records
        records = ET.fromstring(records.encode('utf-8'))
        listRedcapFields = self.getFormFields(formname)
        patient_data_set = []
        for item in records:
            patient_data = {}
            # load special fields
            if item.find('record_id') == None:
                raise Exception('record_id is blank. No data for this patient can be processed')
            record_id = item.find('record_id').text
            patient_data['record_id'] = record_id
            if item.find('redcap_event_name') == None:
                raise Exception('redcap_event_name is blank. No data for this patient can be processed')
            redcap_event_name = item.find('redcap_event_name').text
            patient_data['redcap_event_name'] = redcap_event_name
            form_timestamp_string = None
            if item.find(formname + '_timestamp') != None:
                form_timestamp_string = item.find(formname + '_timestamp').text
            patient_data['form_timestamp'] = form_timestamp_string
            patient_data['form_name'] = formname
            # load remaining fields
            
            #have the following items be part of the dictionary: record_id, redcap_event_name, form_timestamp, form_name,            
            # and form_data.  Have form_data be an array of dictionaries like this:
            # [ {"field_name":"id_06", "field_value":"3","field_type":checkbox"},
            # {"field_name":"id_06", "field_value":"5","field_type":checkbox"},
            # {"field_name":"text_field", "field_value":"here is my text", "field_type":"text"...]
            
            form_data = []
            for field in listRedcapFields:
                form_data_item = {}
                form_data_item['field_name'] = field
                form_data_item['field_type'] = listRedcapFields[field]['field_type']
                #if listRedcapFields[field] != 'checkbox' and listRedcapFields[field] != 'radio' and listRedcapFields[field] != 'yesno' :
                #    print "Field type" + listRedcapFields[field]
                if listRedcapFields[field]['field_type'] == 'checkbox':
                    # this code handles checkboxes.  Checkbox data varies from the field name
                    # A checkbox with 5 choices for field name bckgr12 creates several entries
                    # in the patient data.  These data fields are called bckgr12___1, bckgr12___2, ..., bckgr12___5 
                    for rcField in item:
                        tagName = rcField.tag
                        # for each patient data element with a checkbox do the following:
                        # 1.  check the tagName includes '___' and the text is set to 1
                        # 2.  split the option code from the rest of the field name
                        # 3.  write the option code as the value for the field 
                        if tagName.startswith(field + '___') and rcField.text == "1":
                            option_num = tagName[tagName.rindex("___") + 3:]
                            form_data_item['field_value'] = option_num
                elif listRedcapFields[field]['field_type'] == 'text':
                    #TODO: handle text fields
                    if item.find(field) != None:
                        if item.find(field).text != None:
                           form_data_item['field_value'] = item.find(field).text
                        else:
                            form_data_item['field_value'] = 'NORESPONSE'
                # TODO: need to add logic to handle other data types radio, yesno
                else:
                    if item.find(field) != None:
                        if item.find(field).text != None:
                           form_data_item['field_value'] = item.find(field).text
                        else:
                            form_data_item['field_value'] = 'NORESPONSE'
                form_data.append(form_data_item)
            patient_data['field_data'] = form_data
            patient_data_set.append(patient_data)        
        return patient_data_set

    def getTimestamp(self, fieldname):
        pass

