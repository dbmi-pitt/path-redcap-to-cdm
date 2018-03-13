# Code Overview
---
The REDCap python code for the PaTH project extracts patient responses stored in REDCap
and loads them into PCORI CDM 4.0.  It performs three overall tasks:

1.  Maps REDCap patient identifiers to their CDM PATID using a pre-existing patient mapping table
2.  Connects to REDCap server (using configuration data from config.ini) and extracts the survey data
3.  Writes the survey data into the PRO_CM CDM 4.0 table

# Installation Steps
---

## Install all the python dependencies
---

The code requires a python interface to REDCap called PyCap.  PyCap has a dependency called Requests.
These dependencies can be installed individually or using the requirements.txt file in a 'pip install' command:

```
pip install -r requirements.txt 
```

## Update Database
---

The REDCap python code assumes certain tables exist and are populated before they are run.
These tables are referenced in the config.ini (see below).  These file to help create
and populate these tables are found in the /sql_files directory.

#### event_mapping_table
The event_mapping_table must exist and be populated *before* the python code is run.  
This table is created by the sql_files/create_event_mapping_table_oracle.sql script (Oracle only).  You 
populate this table using either Rsql_files/EDCAP_EVENT_MAPPING.csv or sql_files/REDCAP_EVENT_MAPPING.sql (Oracle only).
These files contain a default set of data.  After you populate the table, make sure all the unique_event_name
 and arm_num data matches your REDCap data dictionary.

#### answer_mapping_table
The answer_mapping_table must exist and be populated *before* the python code is run.
This table is created by the sql_files/create_answer_mapping_table_oracle.sql script (Oracle only).  You 
populate this table using either sql_files/REDCAP_ANSWER_MAPPING.csv or sql_files/REDCAP_ANSWER_MAPPING.sql (Oracle only).
These files contain a default set of data.  After you populate the table, make sure all the form_name
 and field_name data matches your REDCap data dictionary.

#### patient_mapping_table
The patient_mapping_table must exist and be populated *before* the python code is run.  
This table is created by the sql_files/create_redcap_patient_mapping_table_oracle.sql script (Oracle only).
You must map the REDCap record_id values to your PATID in your CDM DEMOGRAPHIC table.

#### pro_cm_table
This is the name of the CDM 4.0 PRO_CM table in your CDM schema.  Typically this is 'PRO_CM'.  The data from the python scripts 
will be written to this table.  This table is created by the sql_files/create_pro_cm_table_oracle.sql script (Oracle only).

## config.ini
---

Before running any python scripts create a new file call *config.ini*, 
based on the content in *config.ini.example* file.  you must edit the 
config.ini file with your site settings.  There are two groups of settings: Database and REDCap. 

### Database Settings
---

#### dbms 
This flag indicates what brand of database to leverage.  Currently the choices are 'Oracle' and 'SQLServer'.
Check to ensure the computer you deploy the python scripts has the correct database drivers to access the chosen dbms.

#### host
The server name hosting your database.  Check to ensure the computer you deploy the python scripts to can access this host.

#### port
The port number used to access the database.  Check to ensure the computer you deploy the python scripts to have the correct 
firewall settings so it can access the host via this port.

#### dbuser
The username for the database user.  Check to ensure this account can access the tables listed in the 
config.ini (ex: patient_mapping_table, pro_cm_table, etc.).  The code needs to write data to the pro_cm_table.

#### dbpassword
The password for the aforementioned dbuser.

#### sid (Oracle setting ONLY)
The Oracle SID for the Oracle database.  This only applies to Oracle servers.

#### dbname (SQLServer setting ONLY)
The dbname for the SQL Server database.  This setting only applies to SQL Server databases.

#### event_mapping_table
This is the name of the event_mapping_table in the database specified above.  The event_mapping_table must
exist and be populated *before* the python code is run.  

#### answer_mapping_table
This is the name of the answer_mapping_table in the database specified above.  The answer_mapping_table must
exist and be populated *before* the python code is run.  

#### patient_mapping_table
This is the name of the patient_mapping_table in the database specified above.  The patient_mapping_table must
exist and be populated *before* the python code is run.  .
You must map the REDCap record_id values to your PATID in your CDM DEMOGRAPHIC table.

#### pro_cm_table
This is the name of the CDM 4.0 PRO_CM table in your CDM schema.  It should be called 'PRO_CM'.  The data from the python scripts 
will be written to this table.

### REDCap Settings
---

#### api_url
Change the api_url variable to your REDCap API URL.  It is most likely your REDCap web address plus '/api/'.  For example: 
```https://www.ctsiredcap.myinstitution.edu/redcap/api/```

#### redcap_project_info
The config.ini file contains an array called redcap_project_info.  This array contains entries for the REDCap projects.
For each project supply the following information: site_project_id and api_key.  

- Modify the site_project_id to match what is found in your local REDCap instance.
- Modify the api_key for the project to match your REDCap API key*.

   *NOTE: You will need to generate a key for each REDCap project (IPF, AFib, Weight).  To generate API keys for your REDCap projects, follow these instructions:
http://redi.readthedocs.org/en/latest/add_new_redcap_project.html

#### load_text_fields
A boolean indicating if you want to process all the available text fields from REDCap.  True means, write the text data to the pro_cm_table.  False means do not write
text data to pro_cm_table.     

#### verify_ssl
A boolean indicating whether or not to contact the REDCap server via an SSL socket.  True indicates the REDCap server is contacted via an SSL socket.  False indicates the REDCap server is not accessed via SSL.  This flag depends on how your REDCap server's API is configured and its firewall rules.  
This flag is found in the third party REDCap python code.

#### lazy
A boolean indicating if the third party REDCap code should fetch data in a lazy manner.  True indicates the data from the REDCap server should only 
load the minimal amount of data with each call to the server.  False indicates the server should load all the information with each call.  Suggested
value for this setting is ```False```.  This flag is found in the third party REDCap python code. 

#### logging_level
A flag corresponding to the python logging software.  This flag dictates how verbose the log files are when running the script.  
Set this value to one of the following options: ERROR, WARNING, INFO, DEBUG.  Suggested value for this setting is ```ERROR```  

More information is available here:
https://docs.python.org/2/howto/logging.html#logging-levels  

# Running the Code
---
You can use the run_redcap.sh file on a Unix machine or use the command below to run the REDCap code.  If running the code against an Oracle database, the code requires environment variables for ORACLE_HOME and LD_LIBRARY_PATH to be set.  Please use python 2.7 since the code has not been tested on any other version of python.  

```
python2.7 redcapToCdm.py
```
