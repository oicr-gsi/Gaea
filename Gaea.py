# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 13:37:40 2018

@author: rjovelin
"""

import json
import subprocess
import time
import pymysql
import os
import argparse
import requests
import uuid
import xml.etree.ElementTree as ET
import gzip
from ftplib import FTP


def extract_credentials(credential_file):
    '''
    (str) -> dict
    
    Returns a dictionary with the database and EGA boxes credentials
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    '''
    
    D = {}            
    infile = open(credential_file)            
    for line in infile:
        if line.rstrip() != '':
            line = line.rstrip().split('=')
            D[line[0].strip()] = line[1].strip()
    infile.close()        
    return D


def connect_to_database(credential_file, database):
    '''
    (str, str) -> pymysql.connections.Connection
    
    Open a connection to the EGA database by parsing the CredentialFile
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - database (str): Name of the database
    '''

    # get the database credentials
    credentials = extract_credentials(credential_file)
    DbHost = credentials['DbHost']
    DbUser, DbPasswd = credentials['DbUser'], credentials['DbPasswd']
    
    try:
        conn = pymysql.connect(host = DbHost, user = DbUser, password = DbPasswd,
                               db = database, charset = "utf8", port=3306)
    except:
        try:
            conn = pymysql.connect(host=DbHost, user=DbUser, password=DbPasswd, db=database)
        except:
            raise ValueError('cannot connect to {0} database'.format(database))
    return conn


def show_tables(credential_file, database):
    '''
    (str) -> list
    
    Returns a list of tables in the EGA database
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    '''
    
    # connect to EGA database
    conn = connect_to_database(credential_file, database)
    # make a list of database tables
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    tables = [i[0] for i in cur]
    conn.close()
    return tables

 
def get_working_directory(S, working_dir):
    '''
    (str, str) -> str
    Returns a subdirectory in where encrypted and md5sum files are written 
    by appending S to working_dir
    
    Parameters
    ----------
    - S (str): Subdirectory in working_dir in which md5sums and encrypted files are written 
    - working_dir (str): Directory where sub-directories used for submissions are written
    '''
    
    return os.path.join(working_dir, S)



def add_working_directory(credential_file, database, table, box, working_dir):
    '''
    (str, str, str, str, str) --> None
    Create unique directories in file system for each alias in table and given Box
    and record working directory in database table

    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - database (str): Name of the database
    - table (str): Table name in database
    - box (str): EGA box
    - working_dir (str): Directory where sub-directories used for submissions are written
    '''
    
    # check if table exists
    tables = show_tables(credential_file, database)
    
    if table in tables:
        # connect to db
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        # get the alias with valid status
        cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"valid\" and {0}.egaBox=\"{1}\"'.format(table, box))
        data = cur.fetchall()
        if len(data) != 0:
            # loop over alias
            for i in data:
                alias = i[0]
                # create working directory with random unique identifier
                UID = str(uuid.uuid4())             
                # record identifier in table, create working directory in file system
                cur.execute('UPDATE {0} SET {0}.WorkingDirectory=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, UID, alias, box))  
                conn.commit()
                # create working directories
                working_dir = get_working_directory(UID, working_dir)
                os.makedirs(working_dir)
        conn.close()
        
        # check that working directory was recorded and created
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        # get the alias and working directory with valid status
        cur.execute('SELECT {0}.alias, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"valid\" and {0}.egaBox=\"{1}\"'.format(table, box))
        data = cur.fetchall()
        if len(data) != 0:
            for i in data:
                error = []
                alias = i[0]
                working_dir = get_working_directory(i[1], working_dir)
                if i[1] in ['', 'NULL', '(null)']:
                    error.append('Working directory does not have a valid Id')
                if os.path.isdir(working_dir) == False:
                    error.append('Working directory not generated')
                # check if error message
                if len(error) != 0:
                    # error is found, record error message, keep status valid --> valid
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, ';'.join(error), alias, box))  
                    conn.commit()
                else:
                    # no error, update Status valid --> start
                    cur.execute('UPDATE {0} SET {0}.Status=\"encrypt\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box))  
                    conn.commit()
        conn.close()            


def format_data(L):
    '''
    (list) -> tuple
    Returns a tuple with data to be inserted in a database table 
        
    Parameters
    ----------
    - L (list): List of data to be inserted into database table
    '''
    
    # create a tuple of strings data values
    Values = []
    # loop over data 
    for i in range(len(L)):
        if L[i] == '' or L[i] == None or L[i] == 'NA':
            Values.append('NULL')
        else:
            Values.append(str(L[i]))
    return tuple(Values)


def list_enumerations(URL='https://ega-archive.org/submission-api/v1/'):
    '''
    (str) -> dict
    
    Returns a dictionary with EGA enumerations as key and dictionary of metadata as value
    Precondition: the list of enumerations available from EGA is hard-coded

    Parameters
    ----------
    - URL (str): URL of the API. Default is 'https://ega-archive.org/submission-api/v1/'
    '''
    
    # build the URL    
    URL = format_url(URL)
    URL = URL + 'enums/'
    # list all enumerations available from EGA
    L = ['analysis_file_types', 'analysis_types', 'case_control', 'dataset_types', 'experiment_types',
         'file_types', 'genders', 'instrument_models', 'library_selections', 'library_sources',
         'library_strategies', 'reference_chromosomes', 'reference_genomes', 'study_types']
    URLs = [os.path.join(URL, i) for i in L]
    # create a dictionary to store each enumeration
    enums = {}
    for URL in URLs:
        # create a dict to store the enumeration data (value: tag}
        d = {}
        # retrieve the information for the given enumeration
        response = requests.get(URL)
        # check response code
        if response.status_code == requests.codes.ok:
            # loop over dict in list
            for i in response.json()['response']['result']:
                if 'instrument_models' in URL:
                    if i['value'] == 'unspecified':
                        # grab label instead of value
                        assert i['label'] not in d
                        d[i['label']] = i['tag']
                    else:
                        assert i['value'] not in d
                        d[i['value']] = i['tag']
                elif 'reference_chromosomes' in URL:
                    # grab value : tag
                    # group corresponds to tag in reference_genomes. currently, does not suppot patches
                    # group = 15 --> tag = 15 in reference_genomes = GRCH37
                    # group = 1 --> tag = 1 in reference_genomes = GRCH38
                    if i['group'] in ['1', '15']:
                        assert i['value'] not in d
                        d[i['value']] = i['tag']
                else:
                    assert i['value'] not in d
                    d[i['value']] = i['tag']
        enums[os.path.basename(URL).title().replace('_', '')] = d
    return enums


def record_message(credential_file, database, table, box, alias, message, status):
    '''
    (str, str, str, str, str, str, str) -> None
    Update the error message or the submission status for a given alias and box in
    database table if status is respectively "Error" or "Status"
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - database (str): Name of the database
    - table (str): Table name in database
    - box (str): EGA box
    - alias (str): Unique alias (primary key) of EGA object in table  
    - message (str): Error message or submission status
    - status (str): Submission status of the EGA object
    '''
    
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    if status == 'Error':
        # record error message
        cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" and {0}.egaBox=\"{3}\"'.format(table, message, alias, box))
    elif status == 'Status':
        # record submission status
        cur.execute('UPDATE {0} SET {0}.submissionStatus=\"{1}\" WHERE {0}.alias="\{2}\" AND {0}.egaBox=\"{3}\"'.format(table, message, alias, box))
    conn.commit()
    conn.close()
 
    
def delete_validated_objects_with_errors(credential_file, database, table, box, ega_object, URL, submission_status):
    '''
    (str, str, str, str, str, str) - > None
    
    Deletes the corresponding ega_object with submission_status being VALIDATED_WITH_ERRORS,
    VALIDATED or DRAFT from the EGA API.
    This step is requires prior submitting metadata for the ega_object if a previous attempt
    didn't complete or the new submission will result in error because the object already exists
        
    Parameters
    ----------
    
    - credential_file (str): File with ega-box and database credentials
    - database (str): Name of the submission database
    - table (str): Table in database
    - box (str): EGA submission box (ega-box-xxxx)
    - ega_object 
    - URL (str): URL of the EGA API
    - submission_status (str): ega_object submission status. Valid status:
                               (VALIDATED_WITH_ERRORS, VALIDATED, DRAFT)
    '''

    # grab all aliases with submit status
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    try:
        cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"submit\" AND {0}.egaBox=\"{1}\"'.format(table, box))
        # extract all information 
        data = cur.fetchall()
    except:
        data = []
    conn.close()
    
    # check if alias with submit status
    if len(data) != 0:
        # extract the aliases
        aliases = [i[0] for i in data]
        # parse credentials to get userName and Password
        credentials = extract_credentials(credential_file)
        # create json with credentials
        submission_data = {"username": box, "password": credentials[box], "loginType": "submitter"}
        # re-format URL if ending slash missing
        URL = format_url(URL)
        # connect to api and get a token
        token = connect_to_api(box, credentials[box], URL)
        # retrieve all objects with submission_status
        headers = {"Content-type": "application/json", "X-Token": token}
        response = requests.get(URL + '{0}?status={1}&skip=0&limit=0'.format(ega_object, submission_status), headers=headers, data=submission_data)
        # loop over aliases
        for i in range(len(aliases)):
            objectId = ''
            for j in response.json()['response']['result']:
                # check if alias with validated_with_errors status
                if j["alias"] == aliases[i]:
                    objectId = j['id']
                    if objectId != '':
                        # delete object
                        requests.delete(URL + '/{0}/{1}'.format(ega_object, objectId), headers=headers)
                        break
        # disconnect from api
        requests.delete(URL + 'logout', headers={"X-Token": token})     

    
def register_objects(credential_file, database, table, box, ega_object, portal):
    '''
    (str, str, str, str, str, str) -> None
        
    Register the ega_objects in EGA box using the submission portal by
    submitted a json for each ega object in table of database with submit status 
        
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - database (str): Name of the submission database
    - table (str): Table in database
    - box (str): EGA submission box (ega-box-xxxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - portal (str): URL address of the EGA submission API
    '''
    
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    try:
        cur.execute('SELECT {0}.Json, {0}.egaAccessionId FROM {0} WHERE {0}.Status=\"submit\" AND {0}.egaBox=\"{1}\"'.format(table, box))
        # extract all information 
        data = cur.fetchall()
    except:
        # record error message
        data = []
    conn.close()
        
    # check that objects in submit mode do exist
    if len(data) != 0:
        # make a list of jsons. filter out filesobjects already registered that have been re-uploaded because not archived
        L = [json.loads(i[0].replace("'", "\""), strict=False) for i in data if not i[1].startswith('EGA')]
        
        # format chromosomeReferences field
        for i in range(len(L)):
            if 'chromosomeReferences' in L[i]:
                if L[i]['chromosomeReferences'] != []:
                    for j in range(len(L[i]['chromosomeReferences'])):
                        if L[i]['chromosomeReferences'][j]['label'] == 'None':
                            L[i]['chromosomeReferences'][j]['label'] = None
        
        # connect to EGA and get a token
        credentials = extract_credentials(credential_file)
        # make sure portal ends with a slash
        portal = format_url(portal)
        # connect to API and open a submission for each object
        for J in L:
            # record error message if no token or open submission if token is obtained
            try:
                token = connect_to_api(box, credentials[box], portal)
            except:
                # record error message
                record_message(credential_file, database, table, box, J["alias"], 'Cannot obtain a token', 'Error')                
            else:
                # open a submission with token
                headers = {"Content-type": "application/json", "X-Token": token}
                submission_json = {"title": "{0} submission".format(ega_object), "description": "opening a submission for {0} {1}".format(ega_object, J["alias"])}
                open_submission = requests.post(portal + 'submissions', headers=headers, data=str(submission_json).replace("'", "\""))
                # record error if submission Id is not retrieved or create object if submission successfully open
                try:
                    # get submission Id
                    submissionId = open_submission.json()['response']['result'][0]['id']
                except:
                    # record error message
                    record_message(credential_file, database, table, box, J["alias"], 'Cannot obtain a submissionId', 'Error') 
                else:
                    # create object
                    object_creation = requests.post(portal + 'submissions/{0}/{1}'.format(submissionId, ega_object), headers=headers, data=str(J).replace("'", "\""))
                    # record status (DRAFT) and validate object if created or record error message. status --> VALIDATED or VALITED_WITH_ERRORS 
                    try:
                        objectId = object_creation.json()['response']['result'][0]['id']
                        submission_status = object_creation.json()['response']['result'][0]['status']
                    except:
                        # record error message
                        error = object_creation.json()['header']['userMessage']
                        record_message(credential_file, database, table, box, J["alias"], 'Cannot create an object: {0}'.format(error), 'Error') 
                    else:
                        # store submission json and status (DRAFT) in db table
                        record_message(credential_file, database, table, box, J["alias"], submission_status, 'Status') 
                        # validate object
                        object_validation = requests.put(portal + '{0}/{1}?action=VALIDATE'.format(ega_object, objectId), headers=headers)
                        # record status and submit object or record error message
                        try:
                            object_status = object_validation.json()['response']['result'][0]['status']
                            error_messages = clean_up_error(object_validation.json()['response']['result'][0]['validationErrorMessages'])
                        except:
                            error = object_validation.json()['header']['userMessage'] + ';' + object_validation.json()['header']['developerMessage']
                            record_message(credential_file, database, table, box, J["alias"], 'Cannot obtain validation status: {0}'.format(error), 'Error')
                        else:
                            # record error messages
                            record_message(credential_file, database, table, box, J["alias"], error_messages, 'Error')
                            # record object status
                            record_message(credential_file, database, table, box, J["alias"], object_status, 'Status') 
                            # check if object is validated
                            if object_status == 'VALIDATED':
                                # submit object
                                object_submission = requests.put(portal + '{0}/{1}?action=SUBMIT'.format(ega_object, objectId), headers=headers)
                                # update error, record status or record error if submission cannot be done
                                try:
                                    error_messages = clean_up_error(object_submission.json()['response']['result'][0]['submissionErrorMessages'])
                                    object_status = object_submission.json()['response']['result'][0]['status']                
                                except:
                                    # record error message
                                    record_message(credential_file, database, table, box, J["alias"], 'Cannot obtain submission status', 'Error')
                                else:
                                    # update error, record status
                                    record_message(credential_file, database, table, box, J["alias"], error_messages, 'Error')
                                    record_message(credential_file, database, table, box, J["alias"], object_status, 'Status') 
                                    # check status
                                    if object_status == 'SUBMITTED':
                                        # get the receipt, and the accession id
                                        try:
                                            receipt = str(object_submission.json()).replace("\"", "")
                                            # egaAccessionId is None for experiments, but can be obtained from the list of egaAccessionIds
                                            if ega_object == 'experiments':
                                                egaAccessionId = object_submission.json()['response']['result'][0]['egaAccessionIds'][0]
                                            else:
                                                egaAccessionId = object_submission.json()['response']['result'][0]['egaAccessionId']
                                        except:
                                            # record error message
                                            record_message(credential_file, database, table, box, J["alias"], 'Cannot obtain receipt and/or accession Id', 'Error')
                                        else:
                                            # store the date it was submitted
                                            current_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
                                            # add receipt, accession and time to table and change status
                                            conn = connect_to_database(credential_file, database)
                                            cur = conn.cursor()
                                            cur.execute('UPDATE {0} SET {0}.Receipt=\"{1}\", {0}.egaAccessionId=\"{2}\", {0}.Status=\"{3}\", {0}.submissionStatus=\"{3}\", {0}.CreationTime=\"{4}\" WHERE {0}.alias=\"{5}\" AND {0}.egaBox=\"{6}\"'.format(table, receipt, egaAccessionId, object_status, current_time, J["alias"], box))
                                            conn.commit()
                                            conn.close()
                                    else:
                                        # delete object
                                        requests.delete(portal + '{0}/{1}'.format(ega_object, objectId), headers=headers)
                            else:
                                #delete object
                                requests.delete(portal + '{0}/{1}'.format(ega_object, objectId), headers=headers)
                    # disconnect by removing token
                    close_api_connection(token, portal)



def extract_accessions(credential_file, database, box, table):
    '''
    (file, str, str, str) -> dict
    
    Returns a dictionary with alias: accessions pairs registered in box for the given object/Table
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - database (str): Name of the database
    - box (str): EGA box (e.g. ega-box-xxx)
    - table (str): Name of table in database
    '''
    
    # connect to metadata database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    # pull down analysis alias and egaId from metadata db, alias should be unique
    cur.execute('SELECT {0}.alias, {0}.egaAccessionId from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box)) 
    # create a dict {alias: accession}
    # some PCSI aliases are not unique, 1 sample is chosen arbitrarily
    registered = {}
    for i in cur:
        registered[i[0]] = i[1]
    conn.close()
    return registered


def map_enumerations():
    '''
    (None) -> dict
    
    Returns a dictionary of EGA Id corrresponding to enumations
    '''
    
    # map typeId with enumerations
    map_enum = {"experimentTypeId": "ExperimentTypes", "analysisTypeId": "AnalysisTypes",
               "caseOrControlId": "CaseControl", "genderId": "Genders", "datasetTypeIds": "DatasetTypes",
               "instrumentModelId": "InstrumentModels", "librarySourceId": "LibrarySources",
               "librarySelectionId": "LibrarySelections",  "libraryStrategyId": "LibraryStrategies",
               "studyTypeId": "StudyTypes", "chromosomeReferences": "ReferenceChromosomes",
               "genomeId": "ReferenceGenomes", "fileTypeId": "AnalysisFileTypes", "runFileTypeId": "FileTypes"}
    return map_enum


def get_json_keys(ega_object, action):
    '''
    (str, str) -> (list, list)
    
    Returns a tuple with lists of keys and required keys to validate or form the submission json
    
    Parameters
    ----------
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - action (str): String specifying which json keys are required for json validation or formation 
                    Accepted values: "validation" or "formation"
    '''
    
    if ega_object == 'analyses':
        keys = ['alias', 'analysisCenter', 'analysisTypeId', 'attributes', 'description',
                'experimentTypeId', 'files', 'genomeId', 'sampleReferences', 'studyId', 'title']
        required = ['StagePath', 'alias', 'analysisCenter', 'analysisTypeId',
                    'description', 'experimentTypeId', 'files', 'genomeId',
                    'sampleReferences', 'studyId', 'title']
        if action == 'validation':
            keys.extend(['egaBox', 'AttributesKey', 'ProjectKey', 'StagePath',  'Broker'])
            required.extend(['egaBox', 'AttributesKey', 'ProjectKey', 'Broker'])              
        elif action == 'formation':
            keys.extend(["analysisDate", "chromosomeReferences", "platform"])
    elif ega_object == 'samples':
        keys = ['alias', 'attributes', 'caseOrControlId', 'description',
                'genderId', 'phenotype', 'title']
        required = ['alias', 'caseOrControlId', 'description', 'genderId', 'phenotype', 'title']
        if action == 'validation':
            keys.extend(['egaBox', 'AttributesKey'])
            required.extend(['egaBox', 'AttributesKey'])
        elif action == 'formation':
            keys.extend(["organismPart", "cellLine", "region", "subjectId",
                         "anonymizedName", "bioSampleId", "sampleAge", "sampleDetail"])
    elif ega_object == 'datasets':
        keys = ['alias', 'analysisReferences', 'attributes', 'datasetLinks',
                'datasetTypeIds', 'description', 'policyId', 'runsReferences', 'title']
        required = ['alias', 'datasetTypeIds', 'description', 'egaBox', 'policyId', 'title']     
        if action == 'validation':
            keys.append('egaBox')     
    elif ega_object == 'studies':
        keys = ['alias', 'customTags', 'egaBox', 'ownTerm', 'pubMedIds', 'shortName',
                'studyAbstract', 'studyTypeId', 'title']
        required = ['alias', 'egaBox', 'studyAbstract', 'studyTypeId', 'title']
    elif ega_object == 'policies':
        keys = ['alias', 'dacId', 'egaBox', 'policyText', 'title', 'url']
        required = ['alias', 'dacId', 'egaBox', 'policyText', 'title']
    elif ega_object == 'dacs':
        keys = ['alias', 'contacts', 'egaBox', 'title']
        required = ['alias', 'contacts', 'egaBox', 'title']
    elif ega_object == 'runs':
        keys = ['alias', 'egaBox', 'experimentId', 'files', 'runFileTypeId', 'sampleId']
        required = ['alias', 'egaBox', 'experimentId', 'files', 'runFileTypeId', 'sampleId']
    elif ega_object == 'experiments':
        keys = ['alias', 'designDescription', 'egaBox', 'instrumentModelId',
                'libraryConstructionProtocol', 'libraryLayoutId', 'libraryName',
                'librarySelectionId', 'librarySourceId', 'libraryStrategyId',
                'pairedNominalLength', 'pairedNominalSdev', 'sampleId',
                'studyId', 'title']
        required = ['alias', 'designDescription', 'egaBox', 'instrumentModelId',
                    'libraryLayoutId', 'libraryName', 'librarySelectionId',
                    'librarySourceId', 'libraryStrategyId', 'pairedNominalLength',
                    'pairedNominalSdev', 'sampleId', 'studyId', 'title']
    return keys, required


def is_info_valid(credential_file, metadata_database, submission_database, table, box, ega_object, **KeyWordParams):
    '''
    (str, str, str, str, str, str, dict) -> dict
    
    Checks if the information stored in the database tables is valid and returns 
    a dictionary with a error message for each object alias
    
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - metadata_database (str): Name of the database storing EGA metadata information
    - submission_database (str): Name of the database storing information required for submission
    - table (str): Name of table in database
    - box (str): EGA submission box (ega-box-xxxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - KeyWordParams (dict): Optional table arguments. Valid key words: 'attributes' or 'projects'
    '''

    # create a dictionary {alias: error}
    D = {}

    # get the enumerations
    enumerations = list_enumerations()
    
    # connect to db
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()      
    
    # get optional tables
    if 'attributes' in KeyWordParams:
        attributes_table = KeyWordParams['attributes']
    if 'projects' in KeyWordParams:
        projects_table = KeyWordParams['projects']
    
    # get the json keys
    keys, required = get_json_keys(ega_object, 'validation')
    
    # get required information
    if ega_object == 'analyses':
        # extract information from Analyses, atrtibutes and projects tables
        cmd = 'SELECT {0}.alias, {0}.sampleReferences, {0}.files, {0}.egaBox, \
            {0}.AttributesKey, {0}.ProjectKey, {1}.title, {1}.description, {1}.attributes, \
            {1}.genomeId, {1}.StagePath, {2}.studyId, {2}.analysisCenter, {2}.Broker, \
            {2}.analysisTypeId, {2}.experimentTypeId FROM {0} JOIN {1} JOIN {2} \
            WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{3}\" AND {0}.AttributesKey={1}.alias \
            AND {0}.ProjectKey={2}.alias'.format(table, attributes_table, projects_table, box)
    elif ega_object == 'samples':
        cmd = 'Select {0}.alias, {0}.caseOrControlId, {0}.genderId, {0}.phenotype, {0}.egaBox, \
              {0}.AttributesKey, {1}.title, {1}.description, {1}.attributes FROM {0} JOIN {1} WHERE \
              {0}.Status=\"start\" AND {0}.egaBox=\"{2}\" AND {0}.AttributesKey={1}.alias'.format(table, attributes_table, box)
    elif ega_object == 'datasets':
        cmd = 'SELECT {0}.alias, {0}.datasetTypeIds, {0}.policyId, {0}.runsReferences, {0}.analysisReferences, \
        {0}.title, {0}.description, {0}.datasetLinks, {0}.attributes , {0}.egaBox FROM {0} WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(table, box)            
    elif ega_object == 'experiments':
        cmd  = 'SELECT {0}.alias, {0}.title, {0}.instrumentModelId, {0}.librarySourceId, \
        {0}.librarySelectionId, {0}.libraryStrategyId, {0}.designDescription, {0}.libraryName, \
        {0}.libraryConstructionProtocol, {0}.libraryLayoutId, {0}.pairedNominalLength, \
        {0}.pairedNominalSdev, {0}.sampleId, {0}.studyId, {0}.egaBox FROM {0} WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'studies':
        cmd = 'SELECT {0}.alias, {0}.studyTypeId, {0}.shortName, {0}.title, \
        {0}.studyAbstract, {0}.ownTerm, {0}.pubMedIds, {0}.customTags, {0}.egaBox FROM {0} \
        WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'policies':
        cmd = 'SELECT {0}.alias, {0}.dacId, {0}.title, {0}.policyText, {0}.url, {0}.egaBox FROM {0} \
        WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'dacs':
        cmd = 'SELECT {0}.alias, {0}.title, {0}.contacts, {0}.egaBox FROM {0} WHERE {0}.status=\"start\" AND {0}.egaBox="\{1}\"'.format(table, box)
    elif ega_object == 'runs':
        cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.runFileTypeId, {0}.experimentId, \
        {0}.files, {0}.egaBox FROM {0} WHERE {0}.status=\"start\" AND {0}.egaBox=\"{1}\"'.format(table, box)
        

    # extract data 
    try:
        cur.execute(cmd)
        data = cur.fetchall()
    except:
        data = []
    conn.close()
    
    # map typeId with enumerations
    map_enum = map_enumerations()

    # check info
    if len(data) != 0:
        for i in range(len(data)):
            # set up boolean. update if missing values
            missing = False
            # create a dict with all information
            d = {keys[j]: data[i][j] for j in range(len(keys))}
            # create an error message
            error = []
            # check if information is valid
            for key in keys:
                if key in required:
                    if d[key] in ['', 'NULL', None]:
                        missing = True
                        error.append('Missing required key {0}'.format(key))
                # check that alias is not already used
                if key == 'alias':
                    # extract alias and accessions from table
                    registered = extract_accessions(credential_file, metadata_database, box, table)
                    if d[key] in registered:
                        # alias already used for the same table and box
                        missing = True
                        error.append('Alias already registered')
                    if ega_object in ['runs', 'analyses'] and '__' in d[key]:
                        # double underscore is not allowed in runs and analyses alias
                        # because alias and file name are retrieved from job name
                        # split on double underscore for checking upload and encryption
                        missing = True
                        error.append('Double underscore not allowed in runs and analyses alias')
                # check that references are provided for datasets
                if 'runsReferences' in d and 'analysisReferences' in d:
                    # at least runsReferences or analysesReferences should include some accessions
                    if d['runsReferences'] in ['', 'NULL', None] and d['analysisReferences'] in ['', 'NULL', None]:
                        missing = True
                        error.append('Missing runsReferences and analysisReferences')
                    if d['runsReferences'] not in ['', 'NULL', None]:
                        if False in list(map(lambda x: x.startswith('EGAR'), d['runsReferences'].split(';'))):
                            missing = True
                            error.append('Missing runsReferences')
                    if d['analysisReferences'] not in ['', 'NULL', None]:
                        if False in list(map(lambda x: x.startswith('EGAZ'), d['analysisReferences'].split(';'))):
                            missing = True
                            error.append('Missing analysisReferences')
                # check that accessions or aliases are provided
                if key in ['sampleId', 'sampleReferences', 'dacId', 'studyId']:
                    if d[key] in ['', 'None', None, 'NULL']:
                        missing = True
                        error.append('Missing alias and or accession for {0}'.format(key))
                # check files
                if key == 'files':
                    files = json.loads(d['files'].replace("'", "\""))
                    for file_path in files:
                        # check if file is valid
                        if os.path.isfile(file_path) == False:
                            missing = True
                            error.append('Invalid file paths')
                        # check validity of file type for Analyses objects only. doesn't exist for Runs
                        if ega_object == 'Analyses':
                            if files[file_path]['fileTypeId'].lower() not in enumerations['FileTypes']:
                                missing = True
                                error.append('Invalid fileTypeId')
                # check policy Id
                if key == 'policyId':
                    if 'EGAP' not in d[key]:
                        missing = True
                        error.append('Invalid policyId, should start with EGAP')
                # check library layout
                if key == "libraryLayoutId":
                    if str(d[key]) not in ['0', '1']:
                        missing = True
                        error.append('Invalid {0}: should be 0 or 1'.format(key))
                if key in ['pairedNominalLength', 'pairedNominalSdev']:
                    try:
                        float(d[key])
                    except:
                        missing = True
                        error.append('Invalid type for {0}, should be a number'.format(key))
                # check enumerations
                if key in map_enum:
                    # datasetTypeIds can be a list of multiple Ids
                    if key == 'datasetTypeIds':
                        for k in d[key].split(';'):
                            if k not in enumerations[map_enum[key]]:
                                missing = True
                                error.append('Invalid enumeration for {0}'.format(key))
                    # check that enumeration is valid
                    if d[key] not in enumerations[map_enum[key]]:
                        missing = True
                        error.append('Invalid enumeration for {0}'.format(key))
                # check custom attributes
                if key == 'attributes':
                    if d['attributes'] not in ['', 'NULL', None]:
                        # check format of attributes
                        attributes = [json.loads(j.replace("'", "\"")) for j in d['attributes'].split(';')]
                        for k in attributes:
                            # do not allow keys other than tag, unit and value
                            if set(k.keys()).union({'tag', 'value', 'unit'}) != {'tag', 'value', 'unit'}:
                                missing = True
                                error.append('Invalid {0} format'.format(key))
                            # tag and value are required keys
                            if 'tag' not in k.keys() and 'value' not in k.keys():
                                missing = True
                                error.append('Missing tag and value from {0}'.format(key))

            # check if object has missing/non-valid information
            if missing == True:
                 error = ';'.join(list(set(error)))
            elif missing == False:
                error = 'NoError'
            assert d['alias'] not in D
            D[d['alias']] = error
    
    return D

     
def check_table_information(credential_file, metadata_database, submission_database, table, ega_object, box, **KeyWordParams):
    '''
    (str, str, str, str, str, str, dict) -> None

    Checks that information in the submission database is valid and updates
    the errorMessages column for each alias in table
    
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - metadata_database (str): Name of the database storing EGA metadata information
    - submission_database (str): Name of the database storing information required for submission
    - table (str): Name of table in database
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - box (str): EGA submission box (ega-box-xxxx)
    - KeyWordParams (dict): Optional table arguments. Valid key words: 'attributes' or 'projects'
    '''

    # check Table information
    D = is_info_valid(credential_file, metadata_database, submission_database, table, box, ega_object, **KeyWordParams)
      
    # extract all aliases with start status
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()      
    try:
        cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(table, box))
        data = [i[0] for i in cur]
    except:
        data = []
    conn.close()
    
    # create dict {alias: errors}
    K = {}
    # record error messages
    for alias in data:
        if alias in D:
            K[alias] = D[alias]
        else:
            K[alias] = 'No information. Possible issues with table keys or database connection'
    # update status and record errorMessage
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()    
    for alias in K:
        # record error message and/or update status
        if K[alias] == 'NoError':
            # update status
            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"clean\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, K[alias], alias, box))
            conn.commit()
        else:
            # record error
            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, K[alias], alias, box))
            conn.commit()
    conn.close()



def is_gzipped(file):
    '''
    (str) -> bool
    
    Returns True if file is gzipped
    
    Parameters
    ----------
    - file (str): Path to file
    '''
    
    # open file in rb mode
    infile = open(file, 'rb')
    header = infile.readline()
    infile.close()
    if header.startswith(b'\x1f\x8b\x08'):
        return True
    else:
        return False


def open_file(file):
    '''
    (str) -> _io.TextIOWrapper
    
    Returns a file opened in text mode if file is gzipped or not
    
    Parameters
    ----------
    - file (str): Path to file gzipped or not
    '''
    
    # check if vcf if gzipped
    if is_gzipped(file):
        infile = gzip.open(file, 'rt')
    else:
        infile = open(file)
    return infile


def extract_contigs_from_vcf(file):
    '''
    (str) -> list

    Returns a list of contigs found in the vcf header or body if header is missing

    Parameters
    ----------
    - file (str): Path to vcf file    
    '''
    
    contigs = []
    infile = open_file(file)
    
    # read vcf header  
    for line in infile:
        if line.startswith('##contig'):
            contig = line.split(',')[0].split('=')[-1]
            if '_' in contig:
                contig = contig[:contig.index('_')]
            if not contig.lower().startswith('chr'):
                contig = 'chr' + contig
            contigs.append(contig)
        elif not line.startswith('#'):
            line = line.rstrip().split('\t')
            if '_' in line[0]:
                contig = line[0][:line[0].index('_')]
            if not contig.lower().startswith('chr'):
                contig = 'chr' + contig
            contigs.append(contig)
    infile.close()

    # remove duplicate names
    contigs = list(set(contigs))
    return contigs        
    

def extract_contigs_from_tsv(file):
    '''
    (str) -> list
    
    Returns a list of contigs found in a TSV file (compressed or not)
    
    Parameters
    ----------
    - file (str): Path to TSV file, gzipped or not
    '''
    
    infile = open_file(file)
         
    # get the chromosomes
    chromos = []
    # read file
    for line in infile:
        line = line.rstrip()
        if line != '':
            line = line.split('\t')
            line = list(map(lambda x: x.strip(), line))
            contig = line[0]
            if 'chr' not in contig.lower():
                contig = 'chr' + contig
            else:
                contig = contig.lower()
            chromos.append(contig)
    infile.close()
    chromos = list(set(chromos))
    return chromos


def map_chromo_names():
    '''
    (None) -> dict
    
    Returns a dictionary of chromosomes, names pair, values
    '''
    
    chromo_to_names = {'chr1': 'CM000663', 'chr2': 'CM000664', 'chr3': 'CM000665',
                   'chr4': 'CM000666', 'chr5': 'CM000667', 'chr6': 'CM000668',
                   'chr7': 'CM000669', 'chr8': 'CM000670', 'chr9': 'CM000671',
                   'chr10': 'CM000672', 'chr11': 'CM000673', 'chr12': 'CM000674',
                   'chr13': 'CM000675', 'chr14': 'CM000676', 'chr15': 'CM000677',
                   'chr16': 'CM000678', 'chr17': 'CM000679', 'chr18': 'CM000680',
                   'chr19': 'CM000681', 'chr20': 'CM000682', 'chr21': 'CM000683',
                   'chr22': 'CM000684', 'chrX': 'CM000685', 'chrY': 'CM000686'}
    return chromo_to_names


def format_json(D, ega_object):
    '''
    (dict, str) -> dict
    
    Returns a dictionary with information in the expected submission format or
    a dictionary with the object alias if required fields are missing
    Precondition: strings in D have double-quotes
    
    Parameters
    ----------
    - D (dict): Dictionary with information extracted from the submission database for a given EGA object
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    '''
    # get the EGA enumerations
    enumerations = list_enumerations()
        
    # create a dict to be strored as a json. note: strings should have double quotes
    J = {}
    
    # get json keys
    JsonKeys, required  = get_json_keys(ega_object, 'formation')

    # map typeId with enumerations
    map_enum = map_enumerations()

    # map chromosome names for vcf
    chromo_to_names = map_chromo_names()
                      
    # reverse dictionary
    names_to_chromo = {}
    for i in chromo_to_names:
        names_to_chromo[chromo_to_names[i]] = i

    # loop over required json keys
    for field in JsonKeys:
        if field in D:
            if D[field] in ['NULL', '', None]:
                # some fields are required, return empty dict if field is empty
                if field in required:
                    # erase dict and add alias
                    J = {}
                    J["alias"] = D["alias"]
                    # return dict with alias only if required fields are missing
                    return J
                # other fields can be missing, either as empty list or string
                else:
                    # check if field is already recorded. eg: chromosomeReferences may be set already for vcf
                    if field not in J:
                        # some non-required fields need to be lists
                        if field in ["chromosomeReferences", "attributes", "datasetLinks", "runsReferences",
                                     "analysisReferences", "pubMedIds", "customTags"]:
                            J[field] = []
                        else:
                            J[field] = ""
            else:
                if field == 'files':
                    assert D[field] != 'NULL'
                    J[field] = []
                    # convert string to dict
                    files = D[field].replace("'", "\"")
                    files = json.loads(files)
                    # file format is different for analyses and runs
                    if ega_object == 'analyses':
                        # make a list of contigs used. required for vcf, optional for bam
                        contigs = []
                        # loop over file name
                        for file_path in files:
                            # create a dict to store file info
                            # check that fileTypeId is valid
                            if files[file_path]["fileTypeId"].lower() not in enumerations[map_enum['fileTypeId']]:
                                # cannot obtain fileTypeId. erase dict and add alias
                                J = {}
                                J["alias"] = D["alias"]
                                # return dict with alias only if required fields are missing
                                return J
                            else:
                                file_typeId = enumerations[map_enum['fileTypeId']][files[file_path]["fileTypeId"].lower()]
                            # check if analysis object is bam or vcf
                            # chromosomeReferences is optional for bam but required for vcf and tab
                            if files[file_path]["fileTypeId"].lower() == 'vcf':
                                # make a list of contigs found in the vcf header or body
                                contigs.extend(extract_contigs_from_vcf(file_path))
                            elif files[file_path]["fileTypeId"].lower() == 'tab':
                                # make a list of contigs 
                                contigs.extend(extract_contigs_from_tsv(file_path))
                            # create dict with file info, add path to file names
                            d = {"fileName": os.path.join(D['StagePath'], files[file_path]['encryptedName']),
                                 "checksum": files[file_path]['checksum'],
                                 "unencryptedChecksum": files[file_path]['unencryptedChecksum'],
                                 "fileTypeId": file_typeId}
                            J[field].append(d)
                    
                        # check if chromosomes were recorded
                        if len(contigs) != 0:
                            # remove duplicate names
                            contigs = list(set(contigs))
                            # map chromosome names 
                            if 'genomeId' not in D:
                                # erase dict and add alias
                                J = {}
                                J["alias"] = D["alias"]
                                return J
                            else:
                                # only GRCh37 and GRch38 are supported
                                if D['genomeId'].lower() not in ['grch37', 'grch38']:
                                    # erase dict and add alias
                                    J = {}
                                    J["alias"] = D["alias"]
                                    return J
                                else:
                                    if D['genomeId'].lower() == 'grch37':
                                        suffix = '.1'
                                    elif D['genomeId'].lower() == 'grch38':
                                        suffix = '.2'
                                    values = [chromo_to_names[i] + suffix for i in contigs if i in chromo_to_names]
                                    # add chromosome reference info
                                    J['chromosomeReferences'] = [{"value": enumerations['ReferenceChromosomes'][i], "label": names_to_chromo[i.replace(suffix, '')]} for i in values if i in enumerations['ReferenceChromosomes']]  
                    elif ega_object == 'runs':
                        # loop over file name
                        for file_path in files:
                            # create a dict with file info, add stagepath to file name
                            d = {"fileName": os.path.join(D['StagePath'], files[file_path]['encryptedName']),
                                 "checksum": files[file_path]['checksum'], "unencryptedChecksum": files[file_path]['unencryptedChecksum'],
                                 "checksumMethod": 'md5'}
                            J[field].append(d)
                elif field in ['runsReferences', 'analysisReferences', 'pubMedIds']:
                    J[field] = D[field].split(';')
                elif field in ['attributes', 'datasetLinks', 'customTags']:
                    # ensure strings are double-quoted
                    attributes = D[field].replace("'", "\"")
                    # convert string to dict
                    # loop over all attributes
                    attributes = attributes.split(';')
                    J[field] = [json.loads(attributes[i].strip().replace("'", "\"")) for i in range(len(attributes))]
                elif field == 'libraryLayoutId':
                    try:
                        int(D[field]) in [0, 1]
                        J[field] = int(D[field])
                    except:
                        # must be coded 0 for paired end or 1 for single end
                        J = {}
                        # return dict with alias if required field is missing
                        J["alias"] = D["alias"]
                        return J
                elif field  in ['pairedNominalLength', 'pairedNominalSdev']:
                    try:
                        float(D[field])
                        J[field] = float(D[field])
                    except:
                        # must be coded 0 for paired end or 1 for single end
                        J = {}
                        # return dict with alias if required field is missing
                        J["alias"] = D["alias"]
                        return J        
                # check enumerations
                elif field in map_enum:
                    # check that enumeration is valid
                    if D[field] not in enumerations[map_enum[field]]:
                        # cannot obtain enumeration. erase dict and add alias
                        J = {}
                        J["alias"] = D["alias"]
                        # return dict with alias only if required fields are missing
                        return J
                    else:
                        # check field to add enumeration to json
                        if field == "experimentTypeId":
                            J[field] = [enumerations[map_enum[field]][D[field]]]
                        elif field == "datasetTypeIds":
                            # multiple Ids can be stored
                            J[field] = [enumerations[map_enum[field]][k] for k in D[field].split(';')]
                        else:
                            J[field] = enumerations[map_enum[field]][D[field]]
                elif field == 'sampleReferences':
                    # populate with sample accessions
                    J[field] = [{"value": accession.strip(), "label":""} for accession in D[field].split(';')]
                elif field == 'contacts':
                    J[field] = [json.loads(contact.replace("'", "\"")) for contact in D[field].split(';')]
                
                # fields added as aliases must be replaced with accessions
                elif field in ['studyId', 'policyId', 'dacId', 'experimentId']:
                    a = ['studyId', 'policyId', 'dacId', 'experimentId']
                    b = ['EGAS', 'EGAP', 'EGAC', 'EGAX']
                    for i in range(len(a)):
                        if field == a[i]:
                            if D[field].startswith(b[i]):
                                J[field] = D[field]
                            else:
                                # erase dict and add alias
                                J = {}
                                J["alias"] = D["alias"]
                                return J
                else:
                    J[field] = D[field]
    return J                



def add_json_to_table(credential_file, database, table, box, ega_object, **KeyWordParams):
    '''
    (str, str, str, str, str, dict) -> None
    
    Forms a json with required information for registering the EGA object in the 
    given box and adds the json to the table in database and updates status if the
    json is formed correctly
    
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - database (str): Name of the submission database 
    - table (str): Table in database storing information about ega_object
    - box (str): EGA submission box (ega-box-xxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - KeyWordParams (dict): Optional table arguments. Valid key words: 'attributes' or 'projects'
    '''
    
    # connect to the database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    
    # get optional tables
    if 'projects' in KeyWordParams:
        projects_table = KeyWordParams['projects']
    else:
        projects_table = 'empty'
    if 'attributes' in KeyWordParams:
        attributes_table = KeyWordParams['attributes']
    else:
        attributes_table = 'empty'
    
    # command depends on Object type    
    if ega_object == 'analyses':
        Cmd = 'SELECT {0}.alias, {0}.sampleReferences, {0}.analysisDate, {0}.files, \
        {1}.title, {1}.description, {1}.attributes, {1}.genomeId, {1}.chromosomeReferences, {1}.StagePath, {1}.platform, \
        {2}.studyId, {2}.analysisCenter, {2}.Broker, {2}.analysisTypeId, {2}.experimentTypeId \
        FROM {0} JOIN {1} JOIN {2} WHERE {0}.Status=\"uploaded\" AND {0}.egaBox=\"{3}\" AND {0}.AttributesKey = {1}.alias \
        AND {0}.ProjectKey = {2}.alias'.format(table, attributes_table, projects_table, box)
    elif ega_object == 'samples':
        Cmd = 'SELECT {0}.alias, {0}.caseOrControlId, {0}.genderId, {0}.organismPart, \
        {0}.cellLine, {0}.region, {0}.phenotype, {0}.subjectId, {0}.anonymizedName, {0}.bioSampleId, \
        {0}.sampleAge, {0}.sampleDetail, {1}.title, {1}.description, {1}.attributes FROM {0} JOIN {1} \
        WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{2}\" AND {0}.AttributesKey = {1}.alias'.format(table, attributes_table, box)
    elif ega_object == 'datasets':
        Cmd = 'SELECT {0}.alias, {0}.datasetTypeIds, {0}.policyId, {0}.runsReferences, \
        {0}.analysisReferences, {0}.title, {0}.description, {0}.datasetLinks, {0}.attributes FROM {0} \
        WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'experiments':
        Cmd  = 'SELECT {0}.alias, {0}.title, {0}.instrumentModelId, {0}.librarySourceId, \
        {0}.librarySelectionId, {0}.libraryStrategyId, {0}.designDescription, {0}.libraryName, \
        {0}.libraryConstructionProtocol, {0}.libraryLayoutId, {0}.pairedNominalLength, \
        {0}.pairedNominalSdev, {0}.sampleId, {0}.studyId FROM {0} WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'studies':
        Cmd = 'SELECT {0}.alias, {0}.studyTypeId, {0}.shortName, {0}.title, {0}.studyAbstract, \
        {0}.ownTerm, {0}.pubMedIds, {0}.customTags FROM {0} WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{1}\"'.format(table, box) 
    elif ega_object == 'policies':
        Cmd = 'SELECT {0}.alias, {0}.dacId, {0}.title, {0}.policyText, {0}.url FROM {0} \
        WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'dacs':
        Cmd = 'SELECT {0}.alias, {0}.title, {0}.contacts FROM {0} WHERE {0}.status=\"clean\" AND {0}.egaBox="\{1}\"'.format(table, box)
    elif ega_object == 'runs':
        Cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.runFileTypeId, {0}.experimentId, {0}.files, \
        {0}.WorkingDirectory, {0}.StagePath FROM {0} WHERE {0}.Status=\"uploaded\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    
          
    # extract information to form json    
    try:
        cur.execute(Cmd)
        # get column headers
        header = [i[0] for i in cur.description]
        # extract all information 
        data = cur.fetchall()
    except:
        data = []
        
    # check that object are with appropriate status and/or that information can be extracted
    if len(data) != 0:
        # create a list of dicts storing the object info
        L = []
        for i in data:
            D = {}
            assert len(i) == len(header)
            for j in range(len(i)):
                D[header[j]] = i[j]
            L.append(D)
        # create object-formatted jsons from each dict 
        Jsons = [format_json(D, ega_object) for D in L]
        # add json back to table and update status
        for D in Jsons:
            # check if json is correctly formed (ie. required fields are present)
            if len(D) == 1:
                error = 'Cannot form json, required field(s) missing'
                # add error in table and keep status (uploaded --> uploaded for analyses and valid --> valid for samples)
                alias = D['alias']
                cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box))
                conn.commit()
            else:
                # add json back in table and update status
                alias = D['alias']
                cur.execute('UPDATE {0} SET {0}.Json=\"{1}\", {0}.errorMessages=\"None\", {0}.Status=\"submit\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\";'.format(table, str(D), alias, box))
                conn.commit()
    conn.close()


def get_job_exit_status(job_name):
    '''
    (str) -> str
    
    Returns the exit code of a job named job_name after it finished running 
    ('0' indicates a normal, error-free run and '1' or another value inicates an error)
    
    Parameters
    ----------
    - job_name (str): Name of the job run on cluster
    '''
    
    # make a sorted list of accounting files with job info archives
    archives = subprocess.check_output('ls -lt /oicr/cluster/uge-8.6/default/common/accounting*', shell=True).decode('utf-8').rstrip().split('\n')
    # keep accounting files for the current year
    archives = [archives[i].split()[-1] for i in range(len(archives)) if ':' in archives[i].split()[-2]]
    
    # loop over the most recent archives and stop when job is found    
    for accounting_file in archives:
        try:
            i = subprocess.check_output('qacct -j {0} -f {1}'.format(job_name, accounting_file), shell=True).decode('utf-8').rstrip().split('\n')
        except:
            i = ''
        else:
            if i != '':
                break
            
    # check if accounting file with job has been found
    if i == '':
        # return error
        return '1'        
    else:
        # record all exit status. the same job may have been run multiple times if re-encryption was needed
        d = {}
        for j in i:
            if 'end_time' in j:
                k = j.split()[1:]
                if len(k) != 0:
                    # convert date to epoch time
                    if '.' in k[1]:
                        k[1] = k[1][:k[1].index('.')]
                    date = '.'.join([k[0].split('/')[1], k[0].split('/')[0], k[0].split('/')[-1]]) + ' ' + k[1]
                    p = '%d.%m.%Y %H:%M:%S'
                    date = int(time.mktime(time.strptime(date, p)))
                else:
                    date = 0
            elif 'exit_status' in j:
                d[date] = j.split()[1]
        # get the exit status of the most recent job    
        end_jobs = list(d.keys())
        end_jobs.sort()
        if len(d) != 0:
            # return exit code
            return d[end_jobs[-1]]
        else:
            # return error
            return '1'
    

def get_subdirectories(user_name, password, directory):
    '''
    (str, str, str) -> list
    
    Returns a list of sub-directories in directory in the box' staging server
    
    Parameters
    ----------
    - user_name (str): EGA submission box
    - password (str): Password for the EGA submission box
    - directory (str): Directory on the box' staging server
    '''
    
    # make a list of directories on the staging servers
    ftp = FTP(host='ftp.ega.ebi.ac.uk', user = user_name, passwd=password)
    a = []
    ftp.cwd(directory)
    ftp.dir(a.append)
    content = [directory + '/' + i.split()[-1] for i in a if i.startswith('d')]
    return content

    
def list_directories_staging_server(credential_file, box):
    '''
    (str, str) -> list
    
    Returns a list of all directories on the staging server of the given box
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # get box credentials
    credentials = extract_credentials(credential_file)
    # exclude EGA-owned directories
    exclude = ['MD5_daily_reports', 'metadata']
    # make a list of directories on the staging server 
    a = get_subdirectories(box, credentials[box], '')
    # dump all sub-directories into the collecting list
    # skip EGA-owned directories
    b = [i for i in a if i not in exclude]
    # add home directory
    b.append('')
    # make a list of directories already traversed
    checked = ['']
    # initialize list with list length so that first iteration always occurs
    L = [0, len(b)]
    while L[-1] != L[-2]:
        for i in b:
            # ignore if already checked and ignore EGA-owned directories
            if i not in checked and i not in exclude:
                b.extend(get_subdirectories(box, credentials[box], i))
                checked.append(i)
        # update L
        L.append(len(b))
    return b


def extract_file_size_staging_server(credential_file, box, directory):
    '''
    (str, str, str) -> dict
    
    Returns a dictionary with file size for all files in directory
    
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - box (str): EGA submission box (ega-box-xxxx)
    - directory (str): Directory on the box' staging server
    '''
    
    # get credentials
    credentials = extract_credentials(credential_file)
        
    # make a list of files under directory on the staging server
    # connect to the ega box
    ftp = FTP(host='ftp.ega.ebi.ac.uk', user = box, passwd=credentials[box])
    # navigate to directory
    ftp.cwd(directory)
    # list directory's content
    content = []
    ftp.retrlines('LIST', content.append)
    # grab the files in directory
    files = [i for i in content if i.startswith('-')]
    # extract file size for all files {filepath: file_size}
    size = {}
    for S in files:
        S = S.rstrip().split()
        file_size = int(S[4])
        file_path = directory + '/' + S[-1]
        size[file_path] = file_size
    return size


def map_files_to_checksum(credential_file, database, table, box):
    '''    
    (str, str, str, str) -> dict
    
    Returns a dictionary with file path, list of md5sums and accession ID key, value pairs
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): name of the database
    - table (str): Table in database
    - box (str): EGA submission box (eg. ega-box-xxxx)
    '''
   
    # connect to db
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    # extract alias, xml, accession number    
    try:
       cur.execute('SELECT {0}.alias, {0}.xml, {0}.egaAccessionId FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
       data = cur.fetchall()
    except:
        data = []
    conn.close()
   
    # create a dict {filepath: [[md5unc, md5enc, accession, alias]]}    
    files = {}  
    if len(data)!= 0:
        for i in data:
            # parse the xml, extract filenames and md5sums
            alias = i[0]
            tree = ET.ElementTree(ET.fromstring(i[1]))
            accession = i[2]
            j = tree.findall('.//FILE')
            for i in range(len(j)):
                filename = j[i].attrib['filename']
                md5unc = j[i].attrib['unencrypted_checksum']
                md5enc = j[i].attrib['checksum']
                if filename in files:
                    files[filename].append([md5unc, md5enc, alias, accession])
                else:
                    files[filename] = [[md5unc, md5enc, alias, accession]]
    return files 


# use this function to get file size and metadata for all files on the staging server in a Specific box box
def merge_file_info_staging_server(file_size, registered_files, box):
    '''
    (dict, dict, str) - > dict
    
    Returns a dictionary of file information that include file size and metadata
    for the files on the box's staging server 
        
    Parameters
    ----------
    
    - file_size (dict): Dictionary with file size for all files on the box's staging server
    - registered_files (dict): Dictionary with registered files in the box 
    - box (str): EGA submission box (ega-box-xxxx)
    '''
    
    D = {}
    for file_path in file_size:
        # get the name of the file. check if file on the root on the staging server
        if os.path.basename(file_path) == '':
            file_name = file_path
        else:
            file_name = os.path.basename(file_path)
        # check if file is md5
        if file_path[-4:] == '.md5':
            name = file_path[:-4]
        else:
            name = file_path
        # add filename, size and initialize empty lists to store alias and accessions
        if file_path not in D:
            D[file_path] = [file_path, file_name, str(file_size[file_path]), [], [], box]
        # check if file is registered
        # file may or may not have .gpg extension in RegisteredFiles
        # .gpg present upon registration but subsenquently removed from file name
        # add aliases and acessions
        if name in registered_files:
            for i in range(len(registered_files[name])):
                D[file_path][3].append(registered_files[name][i][-2])
                D[file_path][4].append(registered_files[name][i][-1])
        elif name[-4:] == '.gpg' and name[:-4] in registered_files:
            for i in range(len(registered_files[name[:-4]])):
                D[file_path][3].append(registered_files[name[:-4]][i][-2])
                D[file_path][4].append(registered_files[name[:-4]][i][-1])
        elif name[-4:] != '.gpg' and name + '.gpg' in registered_files:
            for i in range(len(registered_files[name + '.gpg'])):
                D[file_path][3].append(registered_files[name + '.gpg'][i][-2])
                D[file_path][4].append(registered_files[name + '.gpg'][i][-1])
        else:
            D[file_path][3].append('NULL')
            D[file_path][4].append('NULL')
        # convert lists to strings
        # check if multiple aliases and accessions exist for that file
        if len(D[file_path][3]) > 1:
            D[file_path][3] = ';'.join(D[file_path][3])
        else:
            D[file_path][3] = D[file_path][3][0]
        if len(D[file_path][4]) > 1:
            D[file_path][4]= ';'.join(D[file_path][4])
        else:
            D[file_path][4] = D[file_path][4][0]
    return D


def add_file_info_staging_server(credential_file, metadata_database, submission_database, analysis_table, runs_table, staging_server_table, box):
    '''
    (str, str, str, str, str, str, str) -> None
    
    Populates table StagingServerTable in the submission database with file information
    including size and accession IDs for files on the box's staging server
        
    Paramaters
    ----------
    - credential_file (str): File with EGA box and database credentials 
    - metadata_database (str): Database storing metadata information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - analysis_table (str): Table storing analysis information
    - runs_table (str): Table storing run information 
    - staging_server_table (str): Table storing file information on the EGA staging server
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # list all directories on the staging server of box
    directories = list_directories_staging_server(credential_file, box)
    # Extract file size for all files on the staging server
    file_size = [extract_file_size_staging_server(credential_file, box, i) for i in directories]
    # Extract md5sums and accessions from the metadata database
    registered_analyses = map_files_to_checksum(credential_file, metadata_database, analysis_table, box)
    registered_runs = map_files_to_checksum(credential_file, metadata_database, runs_table, box)
        
    # merge registered files for each box
    registered = {}
    for filename in registered_analyses:
        registered[filename] = registered_analyses[filename]
        if filename in registered_runs:
            registered[filename].append(registered_runs[filename])
    for filename in registered_runs:
        if filename not in registered:
            registered[filename] = registered_runs[filename]
                    
    # cross-reference dictionaries and get aliases and accessions for files on staging servers if registered
    data = [merge_file_info_staging_server(D, registered, box) for D in file_size]
                
    # format colums with datatype and convert to string
    fields = ["file", "filename", "fileSize", "alias", "egaAccessionId", "egaBox"]
    columns = ' '.join([fields[i] + ' TEXT NULL,' if i != len(fields) -1 else fields[i] + ' TEXT NULL' for i in range(len(fields))])
    # create a string with column headers
    column_names = ', '.join(fields)

    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    if staging_server_table not in tables:
        # connect to submission database
        conn = connect_to_database(credential_file, submission_database)
        cur = conn.cursor()
        # format colums with datatype and convert to string
        cur.execute('CREATE TABLE {0} ({1})'.format(staging_server_table, columns))
        conn.commit()
        conn.close()
    
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    # drop all entries for that Box
    cur.execute('DELETE FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(staging_server_table, box))
    conn.commit()
        
    # loop over dicts
    for i in range(len(data)):
        # list values according to the table column order
        for filename in data[i]:
            # convert data to strings, converting missing values to NULL
            values =  format_data(data[i][filename])
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(staging_server_table, column_names, values))
            conn.commit()
    conn.close()            


def add_footprint_data(credential_file, submission_database, staging_server_table, footprint_table, box):
    '''
    (str, str, str, str, str) -> None
    Use credentials to connect to SubDatabase, extract file information from
    StagingServerTable for given Box and collapse it per directory in FootPrintTable
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - submission_database (str): Name of the database storing information required for registing EGA objects
    - staging_server_table (str): Table in the submission database storing information about files located on the EGA's staging server
    - footprint_table (str): Table in the submission database storing foot print on each of the boxes' staging servers
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    try:
        cur.execute('SELECT * FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(staging_server_table, box))
        data = cur.fetchall()
    except:
        data = []
    conn.close()
        
    size = {}
    if len(data) != 0:
        for i in data:
            filesize = int(i[2])
            filename = i[0]
            directory = os.path.dirname(filename)
            box = i[-1]
            if directory == '':
                directory = '/'
            if i[4] not in ['NULL', '', None]:
                registered = True
            else:
                registered = False
            if box not in size:
                size[box] = {}
            if directory not in size[box]:
                size[box][directory] = [directory, 1, 0, 0, filesize, 0, 0]
            else:
                size[box][directory][4] += filesize
                size[box][directory][1] += 1
            if registered  == True:
                size[box][directory][2] += 1
                size[box][directory][5] += filesize
            else:
                 size[box][directory][3] += 1
                 size[box][directory][6] += filesize
        
        # compute size for all files per box
        for i in data:
            box = i[-1]
            size[box]['All'] = ['All', 0, 0, 0, 0, 0, 0]
        for box in size:
            for directory in size[box]:
                if directory != 'All':
                    cumulative = [x + y for x, y in list(zip(size[box]['All'], size[box][directory]))]
                    size[box]['All'] = cumulative
            # correct directory value
            size[box]['All'][0] = 'All'
        
        # list all tables in EGA metadata db
        tables = show_tables(credential_file, submission_database)

        # connect to submission database
        conn = connect_to_database(credential_file, submission_database)
        cur = conn.cursor()
  
        # create table if doesn't exist        
        if footprint_table not in tables:
            fields = ["egaBox", "location", "AllFiles", "Registered", "NotRegistered", "Size", "SizeRegistered", "SizeNotRegistered"]
            # format colums with datatype - convert to string
            columns = ' '.join([fields[i] + ' TEXT NULL,' if i != len(fields) -1 else fields[i] + ' TEXT NULL' for i in range(len(fields))])
            # create table with column headers
            cur.execute('CREATE TABLE {0} ({1})'.format(footprint_table, columns))
            conn.commit()
        else:
            # get the column headers from the table
            cur.execute("SELECT * FROM {0}".format(footprint_table))
            fields = [i[0] for i in cur.description]
            
        # create a string with column headers
        column_names = ', '.join(fields)
        # drop all entries for that Box
        cur.execute('DELETE FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(footprint_table, box))
        conn.commit()
    
        # loop over data in boxes
        for box in size:
            # loop over directory in each box
            for directory in size[box]:
                # add box to list of data
                L = [box]
                L.extend(size[box][directory])
                # list values according to the table column order
                # convert data to strings, converting missing values to NULL
                values =  format_data(list(map(lambda x: str(x), L)))
                cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(footprint_table, column_names, values))
                conn.commit()
        conn.close()            
                

def get_disk_space_staging_server(credential_file, database, footprint_table, box):
    '''
    (str, str, str, str) -> float
    
    Returns the footprint of non-registered files on the staging server of a given box (in Tb)
        
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - database (str): Database storing information for registration of EGA objects
    - footprint_table (str): Table in database storing information about files on the box' staging server 
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    
    # make a list of boxes in foorptint table
    cur.execute('SELECT {0}.egaBox FROM {0}'.format(footprint_table))
    boxes = list(set([i[0] for i in cur]))
    # if Box not in Boxes, footprint is 0
    if box not in boxes:
        return 0
      
    try:
        # extract files for alias in upload mode for given box
        cur.execute('SELECT {0}.SizeNotRegistered from {0} WHERE {0}.location=\"All\" AND {0}.egaBox=\"{1}\"'.format(footprint_table, box))
        # check that some alias are in upload mode
        data = int(cur.fetchall()[0][0]) / (10**12)
    except:
        data = -1
    # close connection
    conn.close()
    return data


def add_accessions(credential_file, metadata_db, submission_db, table, associated_table, column_name, prefix, update_status, box):
    '''
    (str, str, str, str, str, str, str, bool, str) -> None
    
    Update table in the submission database with the accessions of dependent 
    objects in the associate table using prefix and column name.
    Update status --> ready if no error and UpdateStatus is True
    
    Paramaters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_db (str): Database storing metadata information
    - submission_db (str): Database storing submission information 
    - table (str): Name of table in submission_db
    - associated_table (str): Table with information on dependent objects
    - column_name (str): Column name in table 
    - prefix (str): Expected prefix of EGA accession Id (eg EGAN, EGAS)
    - update_status (bool): Update alias status if True and no error is found
    - box (str): EGA submssion box (ega-box-xxxx)
    '''
    
    # grab EGA accessions from metadata database, create a dict {alias: accession}
    registered = extract_accessions(credential_file, metadata_db, box, associated_table)
           
    # connect to the submission database
    conn = connect_to_database(credential_file, submission_db)
    cur = conn.cursor()
    # pull alias, dependent Ids for given box
    Cmd = 'SELECT {0}.alias, {0}.{1} FROM {0} WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{2}\"'.format(table, column_name, box)
    
    try:
        cur.execute(Cmd)
        data = cur.fetchall()
    except:
        data = []
    
    # create a dict {alias: [accessions, ErrorMessage]}
    dependent = {}
    # check if alias are in start status
    if len(data) != 0:
        for i in data:
            # make a list of dependent Alias
            alias = i[1].split(';')
            # make a list of dependent accessions
            accessions = []
            for j in alias:
                if j.startswith(prefix):
                    accessions.append(j)
                elif j in registered:
                    accessions.append(registered[j])
            # record error if aliases have missing accessions
            # alias may be in medata table but accession may be NULL if EGA Id is not yet available
            if len(alias) != len(accessions) or 'NULL' in accessions:
                error = 'Accessions not available'
            else:
                error = ''
            dependent[i[0]] =  [';'.join(accessions), error]
            
        if len(dependent) != 0:
            for alias in dependent:
                error = dependent[alias][-1]
                if error == '':
                    # update accessions
                    cur.execute('UPDATE {0} SET {0}.{1}=\"{2}\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{3}\" AND {0}.egaBox=\"{4}\"'.format(table, column_name, dependent[alias][0], alias, box)) 
                else:
                    # record error message
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box)) 
                conn.commit()
                
                # check if status can be updated
                if update_status == True and error == '':
                    # update runs and experiments status only if studyId and sampleId are present
                    if table in ['Experiments', 'Runs']:
                        # make a list of sampleId
                        try:
                            cur.execute('SELECT {0}.sampleId FROM {0} WHERE {0}.egaBox=\"{1}\" AND {0}.alias=\"{2}\"'.format(table, box, alias))
                            samples = cur.fetchall()[0][0].split(';')
                        except:
                            samples = []
                        if len(samples) != 0:
                            # check if all samples have accession Ids 
                            has_accessions = list(set(list(map(lambda x:x.startswith("EGAN"), samples))))
                            if len(has_accessions) == 1 and has_accessions[0] == True:
                                # update satus start --> ready
                                cur.execute('UPDATE {0} SET {0}.Status=\"ready\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box)) 
                                conn.commit()
                    else:
                        # update satus start --> ready
                        cur.execute('UPDATE {0} SET {0}.Status=\"ready\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box)) 
                        conn.commit()
    conn.close()    

    
def add_studyId_analyses_project(credential_file, metadata_database, submission_database, analysis_table, project_table, studies_table, box):
    '''
    (str, str, str, str, str, str, str) -> None
       
    Updates column studyId in AnalysesProject table with the study EGA accession if alias is present
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Name of the database storing metadata information
    - submission_database (str): Name of the database storing information required for registering EGA objects
    - analysis_table (str): Name of the table storing analyses information
    - project_table (str): Name of the table storing project information
    - studies_table (str): name of the table storing studies information
    - box (str): EGA submission box (eg. ega-box-xxxx)
    '''
    
    # grab EGA accessions from metadata database, create a dict {alias: accession}
    registered = extract_accessions(credential_file, metadata_database, box, studies_table)
    # connect to the submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    # pull alias, dependent Ids for given box
    Cmd = 'SELECT {0}.alias, {0}.studyId FROM {0} JOIN {1} WHERE {1}.ProjectKey={0}.alias AND {1}.egaBox=\"{2}\"'.format(project_table, analysis_table, box)
        
    try:
        cur.execute(Cmd)
        data = cur.fetchall()
    except:
        data = []
    
    # update studyId in project 
    data = list(set(data))
    # make a new list with [(alias: studyId)]
    accessions = []
    for i in data:
        if not i[1].startswith('EGAS'):
            if i[1] in registered:
                accessions.append([i[0], registered[i[1]]])
    if len(accessions) != 0:
        for i in accessions:
            with conn.cursor() as cur:
                cur.execute('UPDATE {0} SET {0}.studyId=\"{1}\" WHERE {0}.alias=\"{2}\"'.format(project_table, i[1], i[0])) 
                conn.commit()
    conn.close()    
    

def check_ega_accession_id(credential_file, submission_database, metadata_database, ega_object, table, box):
    '''
    (str, str, str, str, str, str) -> None
    
    Check that all dependent EGA accessions of an EGA object are available in
    the metadata database. Updates status of all aliases in table of the submission
    database for the given box or keep the same status if accessions are not available
    
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - submission_database (str): Name of the metadata sotring information required for submission
    - metata_database (str): Name of the database storing metadata information
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - table (str): Name of table in submission and metadata databases
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # collect all egaAccessionIds for all tables in EGA metadata db
    # accessions may be egaAccessionIds or may be accessions of dependencies
    # eg. dac EGAC00001000010 is not in any egaAccessionId because it was registered in a different box
    # but policy EGAP00001000077 depends on this dac. it can be retrieved in dacId of the policy table
    
    ega_accessions = []
    # list all tables in EGA metadata db
    tables = show_tables(credential_file, metadata_database)
    # extract accessions for each table
    for i in tables:
        # connect to metadata database
        conn = connect_to_database(credential_file, metadata_database)
        cur = conn.cursor()
        # extract egaAccessions and Ids of dependencies
        for j in ['egaAccessionId', 'dacId', 'policyId']:
            try:
                cur.execute('SELECT {0}.{1} from {0} WHERE {0}.egaBox=\"{2}\"'.format(i, j, box)) 
                data = [i[0] for i in cur]   
            except:
                data = []
            # dump egaAccessions for that table to master list
            ega_accessions.extend(data)
        conn.close()
        
    # connect to the submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    # pull alias and egaAccessionIds to be verified
    if ega_object == 'analyses':
        Cmd = 'SELECT {0}.alias, {0}.sampleReferences FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'experiments':
        Cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.studyId FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'datasets':
        Cmd = 'SELECT {0}.alias, {0}.runsReferences, {0}.analysisReferences, {0}.policyId FROM {0} WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'policies':
        Cmd = 'SELECT {0}.alias, {0}.dacId FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    elif ega_object == 'runs':
        Cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.experimentId FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(table, box)
    
    try:
        cur.execute(Cmd)
        data = cur.fetchall()
    except:
        data = []
       
    # create a dict to collect all accessions to be verified for a given alias
    verify = {}
    # check if alias are in start status
    if len(data) != 0:
        for i in data:
            # get alias
            alias = i[0]
            # make a list with all other accessions
            accessions = []
            for j in range(1, len(i)):
                accessions.extend(list(map(lambda x: x.strip(), i[j].split(';'))))
            # remove NULL from list when only analysis or runs are included in the dataset 
            while 'NULL' in accessions:
                accessions.remove('NULL')
            verify[alias] = accessions
        
        if len(verify) != 0:
            # check if all accessions are readily available from metadata db
            for alias in verify:
                # make a list with accession membership
                if False in [i in ega_accessions for i in verify[alias]]:
                    error = 'EGA accession(s) not available as metadata' 
                    # record error and keep status unchanged
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box)) 
                    conn.commit()
                else:
                    error = 'NoError'
                    # set error to NoError and update status 
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"valid\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box)) 
                    conn.commit()
    conn.close()    


def encrypt_and_checksum(credential_file, database, table, box, alias, ega_object, file_paths, file_names, key_ring, outdir, mem):
    '''
    (str, str, str, str, str, str, list, list, str, str, int) -> list
    
    Launch jobs to encrypt files under alias and returns a list job exit codes specifying
    if the jobs were launched successfully or not
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Database storing information required for registration of EGA objects
    - table (str): Table in database storing information about the files to encrypt
    - box (str): EGA submission box (ega-box-xxx)
    - alias (str): Unique identifier of the files in Table
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - file_paths (list): List of file paths for the given alias
    - file_names (list): List of file names used for registration of files under alias 
                         (ie. file names may be different than the file paths basenames)
    - key_ring (str): Path to the key used for encryption
    - outdir (str): Directory in which the encrypted files are written
    - mem (int): Job memory requirement
    '''

    MyCmd1 = 'md5sum {0} | cut -f1 -d \' \' > {1}.md5'
    MyCmd2 = 'gpg --no-default-keyring --keyring {2} -r EGA_Public_key -r SeqProdBio --trust-model always -o {1}.gpg -e {0}'
    MyCmd3 = 'md5sum {0}.gpg | cut -f1 -d \' \' > {0}.gpg.md5'
    
    # check that lists of file paths and names have the same number of entries
    if len(file_paths) != len(file_names):
        return [-1]
    else:
        # make a list to store the job names and job exit codes
        job_exits, job_names = [], []
        # loop over files for that alias      
        for i in range(len(file_paths)):
            # check that FileName is valid
            if os.path.isfile(file_paths[i]) == False:
                # return error that will be caught if file doesn't exist
                return [-1] 
            else:
                # check if OutDir exist
                if os.path.isdir(outdir) == False:
                    return [-1] 
                else:
                    # make a directory to save the scripts
                    qsubdir = os.path.join(outdir, 'qsubs')
                    os.makedirs(qsubdir, exist_ok=True)
                    # create a log dir
                    logdir = os.path.join(qsubdir, 'log')
                    os.makedirs(logdir, exist_ok=True)
                    # get name of output file
                    outfile = os.path.join(outdir, file_names[i])
                    # put commands in shell script
                    BashScript1 = os.path.join(qsubdir, alias + '_' + file_names[i] + '_md5sum_original.sh')
                    BashScript2 = os.path.join(qsubdir, alias + '_' + file_names[i] + '_encrypt.sh')
                    BashScript3 = os.path.join(qsubdir, alias + '_' + file_names[i] + '_md5sum_encrypted.sh')
            
                    with open(BashScript1, 'w') as newfile:
                        newfile.write(MyCmd1.format(file_paths[i], outfile) + '\n')
                    with open(BashScript2, 'w') as newfile:
                        newfile.write(MyCmd2.format(file_paths[i], outfile, key_ring) + '\n')
                    with open(BashScript3, 'w') as newfile:
                        newfile.write(MyCmd3.format(outfile) + '\n')
        
                    # launch qsub directly, collect job names and exit codes
                    JobName1 = 'Md5sum.original.{0}'.format(alias + '__' + file_names[i])
                    # check if 1st file in list
                    if i == 0:
                        QsubCmd1 = "qsub -b y -P gsi -l h_vmem={0}g -N {1} -e {2} -o {2} \"bash {3}\"".format(mem, JobName1, logdir, BashScript1)
                    else:
                        # launch job when previous job is done
                        QsubCmd1 = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(job_names[-1], mem, JobName1, logdir, BashScript1)
                    job1 = subprocess.call(QsubCmd1, shell=True)
                                   
                    JobName2 = 'Encrypt.{0}'.format(alias + '__' + file_names[i])
                    QsubCmd2 = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobName1, mem, JobName2, logdir, BashScript2)
                    job2 = subprocess.call(QsubCmd2, shell=True)
                            
                    JobName3 = 'Md5sum.encrypted.{0}'.format(alias + '__' + file_names[i])
                    QsubCmd3 = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobName2, mem, JobName3, logdir, BashScript3)
                    job3 = subprocess.call(QsubCmd3, shell=True)
                            
                    # store job names and exit codes
                    job_exits.extend([job1, job2, job3])
                    job_names.extend([JobName1, JobName2, JobName3])
        
        # launch check encryption job
        MyCmd = 'sleep 300; module load Gaea/1.0.0 check_encryption -c {0} -s {1} -t {2} -b {3} -a {4} -o {5} -w {6} -j \"{7}\"'
        # get parent directory
        working_dir = os.path.dirname(outdir)
        # put commands in shell script
        BashScript = os.path.join(qsubdir, alias + '_check_encryption.sh')
        with open(BashScript, 'w') as newfile:
            newfile.write(MyCmd.format(credential_file, database, table, box, alias, ega_object, working_dir, ';'.join(job_names)) + '\n')
                
        # launch qsub directly, collect job names and exit codes
        JobName = 'CheckEncryption.{0}'.format(alias)
        # launch job when previous job is done
        QsubCmd = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(job_names[-1], mem, JobName, logdir, BashScript)
        job = subprocess.call(QsubCmd, shell=True)
        # store the exit code (but not the job name)
        job_exits.append(job)          
        
        return job_exits



def encrypt_files(credential_file, database, table, ega_object, box, key_ring, mem, disk_space, working_dir):
    '''
    (str, str, str, str, str, str, int, str, str) -> None
    
    Encrypt files for all alises of the EGA objects if diskspace (in TB) remains available
    after encryption and update file status to encrypting if encryption and md5sum
    jobs are successfully launched 
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of the database storing information required for registration of EGA objects
    - table (str): Table in database
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - box (str): EGA submission box (ega-box-xxxx)
    - key_ring (str): Path to the keys required for encryption
    - mem (int): Required memory for the encryption jobs
    - disk_space (int): Disk space (in TB) available in scratch after encryption is complete  
    - working_dir (str): Directory containing directories with encrypted files
    '''
    
    # create a list of aliases for encryption 
    aliases = select_aliases_for_encryption(credential_file, database, table, box, disk_space, working_dir)
    
    # check if Table exist
    tables = show_tables(credential_file, database)
    if table in tables:
        # connect to database
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        # pull alias, files and working directory for status = encrypt
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"encrypt\" AND {0}.egaBox=\"{1}\"'.format(table, box))
        data = cur.fetchall()
        conn.close()
        
        # check that some files are in encrypt mode
        if len(data) != 0:
            for i in data:
                alias = i[0]
                # encrypt only files of aliases that were pre-selected
                if alias in aliases:
                    # get working directory
                    working_directory = get_working_directory(i[2], working_dir)
                    # create working directory
                    os.makedirs(working_directory, exist_ok=True)
                    # convert single quotes to double quotes for str -> json conversion
                    files = json.loads(i[1].replace("'", "\""))
                    # create parallel lists of file paths and names
                    file_paths, file_names = [] , [] 
                    # loop over files for that alias
                    for file in files:
                        # get the filePath and fileName
                        file_paths.append(files[file]['filePath'])
                        file_names.append(files[file]['fileName'])

                    # remove encrypted files if already exist in working directory
                    # it generates an error if encrypted files are present and encryption starts again
                    # make a list of files in working directory
                    current_encrypted = [os.path.join(working_directory, j) for j in os.listdir(working_directory) if j[-4:] == '.gpg' in j] 
                    for j in current_encrypted:
                        os.remove(j)
                    
                    # update status -> encrypting
                    conn = connect_to_database(credential_file, database)
                    cur = conn.cursor()
                    cur.execute('UPDATE {0} SET {0}.Status=\"encrypting\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box))
                    conn.commit()
                    conn.close()

                    # encrypt and run md5sums on original and encrypted files and check encryption status
                    job_codes = encrypt_and_checksum(credential_file, database, table, box, alias, ega_object, file_paths, file_names, key_ring, working_directory, mem)
                    # check if encription was launched successfully
                    if not (len(set(job_codes)) == 1 and list(set(job_codes))[0] == 0):
                        # store error message, reset status encrypting --> encrypt
                        error = 'Could not launch encryption jobs'
                        conn = connect_to_database(credential_file, database)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.Status=\"encrypt\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box))
                        conn.commit()
                        conn.close()
 
        

def check_encryption(credential_file, database, table, box, alias, ega_object, job_names, working_dir):
    '''
    (file, str, str, str, str, str, str) -> None

    Take the file with DataBase credentials, a semicolon-seprated string of job
    names used for encryption and md5sum of all files under the Alias of Object,
    extract information from Table regarding Alias with encrypting Status and update
    status to upload and files with md5sums when encrypting is done

    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of database
    - table (str): Table in database
    - box (str): EGA submission box (ega-box-xxxx)
    - alias (str): Unique alias of EGA object
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - job_names (str): Semi-colon-separated string of job names used for the encryption of the files under alias
    - working_dir (str): Directory where encrypted files are written 
    '''        
        
    # make a list of job names
    job_names = job_names.split(';')
    
    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    
    try:
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"encrypting\" AND {0}.egaBox=\"{1}\" AND {0}.alias=\"{2}\"'.format(table, box, alias))
        data = cur.fetchall()
    except:
        data = []
    conn.close()
    # check that files are in encrypting mode for this Alias
    if len(data) != 0:
        data = data[0]
        alias = data[0]
        # get the working directory for that alias
        working_directory = get_working_directory(data[2], working_dir)
        # convert single quotes to double quotes for str -> json conversion
        files = json.loads(data[1].replace("'", "\""))
        # create a dict to store the updated file info
        file_info = {}
                
        # create boolean, update when md5sums and encrypted file not found or if jobs didn't exit properly 
        encrypted = True
        
        # check the exit status of each encryption and md5sum jobs for that alias
        for job in job_names:
            if get_job_exit_status(job) != '0':
                encrypted = False
        
        # check that files were encrypted and that md5sums were generated
        for file in files:
            # get the fileName
            file_name = files[file]['fileName']
            if ega_object == 'analyses':
                file_typeId = files[file]['fileTypeId']
            # check that encryoted and md5sum files do exist
            original_md5_file = os.path.join(working_directory, file_name + '.md5')
            encrypted_md5_file = os.path.join(working_directory, file_name + '.gpg.md5')
            encrypted_file = os.path.join(working_directory, file_name + '.gpg')
            if os.path.isfile(original_md5_file) and os.path.isfile(encrypted_md5_file) and os.path.isfile(encrypted_file):
                # get the name of the encrypted file
                encrypted_name = file_name + '.gpg'
                # get the md5sums
                with open(encrypted_md5_file) as infile:
                    encryptedMd5 = infile.readline().rstrip()
                with open(original_md5_file) as infile:
                    originalMd5 = infile.readline().rstrip()
                if encryptedMd5 != '' and originalMd5 != '':
                    # capture md5sums, build updated dict
                    if ega_object == 'analyses':
                        file_info[file] = {'filePath': file, 'unencryptedChecksum': originalMd5, 'encryptedName': encrypted_name, 'checksum': encryptedMd5, 'fileTypeId': file_typeId} 
                    elif ega_object == 'runs':
                        file_info[file] = {'filePath': file, 'unencryptedChecksum': originalMd5, 'encryptedName': encrypted_name, 'checksum': encryptedMd5}
                else:
                    # update boolean
                    encrypted = False
            else:
                # update boollean
                encrypted = False
                
        # check if md5sums and encrypted files is available for all files
        if encrypted == True:
            # update file info and status only if all files do exist and md5sums can be extracted
            conn = connect_to_database(credential_file, database)
            cur = conn.cursor()
            cur.execute('UPDATE {0} SET {0}.files=\"{1}\", {0}.errorMessages=\"None\", {0}.Status=\"upload\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, str(file_info), alias, box))
            conn.commit()
            conn.close()
        elif encrypted == False:
            # reset status encrypting -- > encrypt, record error message
            error = 'Encryption or md5sum did not complete'
            conn = connect_to_database(credential_file, database)
            cur = conn.cursor()
            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"encrypt\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box))
            conn.commit()
            conn.close()
    else:
        # couldn't evaluate encryption, record error and reset to encrypt
        # reset status encrypting -- > encrypt, record error message
        error = 'Could not check encryption'
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"encrypt\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box))
        conn.commit()
        conn.close()


def create_destination_directory(box, password, directory):
    '''
    (str, str, str) -> None    
    
    Create directory and parent directories on the EGA box's staging server
    
    Parameters
    ----------
    - box (str): EGA submission box (ega-box-xxx)
    - password (str): Password to connect to the EGA box
    - directory (str): Directory to be created on the box staging server
    '''
    
    # connect to the box's staging server
    ftp = FTP(host='ftp.ega.ebi.ac.uk', user = box, passwd=password)
    # make a list of directories
    L = directory.split('/')
    directories = []
    for i in range(len(L)):
        directories.append('/'.join(L[0:i+1]))
    for i in directories:
        try:
            ftp.mkd(i)
        except:
            print('directory {0} already exists'.format(i))
    ftp.quit()


def upload_alias_files(alias, files, stage_path, file_dir, credential_file, database, table, ega_object, box, mem, **KeyWordParams):
    '''
    (str, dict, str, str, str, str, str, str, str, int, dict) -> list
    
    Return a list of exit codes for the jobs used for uploading the encrypted and md5 files to stage_path 
    
    Parameters
    ----------
    
    - alias (str): Unique alias of the EGA object
    - files (dict): Dictionary with the alias' files information
    - StagePath (str): Destination directory of the uploaded files on the box' staging server
    - file_dir (str): Directory where encrypted files are located on the file system
    - credential_file (str): File with EGA box and database creentials
    - database (str): Name of the database storing information required for registering EGA objects
    - table (str): Table in database with the EGA object information
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - box (str): EGA submission box (ega-box-xxx)
    - mem (int): Memory requirement for the uploading job
    - KeyWordParams (dict): Optional attributes table
    '''
    
    # get box credentials
    credentials = extract_credentials(credential_file)
    
    # write shell scripts with command
    assert os.path.isdir(file_dir)
    # make a directory to save the scripts
    qsubdir = os.path.join(file_dir, 'qsubs')
    os.makedirs(qsubdir, exist_ok=True)
    # create a log dir
    logdir = os.path.join(qsubdir, 'log')
    os.makedirs(logdir, exist_ok=True)
        
    # command to upload files. requires aspera to be installed
    upload_cmd = "export ASPERA_SCP_PASS={0};ascp -P33001 -O33001 -QT -l300M {1} {2}@fasp.ega.ebi.ac.uk:{3};ascp -P33001 -O33001 -QT -l300M {4} {2}@fasp.ega.ebi.ac.uk:{3};ascp -P33001 -O33001 -QT -l300M {5} {2}@fasp.ega.ebi.ac.uk:{3};"
      
    # create parallel lists to store the job names and exit codes
    job_exits, job_names = [], []
    
    # make a list of file paths
    file_paths = list(files.keys())
    
    # create destination directory
    create_destination_directory(box, credentials[box], stage_path)
        
    # loop over filepaths
    for i in range(len(file_paths)):
        # get filename and encryptedname
        fileName = os.path.basename(file_paths[i])
        encryptedName = files[file_paths[i]]['encryptedName']
        encryptedFile = os.path.join(file_dir, encryptedName)
        originalMd5 = os.path.join(file_dir, encryptedName[:-4]  + '.md5')
        encryptedMd5 = os.path.join(file_dir, encryptedName + '.md5')
        if os.path.isfile(encryptedFile) and os.path.isfile(originalMd5) and os.path.isfile(encryptedMd5):
            MyCmd = upload_cmd.format(credentials[box], encryptedMd5, box, stage_path, originalMd5, encryptedFile)
            # put command in a shell script    
            BashScript = os.path.join(qsubdir, alias + '_' + encryptedName[:-4] + '_upload.sh')
            newfile = open(BashScript, 'w')
            newfile.write(MyCmd + '\n')
            newfile.close()
            # launch job directly
            JobName = 'Upload.{0}'.format(alias + '__' + fileName)
            if len(job_names) == 0:
                QsubCmd = "qsub -b y -P gsi -l h_vmem={0}g -N {1} -e {2} -o {2} \"bash {3}\"".format(mem, JobName, logdir, BashScript)
            else:
                # hold until previous job is done
                QsubCmd = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(job_names[-1], mem, JobName, logdir, BashScript)
            job = subprocess.call(QsubCmd, shell=True)
            # store job exit code and name
            job_exits.append(job)
            job_names.append(JobName)
        else:
            return [-1]
    
    # launch check upload job
    if ega_object == 'analyses':
        if 'attributes' in KeyWordParams:
            attributes_table = KeyWordParams['attributes']
        CheckCmd = 'sleep 600; module load Gaea/1.0.0; Gaea CheckUpload -c {0} -s {1} -t {2} -b {3} -a {4} -j \"{5}\" -o {6} --Attributes {7}'
    elif ega_object == 'runs':
        CheckCmd = 'sleep 600; module load Gaea/1.0.0; Gaea CheckUpload -c {0} -s {1} -t {2} -b {3} -a {4} -j \"{5}\" -o {6}' 
    
    # put commands in shell script
    BashScript = os.path.join(qsubdir, alias + '_check_upload.sh')
    with open(BashScript, 'w') as newfile:
        if ega_object == 'analyses':
            newfile.write(CheckCmd.format(credential_file, database, table, box, alias, ';'.join(job_names), ega_object, attributes_table) + '\n')
        elif ega_object == 'runs':
            newfile.write(CheckCmd.format(credential_file, database, table, box, alias, ';'.join(job_names), ega_object) + '\n')
            
    # launch qsub directly, collect job names and exit codes
    JobName = 'CheckUpload.{0}'.format(alias)
    # launch job when previous job is done
    QsubCmd = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(job_names[-1], mem, JobName, logdir, BashScript)
    job = subprocess.call(QsubCmd, shell=True)
    # store the exit code (but not the job name)
    job_exits.append(job)          
        
    return job_exits


def upload_object_files(credential_file, database, table, ega_object, footprint_table, box, mem, Max, max_footprint, working_dir, **KeyWordParams):
    '''
    (str, str, str, str, str, str, int, int, int, str, dict) -> None
    
    Upload files of all aliases in table with upload status 
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of database storing information required for registrating EGA objects
    - table (str): Table in database storing information about the files to be uploaded
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - footprint_table (str): Table storing the footprint of uploaded files on the EGA box' staging server
    - box (str): EGA submission box (ega-box-xxxx)
    - mem (int): Memory requirement for uploading jobs
    - Max (int): Maximum number of files to upload at once
    - max_footprint (int): Maximum footprint authorized on the EGA box's staging server
    - working_dir (str): Directory containing the sub-directories for each EGA object
    - KeyWordParams (dict): Optional table attributes table
    '''
    
    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
   
    # check Object
    if ega_object == 'analyses':
        # retreieve attributes table
        if 'attributes' in KeyWordParams:
            attributes_table = KeyWordParams['attributes']
        else:
            attributes_table = 'empty'
    
        # extract files
        try:
            # extract files for alias in upload mode for given box
            cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.Status=\"upload\" AND {0}.egaBox=\"{2}\" AND {0}.AttributesKey = {1}.alias'.format(table, attributes_table, box))
            # check that some alias are in upload mode
            data = cur.fetchall()
        except:
            data = []
    elif ega_object == 'runs':
        # extract files
        try:
            # extract files for alias in upload mode for given box
            cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {0}.StagePath FROM {0} WHERE {0}.Status=\"upload\" AND {0}.egaBox=\"{1}\"'.format(table, box))
            # check that some alias are in upload mode
            data = cur.fetchall()
        except:
            data = []
    # close connection
    conn.close()
        
    # get the footprint of non-registered files on the Box's staging server
    not_registered = get_disk_space_staging_server(credential_file, database, footprint_table, box)
    
    # check that alias are ready for uploading and that staging server's limit is not reached 
    if len(data) != 0 and 0 <= not_registered < max_footprint:
        # count the number of files being uploaded
        uploading = int(subprocess.check_output('qstat | grep Upload | wc -l', shell=True).decode('utf-8').rstrip())        
        # upload new files up to Max
        maximum = int(Max) - uploading
        if maximum < 0:
            maximum = 0
        data = data[: maximum]
        
        for i in data:
            alias = i[0]
            # get the file information, working directory and stagepath for that alias
            files = json.loads(i[1].replace("'", "\""))
            working_directory = get_working_directory(i[2], working_dir)
            stage_path  = i[3]
                            
            # update status -> uploading
            conn = connect_to_database(credential_file, database)
            cur = conn.cursor()
            cur.execute('UPDATE {0} SET {0}.Status=\"uploading\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\";'.format(table, alias, box))
            conn.commit()
            conn.close()
            
            # upload files
            job_codes = upload_alias_files(alias, files, stage_path, working_directory, credential_file, database, table, ega_object, box, mem, **KeyWordParams)
                        
            # check if upload launched properly for all files under that alias
            if not (len(set(job_codes)) == 1 and list(set(job_codes))[0] == 0):
                # record error message, reset status same uploading --> upload
                error = 'Could not launch upload jobs'
                conn = connect_to_database(credential_file, database)
                cur = conn.cursor()
                cur.execute('UPDATE {0} SET {0}.Status=\"upload\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box))
                conn.commit()
                conn.close()



def get_files_staging_server(box, password, directory):
    '''
    (str, str, str) -> list
    
    Returns a list of full paths to files under directory 
    
    Parameters
    ----------
    - box (str): EGA submission box (ega-box-xxx)
    - password (str): Password to connect to the EGA submission box
    - directory (str): Directory on the EGA box' staging server
    '''
    
    # connect to the EGA ftp server
    ftp = FTP(host='ftp.ega.ebi.ac.uk', user = box, passwd = password)
    # navigate to directory
    ftp.cwd(directory)
    # make a list with the directory's content
    content = []
    ftp.dir(content.append)
    # make a list of file paths in directory
    uploaded_files = [os.path.join(directory, i.split()[-1]) for i in content if i.startswith('-')]
    # get the file paths
    for i in range(len(uploaded_files)):
        uploaded_files[i] = uploaded_files[i].split()[-1]
    ftp.quit()
    
    return uploaded_files
    
      
def list_files_staging_server(credential_file, database, table, box, ega_object, **KeyWordParams):
    '''
    (str, str, str, str, str, dict) -> dict
    
    Returns a dictionary with files on the EGA box' staging server for all aliases
    in table with uploading status

    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Database storing information required for registration of EGA objects
    - table (str): Table in database storing information about files to register
    - box (str): EGA submission box (ega-box-xxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - KeyWordParams: Optional attributes table
    '''
        
    # get credentials
    credentials = extract_credentials(credential_file)
        
    # make a dict {directory: [files]}
    files_box = {}
        
    # check that Analysis table exists
    tables = show_tables(credential_file, database)
    if table in tables:
        # connect to database
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        # extract files for alias in upload mode for given box
        if ega_object == 'analyses':
            if 'attributes' in KeyWordParams:
                attributes_table = KeyWordParams['attributes']
            else:
                attributes_table = 'empty'
            Cmd = 'SELECT {0}.alias, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.AttributesKey = {1}.alias AND {0}.Status=\"uploading\" AND {0}.egaBox=\"{2}\"'.format(table, attributes_table, box)
        elif ega_object == 'runs':
            Cmd = 'SELECT {0}.alias, {0}.StagePath FROM {0} WHERE {0}.Status=\"uploading\" AND {0}.egaBox=\"{1}\"'.format(table, box)
        
        try:
            cur.execute(Cmd)
            data = cur.fetchall()
        except:
            data = []
        conn.close()
        
        # check that some aliases have the proper status
        if len(data) != 0:
            # make a list of stagepath
            stage_paths = list(set([i[1] for i in data]))
            for i in stage_paths:
                uploaded_files = get_files_staging_server(box, credentials[box], i)
                # populate dict
                files_box[i] = uploaded_files
                
    return files_box


def convert_to_tb(file_size):
    '''
    (str) -> float
    
    Returns the file size in Tb
    
    Parameters
    ----------
    - file_size (str): File size including unit  
    '''

    if 'T' in file_size:
        size = float(file_size.replace('T', ''))
    elif 'K' in file_size:
        size = float(file_size.replace('K', '')) / 1000000000
    elif 'M' in file_size:
        size = float(file_size.replace('M', '')) / 1000000
    elif 'G' in file_size:
        size = float(file_size.replace('G', '')) / 1000
    return size


def get_working_directory_space(working_dir):
    '''
    (str) -> list
    
    Returns a list with total size, used space and available space (all in Tb)
    for the working directory where all submission directories are written

    Parameters
    ----------
    - working_dir (str): Directory containing the sub-directories for each EGA object
    '''
    
    # get total, free, and used space in working directory
    usage = subprocess.check_output('df -h {0}'.format(working_dir), shell=True).decode('utf-8').rstrip().split()
    total, used, available = usage[8], usage[9], usage[10]
    L = [total, used, available]
    for i in range(len(L)):
        L[i] = convert_to_tb(L[i])
    return L
    

def get_file_size(file_path):
    '''
    (str) -> float
    
    Return the file size in Tb
    
    Parameters
    ----------
    - file_path (str): Path to file
    '''
        
    filesize = subprocess.check_output('du -sh {0}'.format(file_path), shell=True).decode('utf-8').rstrip().split()
    assert file_path == filesize[1]
    # convert file size to Tb
    size = convert_to_tb(filesize[0])
    return size


def count_file_usage(credential_file, database, table, box, status):
    '''
    (str, str, str, str, int) -> dict
    
    Returns a dictionary with the size of all files for a given alias for all aliases
    in table of database with status "encrypting"
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of database with information required for registering EGA objects
    - table (str): Table name in database
    - box (str): EGA submission box (ega-box-xxx)
    - status (str): Submission status of objects in table
    '''
        
    # create a dict {alias : file size}
    D = {}
        
    # check if Table exist
    tables = show_tables(credential_file, database)
    if table in tables:
        # connect to database
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        # pull alias and files for status = encrypt
        cur.execute('SELECT {0}.alias, {0}.files FROM {0} WHERE {0}.Status=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(table, status, box))
        data = cur.fetchall()
        conn.close()
        
        # check that some files are in encrypt mode
        if len(data) != 0:
            for i in data:
                assert i[0] not in D
                # convert single quotes to double quotes for str -> json conversion
                files = json.loads(i[1].replace("'", "\""))
                # make a list to record file sizes of all files under the given alias
                filesize = []
                # loop over filepath:
                for j in files:
                    # get the file size
                    filesize.append(get_file_size(files[j]['filePath']))
                D[i[0]] = sum(filesize)
    return D            


def select_aliases_for_encryption(credential_file, database, table, box, disk_space, working_dir):
    '''
    (str, str, str, str, int, str) -> list
    
    
    Returns a list of aliases with files that can be encrypted while keeping 
    disk_space (in Tb) available after encryption    
        
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of database storing information required for registering EGA objects
    - table (str): Name of table on database 
    - box (str): EGA submission box (ega-box-xxx)
    - disk_space (int): The amount of disk space that should remain available after encryption
    - working_dir (str): Working directory containing sub-directories where encrypted files are written
    '''
        
    # get disk space of working directory
    total, used, available = get_working_directory_space(working_dir)
    # get file size of all files under each alias with encrypt status
    encrypt = count_file_usage(credential_file, database, table, box, 'encrypt')
    # get file size of all files under each alias with encrypting status
    encrypting = count_file_usage(credential_file, database, table, box, 'encrypting')
    # substract file size for all aliases with encrypting status from available size
    for alias in encrypting:
        available -= encrypting[alias]
        
    # record aliases for encryption
    aliases = []
    for alias in encrypt:
        # do not encrypt if the new files result is < DiskSpace of disk availability 
        if available - encrypt[alias] > disk_space:
            available -= encrypt[alias]
            aliases.append(alias)
    return aliases


def is_upload_successfull(log_file):
    '''
    (str) --> bool
    
    Returns True if 3 lines with 'Completed' are in the log (ie. successfull upload
    of 2 md5sums and 1 .gpg), and returns False otherwise
        
    Parameters
    ----------
    - log_file (str): Path to the log file of the job uploading files
    '''
    
    infile = open(log_file)
    content = infile.read()
    infile.close()
    
    # 3 'Completed' if successful upload (2 md5sums and 1 encrypted are uploaded together) 
    if content.count('Completed') == 3:
        return True
    else:
        return False


def check_upload_success(logdir, alias, file_name):
    '''
    (str, str, str) --> bool
    
    Returns True if no error are found in the most recent upload log (ie. all files
    for the given alias uploaded successfully) and False if errors are found
    
    Parameters
    ----------
    - logdir (str): Log directory
    - alias (str): Alias of an EGA object to register in the submission database
    - file_name (str): Name of file to upload
    '''

    # sort the out log files from the most recent to the older ones
    logfiles = subprocess.check_output('ls -lt {0}'.format(os.path.join(logdir, 'Upload.*.o*')), shell=True).decode('utf-8').rstrip().split('\n')
    # keep the log out file names
    for i in range(len(logfiles)):
        logfiles[i] = logfiles[i].strip().split()[-1]
    
    # set up a boolean to update if most recent out log is found for FileName
    Found = False
    
    # loop over the out log
    for filepath in logfiles:
        # extract logname and split to get alias and file name
        # '__' may be present in file name, '__' is not allowed in alias
        logname = os.path.basename(filepath).split('__')
        i, j = logname[0], '__'.join(logname[1:])
        if alias == i.replace('Upload.', '') and file_name == j[:j.rfind('.o')]:
            # update boolean and exit
            Found = True
            break   
    
    # check if most recent out log is found
    if Found == True:
        # check that log file exists
        if os.path.isfile(filepath):
            # check if upload was successful
            return is_upload_successfull(filepath)
        else:
            return False
    else:
        return False
    
    
def check_upload_files(credential_file, database, table, box, ega_object, alias, job_names, working_dir, **KeyWordParams):
    '''
    (str, str, str, str, str, str, str, dict) -> None
    
    Updates status of alias from uploading to uploaded if all the files for
    that alias were successfuly uploaded.  
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Database with information required for registering EGA objects
    - table (str): Name of table in database
    - box (str): EGA submission box (ega-box-xxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - alias (str): Unique identifier for the files in table
    - job_names (str): Semi-colon-separated string of job names used for uploading files under the alias
    - working_dir (str): Parent directory containing sub-folders where encrypted files are located 
    - KeyWordParams (str): Optional attributes table
    '''

    # make a dict {directory: [files]} for alias with uploading status 
    files_box = list_files_staging_server(credential_file, database, table, box, ega_object, **KeyWordParams)
        
    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    
    if ega_object == 'analyses':
        if 'attributes' in KeyWordParams:
            attributes_table = KeyWordParams['attributes']
        else:
            attributes_table = 'empty'
        Cmd = 'SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.AttributesKey = {1}.alias AND {0}.Status=\"uploading\" AND {0}.egaBox=\"{2}\" AND {0}.alias=\"{3}\"'.format(table, attributes_table, box, alias)
    elif ega_object == 'runs':
        Cmd = 'SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {0}.StagePath FROM {0} WHERE {0}.Status=\"uploading\" AND {0}.egaBox=\"{1}\" AND {0}.alias=\"{2}\"'.format(table, box, alias)

    try:
        # extract files for alias in uploading mode for given box
        cur.execute(Cmd)
        data = cur.fetchall()
    except:
        data= []
    conn.close()
        
    if len(data) != 0:
        # check that some files are in uploading mode
        for i in data:
            alias = i[0]
            # convert single quotes to double quotes for str -> json conversion
            files = json.loads(i[1].replace("'", "\""))
            working_directory = get_working_directory(i[2], working_dir)
            stage_path = i[3]
            # set up boolean to be updated if uploading is not complete
            uploaded = True
            
            # get the log directory
            logdir = os.path.join(working_directory, 'qsubs/log')
            # check the out logs for each file
            for file_path in files:
                # get filename
                filename = os.path.basename(file_path)
                # check if errors are found in log
                if check_upload_success(logdir, alias, filename) == False:
                    uploaded = False
            
            # check the exit status of the jobs uploading files
            for jobName in job_names.split(';'):
                if get_job_exit_status(jobName) != '0':
                    uploaded = False
            
            # check if files are uploaded on the server
            for file_path in files:
                # get filename
                filename = os.path.basename(file_path)
                encryptedFile = files[file_path]['encryptedName']
                originalMd5, encryptedMd5 = encryptedFile[:-4] + '.md5', encryptedFile + '.md5'                    
                for j in [encryptedFile, encryptedMd5, originalMd5]:
                    if j not in files_box[stage_path]:
                        uploaded = False
            # check if all files for that alias have been uploaded
            if uploaded == True:
                # connect to database, update status and close connection
                conn = connect_to_database(credential_file, database)
                cur = conn.cursor()
                cur.execute('UPDATE {0} SET {0}.Status=\"uploaded\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box)) 
                conn.commit()                                
                conn.close()              
            elif uploaded == False:
                # reset status uploading --> upload, record error message
                error = 'Upload failed'
                conn = connect_to_database(credential_file, database)
                cur = conn.cursor()
                cur.execute('UPDATE {0} SET {0}.Status=\"upload\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box)) 
                conn.commit()                                
                conn.close()
    else:
        # reset status uploading --> upload, record error message
        error = 'Could not check uploaded files'
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        cur.execute('UPDATE {0} SET {0}.Status=\"upload\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, error, alias, box)) 
        conn.commit()                                
        conn.close()

        
def clean_up_error(error_messages):
    '''
    (str or list or None) -> str
    
    Returns a string with error messages returned by the EGA API
        
    Parameters
    ---------
    - error_messages (str or list or None): Error message returns from the API.
    '''
    
    # check how error Messages is returned from the api 
    if type(error_messages) == list:
        if len(error_messages) >= 1:
            error_messages = ';'.join(error_messages)
        elif len(error_messages) == 0:
            error_messages = 'None'
    else:
        error_messages = str(error_messages)
    # remove double quotes to save in table
    error_messages = str(error_messages).replace("\"", "")
    return error_messages



def remove_files_after_submission(credential_file, database, table, box, remove, working_dir):
    '''
    (str, str, str, str, bool, str) -> None
    
    Removes encrypted and md5sums files from subdirectories in working_dir if remove is True
    after submission, ie for all aliases with "uploaded" status in table of database
        
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - database (str): Name of the database
    - table (str): Table in database
    - box (str): EGA box (ega-box-xxx)
    - remove (bool): Remove files if True
    - working_dir (str): Directory where all the submissions directories are written
    '''
    
    if remove == True:
        # connect to database
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        try:
            # get the directory, files for all alias with "uploaded" status
            cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory FROM {0} WHERE {0}.status=\"uploaded\" AND {0}.egaBox=\"{1}\"'.format(table, box))
            data = cur.fetchall()
        except:
            data = []
        conn.close()
        if len(data) != 0:
            for i in data:
                alias, files = i[0], json.loads(str(i[1]).replace("'", "\""))
                # get the working directory for that alias
                workingdir = get_working_directory(i[2], working_dir)
                files = [os.path.join(workingdir, files[i]['encryptedName']) for i in files]
                for i in files:
                    assert i[-4:] == '.gpg'
                    a, b = i + '.md5', i.replace('.gpg', '') + '.md5'
                    if os.path.isfile(i) and working_dir in i and '.gpg' in i:
                        # remove encrypted file
                        os.system('rm {0}'.format(i))
                    if os.path.isfile(a) and working_dir in a and '.md5' in a:
                        # remove md5sum
                        os.system('rm {0}'.format(a))
                    if os.path.isfile(b) and working_dir in b and '.md5' in b:
                        # remove md5sum
                        os.system('rm {0}'.format(b))


# use this function to check upload    
def check_upload(ega_object, credential_file, submission_database, table, box, alias, jobnames, attributes_table):
    '''    
    (str, str, str, str, str, str, str)
    
    Updates alias status to uploaded if upload is succesfull or reset status to upload
    
    Parameters
    ----------
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - credential_file (str): File to EGA boxes and database credentials
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table in database storing information for a specific EGA object
    - box (str): EGA submission box (ega-box-xxxx)
    - alias (str): Unique identifier for the uploaded files   
    - jobnames (str): semi-colon-separated list of job names used for uploading all the files under a given alias
    - attributes_table (str): Table storing analysis attributes information
    '''
    
    if ega_object == 'analyses':
        # check that files have been successfully uploaded, update status uploading -> uploaded or rest status uploading -> upload
        check_upload_files(credential_file, submission_database, table, box, ega_object, alias, jobnames, attributes = attributes_table)
    elif args.object == 'runs':
        check_upload_files(credential_file, submission_database, table, box, ega_object, alias, jobnames)
    

def create_json(credential_file, submission_database, metadata_database, table, ega_object, working_dir, key_ring, memory, disk_space, samples_attributes_table, analysis_attributes_table, projects_table, footprint_table, max_uploads, max_footprint, remove, box):
    '''
    (str, str, str, str, str, str, str, int, int, str, str, str, str, int, int, bool, str) -> None
    
    Forms the submission json for a given EGA object and stores the json in the submission database
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - submission_database (str): Database storing information required for regitration of EGA objects
    - metadata_database (str): Database storing information about registered EGA objects
    - table (str): Table storing information about a specific EGA object    
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - working_dir (str): Parent directory containing sub-folders in which encrypted files are located
    - key_ring (str): Path to the keys required for encryption
    - memory (int): Job memory requirement 
    - disk_space (int): Disk space (in TB) available in scratch after encryption is complete
    - samples_attributes_table (str): Table storing samples attributes information
    - analysis_attributes_table (str): Table storing analysis attributes information
    - projects_table (str): Table storing project information
    - footprint_table (str): Table with foot print by project on the staging servers
    - max_uploads (int): Maximum number of files to upload at once
    - max_footprint (int): Maximum footprint authorized on the EGA box's staging server
    - remove (bool): Remove encrypted after successful upload if True
    - box (str): EGA submission box (ega-box-xxx)
    '''

    # check if Analyses table exists
    tables = show_tables(credential_file, submit_metadata)
        
    if table in tables:
        ## grab aliases with start status and check if required information is present in table(s) 
        if ega_object == 'analyses':
            # check if required information is present in analyses, attributes and projects tables
            check_table_information(credential_file, metadata_database, submission_database, table, ega_object, box, attributes = analysis_attributes_table, projects = projects_table) 
        elif ega_object == 'samples':
            # check if required information is present in samples and attributes tables
            check_table_information(credential_file, metadata_database, submission_database, table, ega_object, box, attributes = samples_attributes_table)
        else:
            # check if required information is present in object table
            check_table_information(credential_file, metadata_database, submission_database, table, ega_object, box)
        
        ## replace aliases with accessions and change status clean --> ready or keep clean --> clean
        if ega_object == 'analyses':
            # replace studyId in Analyses project table if study alias is present
            add_studyId_analyses_project(credential_file, metadata_database, submission_database, table, 'AnalysesProjects', 'Studies', box)
            # replace sample aliases for analyses objects and update status ir no error
            add_accessions(credential_file, metadata_database, submission_database, table, 'Samples', 'sampleReferences', 'EGAN', True, box)
        elif ega_object == 'experiments':
            # replace sample aliases for experiments objects
            add_accessions(credential_file, metadata_database, submission_database, table, 'Samples', 'sampleId', 'EGAN', False, box)
            # replace study aliases for experiments objects and update status if no error
            add_accessions(credential_file, metadata_database, submission_database, table, 'Studies', 'studyId', 'EGAS', True, box)
        elif ega_object == 'runs':
            # replace sample aliases for runs objects
            add_accessions(credential_file, metadata_database, submission_database, table, 'Samples', 'sampleId', 'EGAN', False, box)
            # replace experiment aliases for runs objects and update status if no error
            add_accessions(credential_file, metadata_database, submission_database, table, 'Experiments', 'experimentId', 'EGAX', True, box)
        elif ega_object == 'policies':
            # replace DAC aliases for policies objects and update status if no error
            add_accessions(credential_file, metadata_database, submission_database, table, 'Dacs', 'DacId', 'EGAC', True, box)
        
        ## check that EGA accessions that object depends on are available metadata and change status --> valid or keep clean --> clean
        if ega_object in ['analyses', 'datasets', 'experiments', 'policies', 'runs']:
            check_ega_accession_id(credential_file, submission_database, metadata_database, ega_object, table, box)
                
        ## encrypt and upload files for analyses and runs 
        if ega_object in ['analyses', 'runs']:
            ## set up working directory, add to analyses table and update status valid --> encrypt
            add_working_directory(credential_file, submission_database, table, box, working_dir)
                   
            ## encrypt new files only if diskspace is available. update status encrypt --> encrypting
            ## check that encryption is done, store md5sums and path to encrypted file in db, update status encrypting -> upload or reset encrypting -> encrypt
            encrypt_files(credential_file, submission_database, table, ega_object, box, key_ring, memory, disk_space, working_dir)
        
            ## upload files and change the status upload -> uploading 
            ## check that files have been successfully uploaded, update status uploading -> uploaded or rest status uploading -> upload
            if ega_object == 'analyses':
                upload_object_files(credential_file, submission_database, table, ega_object, footprint_table, box, memory, max_uploads, max_footprint, attributes = analysis_attributes_table)
            elif ega_object == 'runs':
                upload_object_files(credential_file, submission_database, table, ega_object, footprint_table, box, memory, max_uploads, max_footprint)
            
            ## remove files with uploaded status. does not change status. keep status uploaded --> uploaded
            remove_files_after_submission(credential_file, submission_database, table, box, remove, working_dir)

        ## form json and add to table and update status --> submit or keep current status
        if ega_object == 'analyses':
            ## form json for analyses in uploaded mode, add to table and update status uploaded -> submit
            add_json_to_table(credential_file, submission_database, table, box, ega_object, projects = projects_table, attributes = analysis_attributes_table)
        elif ega_object == 'samples':
             # update status valid -> submit if no error of keep status --> valid and record errorMessage
             add_json_to_table(credential_file, submission_database, table, box, ega_object, attributes = samples_attributes_table)
        else:
            ## form json for all other objects in valid status and add to table
            # update status valid -> submit if no error or leep status --> and record errorMessage
            add_json_to_table(credential_file, submission_database, table, box, ega_object)


def update_submitted_status(credential_file, database, table, box):
    '''
    (str, str, str, str) -> None
    
    Updates Status from submit to SUBMITTED for all aliases in database table 
    that are already registered in box and have an egaAccessionId but for which
    files needed to be re-uploaded 

    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of submission database
    - table (str): Name of table in database
    - box (str): EGA submission box (ega-box-xxxx)
    '''
    
    # connect to submission database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    # grab data with submit status that have egaAccessionId        
    try:
        cur.execute('SELECT {0}.alias, {0}.egaAccessionId FROM {0} WHERE {0}.Status=\"submit\" AND {0}.submissionStatus=\"SUBMITTED\" AND {0}.egaBox=\"{1}\"'.format(table, box))
        # extract all information 
        data = cur.fetchall()
    except:
        # record error message
        data = []
    if len(data) != 0:
        for i in data:
            alias, accession = i[0], i[1]
            # check that accession exists and object is already registered
            if accession.startswith('EGA'):
                # object already registered update status submit --> SUBMITTED 
                cur.execute('UPDATE {0} SET {0}.Status=\"SUBMITTED\" WHERE {0}.alias=\"{1}\" AND {0}.egaAccessionId=\"{2}\" AND {0}.Status=\"submit\" AND {0}.submissionStatus=\"SUBMITTED\" AND {0}.egaBox=\"{3}\"'.format(table, alias, accession, box))  
                conn.commit()
    conn.close()


def submit_metadata(credential_file, submission_database, table, box, ega_object, portal):
    '''
    (str, str, str, str, str, str) -> None
    
    Register a given EGA object by submitting a json with required information to the EGA submission API
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - submission_database (str): Database storing information required for registration of EGA objects
    - table (str): Table storing information about a specific EGA object
    - box (str): EGA submission box (ega-box-xxxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - portal (str): URL of the EGA submisison API
    '''
    
    # check if Analyses table exists
    Tables = show_tables(credential_file, submission_database)
    if table in Tables:
        # clean up objects with VALIDATED_WITH_ERRORS, VALIDATED and DRAFT submission status
        for i in ['VALIDATED_WITH_ERRORS', 'VALIDATED', 'DRAFT']:
            delete_validated_objects_with_errors(credential_file, submission_database, table, box, ega_object, portal, i)
        # submit analyses with submit status and no EGA accessions                
        register_objects(credential_file, submission_database, table, box, ega_object, portal)
        # update submit status to SUBMITTED for analyses and runs objects that have been submitted but needed re-upload
        if ega_object in ['runs', 'analyses']:
            update_submitted_status(credential_file, submission_database, table, box)


def register_ega_objects(credential_file, submission_database, metadata_database, 
                         working_dir, key_ring, memory, disk_space, footprint_table,
                         samples_attributes_table, analysis_attributes_table, projects_table,
                         max_uploads, max_footprint, remove, portal, box):
    '''
    (str, str, str, str, str, str, int, int, str, str, str, str, int, int, bool, str, str) -> None
    
    Register all EGA objects to the EGA API    
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - submission_database (str): Database storing information required for regitration of EGA objects
    - metadata_database (str): Database storing information about registered EGA objects
    - working_dir (str): Parent directory containing sub-folders in which encrypted files are located
    - key_ring (str): Path to the keys required for encryption
    - memory (int): Job memory requirement 
    - disk_space (int): Disk space (in TB) available in scratch after encryption is complete
    - footprint_table (str): Table with foot print by project on the staging servers
    - samples_attributes_table (str): Table storing samples attributes information
    - analysis_attributes_table (str): Table storing analysis attributes information
    - projects_table (str): Table storing project information
    - max_uploads (int): Maximum number of files to upload at once
    - max_footprint (int): Maximum footprint authorized on the EGA box's staging server
    - remove (bool): Remove encrypted after successful upload if True
    - portal (str): URL of the EGA submisison API
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    for ega_object in ['studies', 'runs', 'samples', 'experiments', 'datasets', 'analyses', 'policies', 'dacs']:
        table = ega_object.title()    
        # create json
        create_json(credential_file, submission_database, metadata_database, table, ega_object, working_dir, key_ring, memory, disk_space, samples_attributes_table, analysis_attributes_table, projects_table, footprint_table, max_uploads, max_footprint, remove, box)
        # submit json and register object
        submit_metadata(credential_file, submission_database, table, box, ega_object, portal)


def find_file_typeId(d, L, analysis_enums):
    '''
    (dict, list, dict) -> dict

    Returns a dictionary mapping the files to their fileTYpeId value
        
    Parameters
    ----------
    - d (dict): Dictionary with analysis file information
    - L (list): List of dictionaries with submitted file information
    - analysis_enums (dict): Dictionary of EGA analysis file type enumeration 
    '''

    # reverse dict {tag: value}
    tags = {}
    for i in analysis_enums:
        tags[analysis_enums[i]] = i
    # create a dict to map files to fileTypeId
    file_type = {}
    # loop over file paths in d
    for i in d:
        # loop over list of dicts with submitted file info
        for j in L:
            # map file type Id to file path by comparing checksums
            if d[i]['checksum'] == j['checksum']:
                fileTypeId = j['fileTypeId']
                file_type[i] = tags[fileTypeId]
    return file_type
    

def add_missing_working_directory(credential_file, database, table, alias, box, working_dir):
    '''
    (str, str, str, str, str, str) -> None
    
    Create directory in file system for a given alias with SUBMITTED status if it
    doesn't already exist and record in submission database
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of the database storing information required for registration of EGA objects
    - table (str): Table in database
    - alias (str): Alias of a given EGA object in table
    - box (str): EGA submission box (ega-box-xxx)
    - working_dir (str) = Directory in which all the directories used for submission of metadata are written
    '''
    
    # connect to db
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    try:
        cur.execute('SELECT {0}.alias, {0}.WorkingDirectory FROM {0} WHERE {0}.alias=\"{1}\" AND {0}.Status=\"SUBMITTED\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box))
        data = cur.fetchall()[0]
    except:
        data = []
    if len(data) != 0:
        # check working directory
        alias, workingdir = data[0], data[1]
        if workingdir in ['', None, 'None', 'NULL']:
            # create working directory
            UID = str(uuid.uuid4())             
            # record identifier in table, create working directory in file system
            cur.execute('UPDATE {0} SET {0}.WorkingDirectory=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, UID, alias, box))  
            conn.commit()
            # create working directories
            workingdir = get_working_directory(UID, working_dir)
            os.makedirs(workingdir)
    
  

def edit_submitted_status(credential_file, database, table, alias, box, analysis_enums, working_dir):
    '''
    (str, str, str, str, str, dict, str) -> None
    
    Changes the status of alias in table of database for the given box
    from SUBMITTED to encrypt. Also create a working directory if it doesn't already exist
    Reformat the file information so that files can be encrypted.
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - database (str): Name of the database storing information required for registration of EGA objects
    - table (str): Table in database
    - alias (str): Alias of a given EGA object in table
    - box (str): EGA submission box (ega-box-xxx)
    - analysis_enums (dict): Dictionary of EGA analysis file type enumeration 
    - working_dir (str) = Directory in which all the directories used for submission of metadata are written
    '''
    
    # connect to db
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
        
    # create working directories if they don't exist
    add_missing_working_directory(credential_file, database, table, alias, box, working_dir)
    # check that working directory exist. update Status --> encrypt and reformat file json if no error or keep status and record message
    try:
        cur.execute('SELECT {0}.alias, {0}.egaAccessionId, {0}.files, {0}.json, {0}.WorkingDirectory FROM {0} WHERE {0}.alias=\"{1}\" AND {0}.Status=\"SUBMITTED\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box))
        data = cur.fetchall()[0]
    except:
        data = []
    if len(data) != 0:
        error = []
        alias, ega_accession, files, submission_json, working_directory = data[0], data[1], json.loads(data[2].replace("'", "\"")), json.loads(data[3].replace("'", "\"")), get_working_directory(data[4], working_dir)
        # check if analysis of runs objects
        if ega_accession.startswith('EGAZ'):
            # analysis object, find file type for all files
            file_types = find_file_typeId(files, submission_json['files'], analysis_enums)
            # reformat file json
            new_files = {}
            for file in files:
                file_typeId, filename, file_path = file_types[file], files[file]['encryptedName'], files[file]['filePath']
                assert filename[-4:] == '.gpg'
                filename = filename[:-4]
                new_files[file] = {'filePath': file_path, 'fileName': filename, 'fileTypeId': file_typeId}
        elif ega_accession.startswith('EGAR'):
            # run object
            # reformat file json
            new_files = {}
            for file in files:
                filename, file_path = files[file]['encryptedName'], files[file]['filePath']
                assert filename[-4:] == '.gpg'
                filename = filename[:-4]
                new_files[file] = {'filePath': file_path, 'fileName': filename}
        if working_directory in ['', 'NULL', None, 'None']:
            error.append('Working directory does not have a valid Id')
        if os.path.isdir(working_directory) == False:
            error.append('Working directory not generated')
        # check if error message
        if len(error) != 0:
            # error is found, record error message
            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, ';'.join(error), alias, box))  
            conn.commit()
        else:
            # no error, update Status SUBMITTED --> encrypt and file json
            cur.execute('UPDATE {0} SET {0}.files=\"{1}\", {0}.Status=\"encrypt\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\" AND {0}.Status=\"SUBMITTED\"'.format(table, str(new_files), alias, box))  
            conn.commit()
    conn.close()            


def reupload_registered_files(credential_file, metadata_database, submission_database, analysis_table, runs_table, working_dir, alias_file, box):
    '''
    (str, str, str, str, str, str, str, str) -> None
        
    Changes the status of registered file objects from SUBMITTED to encrypt for
    re-encryption and re-uploading
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Name of the database storing metadata information
    - submission_database (str): Database storing information required for registration of EGA objects
    - analysis_table (str): Table storing analysis metadata  
    - runs_table (str): Table storing runs metadata
    - working_dir (str): Directory in where sub-directories with submission information are written
    - alias_file (str): Tab-delimited file with 2-columns including aliases of files to re-upload and the corresponding EGA accession Id
    - box (str): EGA submission box (ega-box-xxxx)
    '''
    
    # get analysis_file_types enumerations
    enums = list_enumerations()
    analysis_enums = enums['AnalysisFileTypes']
        
    # grab alias and EGA accessions from metadata database, create a dict {alias: accession} for analysis and runs objects
    registered_analyses = extract_accessions(credential_file, metadata_database, box, analysis_table)
    registered_runs = extract_accessions(credential_file, metadata_database, box, runs_table)
        
    # get the list of aliases, egaAccessionId from file
    try:
        infile = open(alias_file)
        aliases = list(map(lambda x: x.strip(), infile.read().rstrip().split('\n')))
        infile.close()
    except:
        aliases = []
        
    # change status of registered aliases to encrypt
    # change status SUBMITTED --> encrypt and create working directory if doesn't exist
    if len(aliases) != 0:
        for i in aliases:
            i = list(map(lambda x: x.strip(), i.split()))
            alias, egaAccession = i[0], i[1]
            if egaAccession.startswith('EGAZ') and alias in registered_analyses:
                edit_submitted_status(credential_file, submission_database, analysis_table, alias, box, analysis_enums, working_dir)
            elif egaAccession.startswith('EGAR') and alias in registered_runs:
                edit_submitted_status(credential_file, submission_database, runs_table, alias, box, analysis_enums, working_dir)
        
   
def file_info_staging_server(credential_file, metadata_database, submission_database, analyses_table, runs_table, staging_table, footprint_table, box):
    '''
    (list) -> None
    
    (str, str, str, str, str, str, str, str) -> None 
    
    Populates staging and footprint tables in submission fatabase with information
    about the files uploaded to the box' staging server
    
    Parameters
    ----------
    - credential_file (str): File with EGA box and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing information required for regitration of EGA objects
    - analyses_table (str): Table storing information about analysis objects
    - runs_table (str): Table storing information about runs objects
    - staging_table (str): Table storing information about files on the box' staging server
    - box (str): EGA submission box (ega-box-xxx)
    - footprint_table (str): Table with foot print by project on the staging servers
    '''

    # add info for all files on staging server for given box
    add_file_info_staging_server(credential_file, metadata_database, submission_database, analyses_table, runs_table, staging_table, box)
    # summarize data into footprint table
    add_footprint_data(credential_file, submission_database, staging_table, footprint_table, box)
    


############## functions from MEGA


def format_url(URL):
    '''
    (str) -> str
        
    Return the URL ending with a slash
    
    Parameters
    ----------
    - URL (str): URL of the API
    '''
    
    if URL[-1] != '/':
        URL = URL + '/'
    return URL
    

def connect_to_api(username, password, URL):
    '''
    (str, str, str) -> str    
    
    Returns a token granting access to the API at URL using the username and password
    for a given EGA box
    
    Parameters
    ----------
    - username (str): Username of a given box (e.g. ega-box-xxxx)
    - password (str): Password to access the EGA box
    - URL (str): URL of the API
    '''

    URL = format_url(URL)
    data = {'username': username, 'password': password, 'loginType': 'submitter'}
    login = requests.post(URL + 'login', data=data)
    token = login.json()['response']['result'][0]['session']['sessionToken']
    return token
    

def close_api_connection(token, URL):
    '''
    (str, str) -> None
    
    Ends the connection to API at URL by deleting Token
    
    Parameters
    ----------
    - token (str): String granting access to the API for a given EGA box
    - URL (str): URL of the API
    '''
        
    URL = format_url(URL)
    headers = {'X-Token': token}
    response = requests.delete(URL + 'logout', headers=headers)

    
def count_objects(username, password, URL):
    '''
    (str, str, str) -> dict
    
    Returns a dictionary with the counts of each object with SUBMITTED status
    in EGA box defined by Username and Password
    
    Parameters
    ----------
    - username (str): Username of a given box (e.g. ega-box-xxxx)
    - password (str): Password to access the EGA box
    - URL (str): URL of the API
    '''
       
    # make a list of objects of interest
    L = ["studies", "runs", "samples", "experiments", "datasets", "analyses", "policies", "dacs"]
    # store the count of each object for the given box
    D = {}
    token = connect_to_api(username, password, URL)
    headers = {'X-Token': token}
    URL = format_url(URL)
    for i in L:
        # connect to API
        response = requests.get(URL + i + '?status=SUBMITTED&skip=0&limit=10', headers=headers)
        D[i] = response.json()['response']['numTotalResults']
        # close connection
    close_api_connection(token, URL)    
    return D


def get_upper_limit(count, chunk_size):
    '''
    (int, int) -> int
    
    Take the number of objects to download, the size of the chunck
    and returns the upper limit of the range of data chunks
    
    Parameters
    ----------
    - count (int): Number of objects to download
    - chunk_size (int): Size of each chunk of data to download at once
    '''
    
    i = count / chunk_size
    if '.' in str(i):
        num, dec = str(i)[:str(i).index('.')], str(i)[str(i).index('.'):]
    else:
        num, dec = i, 0
    if 0 < float(dec) < 0.5:
        u = round(int(num) + float(dec) + 0.5)
    elif float(dec) > 0.5:
        u = round(i)
    elif float(dec) == 0:
        u = i + 1
    return u    

    
def download_metadata(username, password, URL, ega_object, count, chunk_size):
    '''
    (str, str, str, str, dict, int) -> list
    
    Returns a list of dictionaries with all instances of ega_object, downloaded in 
    chunks of size chunk_size from URL for a given box
    
    Parameters
    ----------
    - username (str): Username of a given box (e.g. ega-box-xxxx)
    - password (str): Password to access the EGA box
    - URL (str): URL of the API
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - count (dict): Dictionary with the counts of all submitted EGA objects
    - chunk_size (int): Size of each chunk of data to download at once
    '''
    
    # format URL
    URL = format_url(URL)
        
    # collect all instances of ega_object  
    L = []
    # get the right range limit
    right = get_upper_limit(count[ega_object], chunk_size)
    # download objects in chuncks of chunk_size
    for i in range(0, right):
        # connect to API
        token = connect_to_api(username, password, URL)
        headers = {'X-Token': token}
        # download only chunk_size object, skipping the previous downloaded objects
        response = requests.get(URL + ega_object + '?status=SUBMITTED&skip={0}&limit={1}'.format(i, chunk_size), headers=headers)
        L.extend(response.json()['response']['result'])
        # close connection
        close_api_connection(token, URL)
    # make a list of accession Id
    if ega_object != 'experiments':
        accessions = [i['egaAccessionId'] for i in L]
    else:
        accessions = []
        for i in L:
            if i['egaAccessionId'] != None:
                accessions.append(i['egaAccessionId'])
            else:
                accessions.extend(i['egaAccessionIds'])
    assert len(accessions) == count[ega_object]
    return L


def relevant_info():
    '''
    (None) -> dict
    
    Returns a dictionary with fields of interest for each EGA object
    '''
    
    # map objects with relevant keys
    Info = {'studies': ['ebiId', 'alias', 'centerName', 'creationTime', 'egaAccessionId',
                        'shortName', 'status', 'studyType', 'title', 'xml', 'submitterId'],
            'samples': ['ebiId', 'alias', 'attributes', 'caseOrControl', 'centerName',
                        'creationTime', 'description', 'egaAccessionId', 'gender',
                        'phenotype', 'status', 'subjectId', 'title', 'xml', 'submitterId'],
            'experiments': ['ebiId', 'alias', 'centerName', 'creationTime', 'designDescription',
                            'egaAccessionId', 'egaAccessionIds', 'instrumentModel',
                            'instrumentPlatform', 'libraryLayout', 'libraryName', 'librarySelection',
                            'librarySource', 'libraryStrategy', 'pairedNominalLength', 
                            'status', 'title', 'xml', 'submitterId', 'sampleId', 'studyId'],
            'runs': ['ebiId', 'alias', 'centerName', 'creationTime', 'egaAccessionId', 'experimentId',
                     'files', 'runFileType', 'status', 'xml', 'submitterId', 'sampleId'],
            'analyses': ['ebiId', 'alias', 'analysisCenter', 'analysisDate', 'analysisFileType', 'analysisType',
                         'attributes', 'centerName', 'creationTime', 'description', 'egaAccessionId',
                         'files', 'platform', 'status', 'title', 'xml', 'submitterId', 'studyId'],
            'datasets': ['ebiId', 'alias', 'attributes', 'centerName', 'creationTime', 'datasetTypes',
                         'description', 'egaAccessionId', 'status', 'title', 'xml', 'submitterId', 'policyId'],
            'policies': ['ebiId', 'alias', 'centerName', 'egaAccessionId', 'title', 'policyText', 'url',
                         'status', 'creationTime', 'xml', 'submitterId', 'dacId'],
            'dacs': ['ebiId', 'alias', 'title', 'egaAccessionId', 'contacts', 'creationTime', 'submitterId']}
    return Info

    
def extract_info(metadata, ega_object):
    '''
    (list, str) -> list
    
    Returns a list of dictionaries with information of interest for ega_object
    
    Parameters
    ----------
    - metadata (list): List of dictionaries with all information downloaded for each ega_object
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    '''
    
    # get relevant Info
    info = relevant_info()[ega_object]
    # make a list of dicts with relevant info
    L = []
    for d in metadata:
        m = {}
        # loop over relevant keys
        for j in info:
            if d[j] == None:
                # ebiId is sometimes assigned to None upon submission
                # assign a random string, gets replaced once EGA updates
                if j == 'ebiId':
                    m[j] = str(uuid.uuid4())
                elif j == 'egaAccessionId':
                    # if egaAccessionId is None, it can be retrieved from the list of Ids
                    if 'egaAccessionIds' in d:
                        if type(d['egaAccessionIds']) == list:
                            m[j] = d['egaAccessionIds'][0]
                        else:
                            m[j] = d['egaAccessionIds']
                    else:
                        m[j] = d[j]
                else:
                    m[j] = d[j]
            elif type(d[j]) == list:
                if len(d[j]) == 0:
                    m[j] = None
                else:
                    m[j] = ';'.join(list(map(lambda x: str(x), d[j])))
            else:
                # convert epoch time to readabale format
                if j == 'creationTime':
                    EpochTime = int(d[j]) / 1000
                    m[j] = str(time.strftime('%Y-%m-%d', time.localtime(EpochTime)))
                # record all file names
                elif j == 'files':
                    m[j] = ';'.join([k['fileName'] for k in d['files']])
                # add egaBox
                elif j == 'submitterId':
                    m['egaBox'] = d[j]
                    m[j] = d[j]
                else:
                    m[j] = str(d[j])
        L.append(m)  
    return L


#def map_egaid_to_ebiid(credential_file, ega_object, box, URL, chunk_size):
#    '''
#    (str, str, str, str, int) -> dict
#    
#    Returns a dictionary of egaAccessionId: ebiId key, value pairs
#    for all instances of ega_object in box for which metadata is downloaded in
#    chunks of chunk_size
#    
#    Parameters
#    ----------
#    - credential_file (str): Path to the file with the database and EGA box credentials
#    - ega_object (str): Registered object at the EGA. Accepted values:
#                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
#    - box (str): EGA box (e.g. ega-box-xxx)
#    - URL (str): URL of the API Default is: "https://ega-archive.org/submission-api/v1"
#    - chunk_size (int): Size of each chunk of data to download at once
#    '''
#    
#    # get the box credentials
#    credentials = extract_credentials(credential_file)
#    # count all objects registered in box
#    counts = count_objects(box, credentials[box], URL)
#    # download all metadata for Object in chunks
#    L = download_metadata(box, credentials[box], URL, ega_object, counts, chunk_size)
#    
#    # create a dict with {egaAccessionId : ebiId}
#    D = {}
#    for i in L:
#        egaAccessionId, ebiId = i['egaAccessionId'], i['ebiId']
#        # ebiId is sometimes assigned to None upon submission
#        # assign a random string, gets replaced once EGA updates
#        if ebiId == None:
#            ebiId = str(uuid.uuid4())
#        D[egaAccessionId] = ebiId
#    return D


def map_datasets_to_runs_analyses(credentialFile, box, URL, chunk_size, dataset_metadata):
    '''
    (str, str, str, int, list) -> dict
    
    Take the dataset metadata and return a dictionary of dataset ebiId, list of runs
    and analyses ebiId key, value pairs 
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - box (str): EGA box (e.g. ega-box-xxx)
    - URL (str): URL of the API Default is: "https://ega-archive.org/submission-api/v1"
    - chunk_size (int): Size of each chunk of data to download at once
    - dataset_metadata (list): List of dictionaries with dataset metadata
    '''
    
    # ebiId could be the key in the link table
    # but for unknown reasons, many runs objects have ebiId set to None
    # uses egaAccessionId as key instead
    
    # map analyses and runs egaAccessionId to their ebiId
    #analyses = MapEgaIdToEbiId(CredentialFile, 'analyses', box, URL, chunk_size)
    #runs = MapEgaIdToEbiId(CredentialFile, 'runs', box, URL, chunk_size)
    
    D = {}
    for i in dataset_metadata:
        # get dataset Id
#        if i['ebiId'] == None:
#            datasetId = str(uuid.uuid4())
#        else:
#            datasetId = i['ebiId']
        if i['egaAccessionId'] == None:
            datasetId = str(uuid.uuid4())
        else:
            datasetId = i['egaAccessionId']
        
        # make a list of analyses ebId
        # analysisReferences are egaAccessionId
        #a = [analyses[j] for j in i['analysisReferences']]
        a = i['analysisReferences']
        # runsReferences are egaAccessionId or random string
        # some run accessions may not have a ebiId
        #r = [runs[j] for j in i['runsReferences']]
        r = i['runsReferences']
        #r = [runs[j] if j in runs else j for j in i['runsReferences']]
        assert not (len(a) == 0 and len(r) == 0)
        # make a list of analyses and runs ebiId
        D[datasetId] = a + r         
    return D


def map_analyses_to_samples(analysis_metadata):
    '''
    (list) -> dict
    
    Returns a dictionary of analysis ebiId, list of samples ebiId key, value pairs 
    
    Parameters
    ----------
    - analysis_metadata (list): List of dictionaries with analysis metadata
    '''
    
    # create a dict {analysis_ebiId: [sample_ebiId]}
    D = {}
    
    for i in analysis_metadata:
        # samples are not defined in sampleRefences list
        # but can be extracted from the xml
        tree = ET.ElementTree(ET.fromstring(i['xml']))
        sample_ref = tree.findall('.//SAMPLE_REF')
        # capture all sample IDs in a list, there mayy be more than 1 for vcf files
        accessions = [sample_ref[j].attrib['accession'] for j in range(len(sample_ref))]
        # get the analysis ebiId
        ebiId = i['ebiId']
        D[ebiId] = accessions
    return D



def specify_column_type(L):
    '''
    (list) -> str
    
    Take a list of fields for a given object and return a SQL string with 
    column name and column type
    Preconditions: all column entries are string, and first column is primary key
    
    Parameters
    ----------
    - L (list): List of fields for a given EGA object
    '''
    # all columns hold string data, add 
    cols = []
    for i in range(1, len(L)):
        if L[i] in ('title', 'description', 'designDescription'):
            if i == len(L) -1:
                cols.append(L[i] + ' TEXT NULL')
            else:
                cols.append(L[i] + ' TEXT NULL,')
        elif L[i] in ('files', 'xml', 'policyText', 'contacts', 'attributes'):
            if i == len(L) -1:
                cols.append(L[i] + ' MEDIUMTEXT NULL')
            else:
                cols.append(L[i] + ' MEDIUMTEXT NULL,')
        else:
            if i == len(L) -1:
                cols.append(L[i] + ' VARCHAR(100) NULL')
            else:
                cols.append(L[i] + ' VARCHAR(100) NULL,')
    # first column holds primary key
    cols.insert(0, L[0] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
    return ' '.join(cols)
    
  
def create_table(credential_file, ega_object, database):
    '''
    (str, str, str) -> None
    
    Creates a table for ega_object in database
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - database (str): Name of the database
    '''
    
    # Get the relevant metadata fields
    info = relevant_info()[ega_object]
    # add egaBox field
    info.append('egaBox')
    # determine column types
    columns = specify_column_type(info)
    # get table name
    table_name = ega_object.title()
    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    # create table
    cur.execute('CREATE TABLE {0} ({1})'.format(table_name, columns))
    conn.commit()
    conn.close()

    
def delete_records(credential_file, table, box, database):
    '''
    (str, str, str, str) -> None
    
    Delete rows from table in database corresponding to box
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - table (str): Name of table in database
    - box (str): EGA box (e.g. ega-box-xxx)
    - database (str): Name of the database
    '''

    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()

    # delete rows corresponding to box
    cur.execute('DELETE FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    conn.commit()
    conn.close()
    
    
def create_link_table(credential_file, ega_object, database):
    '''
    (str, str) -> None
    
    Creates link table for datasets or analyses ega_object
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - database (str): Name of the database
    '''

    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    
    # use egaAcessionId for runs and analyses Id in Datasets_RunsAnalyses junction table
    # because many datasets, runs ebiId are None
    if ega_object == 'datasets':
        cur.execute('CREATE TABLE Datasets_RunsAnalysis (datasetId VARCHAR(100), egaAccessionId VARCHAR(100), egaBox VARCHAR(100), PRIMARY KEY (datasetId, egaAccessionId))')
        conn.commit()            
    elif ega_object == 'analyses':
        cur.execute('CREATE TABLE Analyses_Samples (analysisId VARCHAR(100), sampleId  VARCHAR(100), egaBox VARCHAR(100), PRIMARY KEY (analysisId, sampleId))')
        conn.commit()
    conn.close()
    

def insert_metadata_table(credential_file, ega_object, metadata, database):
    '''
    (str, str, list, str) -> None
    
    
    Take a list of dictionaries with Objects metadata and insert it 
    into the corresponding table
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - metadata (list): List of dictionaries with relevant metadata information for ega_object
    - database (str): Name of the database
    '''

    # connect to database
    conn = connect_to_database(credential_file, database)    
    cur = conn.cursor()
    
    # get relevant metadata fields
    info = relevant_info()[ega_object]
    # add egaBox
    info.append('egaBox')
    
    # get table name
    table_name = ega_object.title()
    
    for d in metadata:
        # add values to a tuple
        values = ()
        for i in info:
            if d[i] == '' or d[i] == None:
                values = values.__add__(('NULL',))
            else:
                 values = values.__add__((d[i],))
        assert len(values) == len(info)        
        # make a string with column names
        col_names = ', '.join(info)
        # add values into table
        cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table_name, col_names, values))
        conn.commit()
    conn.close()
    

def instert_info_link_table(credential_file, table, D, box, database):
    '''
    (str, str, dict, str, str) -> None
    
    Inserts object accession IDs in D into the junction table for the given box 
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - table (str): Table in database
    - D (dict): Dictionary with map of Ids between objects
    - box (str): EGA box (e.g. ega-box-xxx)
    - database (str): Name of the database
    '''
        
    # connect to database
    conn = connect_to_database(credential_file, database)
    cur = conn.cursor()
    
    if table == 'Datasets_RunsAnalysis':
        for i in D:
            for j in D[i]:
                cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, egaAccessionId, egaBox) VALUES {0}'.format((i, j, box)))         
                conn.commit()
    elif table == 'Analyses_Samples':
        for i in D:
            # the same sample could be linked to the same study multiple times
            # remove duplicate sample Ids
            for j in list(set(D[i])):
                cur.execute('INSERT INTO Analyses_Samples (analysisId, sampleId, egaBox) VALUES {0}'.format((i, j, box)))
                conn.commit()
    conn.close()


def get_unique_records(L, ega_object):
    '''
    (list, str) -> list
    
    Returns the list of unique dictionaries with metadata.
    A single record is kept when duplicate entries with same egAccessionId   
    
    Parameters
    ----------
    - L (list): list of dictionaries with metadata downloaded from EGA for ega_object
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    '''

    D = {}
    # experiment egaAccessionId can be either egaAccessionId or egaAccessionIds
    if ega_object != 'experiments':
        for i in L:
            accession = i['egaAccessionId']
            D[accession] = i
    else:
        for i in L:
            if i['egaAccessionId'] != None:
                accession = i['egaAccessionId']
            else:
                accession = i['egaAccessionIds'][0]
            D[accession] = i
    K = [D[i] for i in D]
    return K


def collect_metadata(credential_file, box, ega_object, counts, chunk_size, URL="https://ega-archive.org/submission-api/v1", database='EGA'):
    '''
    (str, str, dict, int, str, )
    
    Dowonload the EGA object's metadata in chuncks of chunksize for a given box from
    the EGA API at URL and instert it into the EGA database 
    
    Parameters
    ----------
    - credential_file (str): Path to the file with the database and EGA box credentials
    - box (str): EGA box (e.g. ega-box-xxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - counts (dict): Counts of registered EGA objects in the given box
    - chunk_size (int): Size of each chunk of data to download at once
    - URL (str): URL of the API Default is: "https://ega-archive.org/submission-api/v1"
    - database (str): Name of the database
    '''
    
    # get the database and box credentials
    credentials = extract_credentials(credential_file)
    # process if objects exist
    if counts[ega_object] != 0:
        # download all metadata for object in chunks
        M = download_metadata(box, credentials[box], URL, ega_object, counts, chunk_size)
        print('downloaded {0} metadata from the API'.format(ega_object))
        
        # keep records with unique accessions
        L = get_unique_records(M, ega_object)
        if len(L) != len(M):
            print('removed {0} duplicate records'.format(ega_object))
               
        # extract relevant information
        metadata = extract_info(L, ega_object)
        print('collected relevant {0} information'.format(ega_object))

        # get the table name    
        table_name = ega_object.title()   
        # make a list of tables
        tables = show_tables(credential_file, database)
        if table_name not in tables:
            # create table
            create_table(credential_file, ega_object, database)
            print('created table {0}'.format(table_name))
        else:
            # update table
            delete_records(credential_file, table_name, box, database)
            print('deleted rows in table {0} for box {1}'.format(table_name, box))
        # insert data into table
        insert_metadata_table(credential_file, ega_object, metadata, database)         
        print('inserted data in table {0} for box {1}'.format(table_name, box))    
          
        # collect data to form Link Tables    
        if ega_object == 'datasets':
            # map dataset Ids to runs and analyses Ids
            D = map_datasets_to_runs_analyses(credential_file, box, URL, chunk_size, L)
            print('mapped datasets to runs and analyses Ids')
            # check if link table needs created or updated
            if 'Datasets_RunsAnalysis' not in tables:
                create_link_table(credential_file, ega_object)
                print('created Datasets_RunsAnalysis junction table')
            else:
                delete_records(credential_file, 'Datasets_RunsAnalysis', box, database)
                print('deleted rows in Datasets_RunsAnalysis junction table')
            # instert data into junction table
            instert_info_link_table(credential_file, 'Datasets_RunsAnalysis', D, box, database)
            print('inserted data in Datasets_RunsAnalysis junction table')
        elif ega_object == 'analyses':
            # map analyses Ids to sample Ids    
            D = map_analyses_to_samples(L)
            print('mapped analyses to samples Ids')
            # check if link table needs created or updated
            if 'Analyses_Samples' not in tables:
                create_link_table(credential_file, ega_object)
                print('created Analyses_Samples junction table')
            else:
                delete_records(credential_file, 'Analyses_Samples', box, database)
                print('deleted rows in Analyses_Samples junction table')
            # instert data into junction table
            instert_info_link_table(credential_file, 'Analyses_Samples', D, box, database)
            print('inserted data in Analyses_Samples junction table')


def collect_registered_metadata(credential_file, box, chunk_size, URL, metadata_database):
    '''
    (str, str, int, str, str) -> None
    
    Downloads registered metadata and adds relevant information to metadata database

    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - box (str): EGA submission box (ega-box-xxx)
    - ega_object (str): Registered object at the EGA. Accepted values:
                        studies, runs, samples, experiments, datasets, analyses, policies, dacs
    - chunk_size (int): Size of each chunk of data to download at once
    - URL (str): URL of the API to download metadata of registered objects
    - metadata_database (str): Database storing information about registered EGA objects
    '''
    
    # count all objects registered in box
    credentials = extract_credentials(credential_file)
    counts = count_objects(box, credentials[box], URL)
        
    ega_objects = ['studies', 'runs', 'samples', 'experiments', 'datasets', 'analyses', 'policies', 'dacs']
    for i in ega_objects:
        try:
            collect_metadata(credential_file, box, i, counts, chunk_size, URL, metadata_database)
        except:
            print('## ERROR ## Could not add {0} metadata for box {1} into EGA database'.format(i, box))


def parse_analysis_input_table(table):
    '''
    (str) -> list
    
    Returns a list of dictionaries, each dictionary storing the information
    for a unique analysis object
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
        
    Parameters
    ----------
    - table (str): Tab-delimited file with analysis file information
    '''
    
    # create a dict to store the information about the files
    D = {}
    
    infile = open(table)
    # get file header
    header = infile.readline().rstrip().split('\t')
    # check that required fields are present
    missing =  [i for i in ['alias', 'sampleReferences', 'filePath'] if i not in header]
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        # required fields are present, read the content of the file
        content = infile.read().rstrip().split('\n')
        for S in content:
            S = S.split('\t')
            # missing values are not permitted
            assert len(header) == len(S), 'missing values should be "" or NA'
            # extract variables from line
            if 'fileName' not in header:
                if 'analysisDate' in header:
                    L = ['alias', 'sampleReferences', 'filePath', 'analysisDate']
                    alias, sample_alias, file_path, analysis_date = [S[header.index(L[i])] for i in range(len(L))]
                else:
                    L = ['alias', 'sampleReferences', 'filePath']
                    alias, sample_alias, file_path = [S[header.index(L[i])] for i in range(len(L))]
                    analysis_date = ''
                # file name is not supplied, use filename in filepath             
                assert file_path != '/' and file_path[-1] != '/' and os.path.isdir(file_path) == False
                file_name = os.path.basename(file_path)                
            else:
                # file name is supplied, use filename
                if 'analysisDate' in header:
                    L = ['alias', 'sampleReferences', 'filePath', 'fileName', 'analysisDate']
                    alias, sample_alias, file_path, file_name, analysis_date = [S[header.index(L[i])] for i in range(len(L))]
                else:
                    L = ['alias', 'sampleReferences', 'filePath', 'fileName']
                    alias, sample_alias, file_path, file_name = [S[header.index(L[i])] for i in range(len(L))]
                    analysis_date = ''
                # check if fileName is provided for that alias
                if file_name in ['', 'NULL', 'NA']:
                    file_name = os.path.basename(file_path)
            # check if alias already recorded ( > 1 files for this alias)
            if alias not in D:
                # create inner dict, record sampleAlias and create files dict
                D[alias] = {}
                # record alias
                D[alias]['alias'] = alias
                D[alias]['analysisDate'] = analysis_date
                # record sampleAlias. multiple sample alias are allowed, eg for VCFs
                D[alias]['sampleReferences'] = [sample_alias]
                D[alias]['files'] = {}
                D[alias]['files'][file_path] = {'filePath': file_path, 'fileName': file_name}
            else:
                assert D[alias]['alias'] == alias
                # record sampleAlias
                D[alias]['sampleReferences'].append(sample_alias)
                # record file info, filepath shouldn't be recorded already 
                assert file_path not in D[alias]['files']
                D[alias]['files'][file_path] = {'filePath': file_path, 'fileName': file_name}
                     
    infile.close()

    # create list of dicts to store the info under a same alias
    # [{alias: {'sampleAlias':sampleAlias, 'files': {filePath: {attributes: key}}}}]
    L = [{alias: D[alias]} for alias in D]             
    return L        


def parse_analyses_accessory_tables(table, table_type):
    '''
    (file, str) -> dict
    
    Returns a dictionary with analysis project or attributes information
    
    Parameters
    ----------
    - table (str): File with analysis project or attributes information
    - table_type (str): Information type. Accepted values: Attributes or Projects
    '''
    
    infile = open(table)
    content = infile.read().rstrip().split('\n')
    infile.close()
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    if table_type == 'Attributes':
        expected = ['alias', 'title', 'description', 'genomeId', 'StagePath']
    elif table_type == 'Projects':
        expected = ['alias', 'analysisCenter', 'studyId', 'Broker', 'analysisTypeId', 'experimentTypeId'] 
    fields = [S.split(':')[0].strip() for S in content if ':' in S]
    missing = [i for i in expected if i not in fields]
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        for S in content:
            S = list(map(lambda x: x.strip(), S.split(':')))
            if S[0] not in ['attributes', 'units']:
                assert len(S) == 2
                D[S[0]] = S[1]
            else:
                assert len(S) == 3
                if 'attributes' not in D:
                    D['attributes'] = {}
                if S[1] not in D['attributes']:
                    D['attributes'][S[1]] = {}    
                if S[0] == 'attributes':
                    if 'tag' not in D['attributes'][S[1]]:
                        D['attributes'][S[1]]['tag'] = S[1]
                    else:
                        assert D['attributes'][S[1]]['tag'] == S[1]
                    D['attributes'][S[1]]['value'] = S[2]
                elif S[0] == 'units':
                    if 'tag' not in D['attributes'][S[1]]:
                        D['attributes'][S[1]]['tag'] = S[1]
                    else:
                        assert D['attributes'][S[1]]['tag'] == S[1]
                    D['attributes'][S[1]]['unit'] = S[2]
    infile.close()
    return D



def parse_experiment_input_table(table):
    '''
    (str) -> list 
    
    Returns a list of dictionaries, each dictionary storing the information for a
    unique experiment object.
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
    
    Parameters
    ----------
    - table (str): Tab-delimited file with experiments information
    '''
    
    # create a dict to store information about the experiments
    D = {}
    
    infile = open(table)
    # get file header
    header = infile.readline().rstrip().split('\t')
    # check that required fields are present
    missing =  [i for i in  ["sampleId", "alias", "libraryName"] if i not in header]
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        # required fields are present, read the content of the file
        content = infile.read().rstrip().split('\n')
        for S in content:
            S = S.split('\t')
            # missing values are not permitted
            assert len(header) == len(S), 'missing values should be "" or NA'
            # extract variables from line
            if "pairedNominalLength" not in header:
                length = 0
            else:
                length = S[header.index("pairedNominalLength")]
            if "pairedNominalSdev" not in header:
                sdev = 0
            else:
                sdev = S[header.index("pairedNominalSdev")]
            L = ["sampleId", "alias", "libraryName"]
            sample, alias, library  = [S[header.index(L[i])] for i in range(len(L))]
            
            assert alias not in D
            # create inner dict
            D[alias] = {'alias': alias, 'libraryName': library, 'sampleId': sample,
             'pairedNominalLength': length, 'pairedNominalSdev': sdev}
    infile.close()

    # create list of dicts to store the info under a same alias
    L = [{alias: D[alias]} for alias in D]             
    return L            
    

def parse_sample_input_table(table):
    '''
    (str) -> list
    
    Returns a list of dictionaries, each dictionary storing the information for a unique sample
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)

    Parameters
    ----------
    - table (str): Tab-delimited file with sample information
    '''
    
    # create list of dicts to store the object info {alias: {attribute: key}}
    L = []
    
    infile = open(table)
    # get file header
    header = infile.readline().rstrip().split('\t')
    # check that required fields are present
    required = ["alias", "caseOrControlId", "genderId", "phenotype", "subjectId"]
    missing = [i for i in required if i not in header]
    
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        # required fields are present, read the content of the file
        content = infile.read().rstrip().split('\n')
        for S in content:
            S = list(map(lambda x: x.strip(), S.split('\t')))
            # missing values are not permitted
            if len(header) != len(S):
                print('missing values are not permitted. Empty strings and NA are allowed')
            else:
                # create a dict to store the key: value pairs
                D = {}
                # get the alias name
                alias = S[header.index('alias')]
                D[alias] = {}
                for i in range(len(S)):
                    assert header[i] not in D[alias]
                    D[alias][header[i]] = S[i]    
                L.append(D)    
    infile.close()
    return L        


def parse_sample_attributes_table(table):
    '''
    (file) -> dict
    
    Returns a dictionary storing sample attributes
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
    
    Parameters
    ----------
    - table (str): File with sample attributes
    '''
    
    infile = open(table)
    content = infile.read().rstrip().split('\n')
    infile.close()
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    expected = ['alias', 'title', 'description']
    fields = [S.split(':')[0].strip() for S in content if ':' in S]
    missing = [i for i in expected if i not in fields]
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        for S in content:
            S = list(map(lambda x: x.strip(), S.split(':')))
            if S[0] != 'attributes':
                assert len(S) == 2
                D[S[0]] = S[1]
            else:
                assert len(S) == 3
                if 'attributes' not in D:
                    D['attributes'] = {}
                if S[1] not in D['attributes']:
                    D['attributes'][S[1]] = {}    
                if S[0] == 'attributes':
                    if 'tag' not in D['attributes'][S[1]]:
                        D['attributes'][S[1]]['tag'] = S[1]
                    else:
                        assert D['attributes'][S[1]]['tag'] == S[1]
                    D['attributes'][S[1]]['value'] = S[2]
    infile.close()
    return D


def parse_study_input_table(table):
    '''
    (str) -> dict
    
    Returns a dictionary with study information 
    
    Parameters
    ----------
    - table (str): Table with study information
    '''
    
    infile = open(table)
    content = infile.read().rstrip().split('\n')
    infile.close()
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    expected = ["alias", "studyTypeId", "title", "studyAbstract"]
    
    fields = [S.split(':')[0].strip() for S in content if ':' in S]
    missing = [i for i in expected if i not in fields]
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        for S in content:
            S = list(map(lambda x: x.strip(), S.split(':')))
            # non-attributes may contain multiple colons. need to put them back together
            if S[0] == 'attributes':
                if 'customTags' not in D:
                    D['customTags'] = []
                D['customTags'].append({'tag': str(S[1]), 'value': ':'.join([str(S[i]) for i in range(2, len(S))])})
            elif S[0] == 'pubMedIds':
                D[S[0]] = ';'.join([str(S[i]) for i in range(1, len(S))])
            else:
                D[S[0]] = ':'.join([str(S[i]) for i in range(1, len(S))])
    infile.close()
    return D


def parse_dac_input_table(table):
    '''
    (str) -> list

    Returns a list of dictionaries with key:values pairs of DAC information with
    keys in header of the table file

    Parameters
    ----------
    - table (str): Tab-delimited file with DAC information
    '''
    
    infile = open(table)
    header = infile.readline().rstrip().split('\t')
    content = infile.read().rstrip().split('\n')
    infile.close()
    # create a list [{key: value}]
    L = []
    # check that required fields are present
    expected = ["contactName", "email", "organisation", "phoneNumber", "mainContact"]
    missing = [i for i in expected if i not in header]    
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        for S in content:
            D = {}
            S = list(map(lambda x: x.strip(), S.split('\t')))
            for i in range(len(header)):
                D[header[i]] = S[i]
            L.append(D)
    return L        


def parse_run_info(table):
    '''
    (str) -> dict
    
    Return a dictionary with run information from the table file
    
    Parameters
    ----------
    - table (str): Tab-delimited file with run information
    '''
    
    # create a dict to store the information about the files
    D = {}
    
    infile = open(table)
    # get file header
    header = infile.readline().rstrip().split('\t')
    # check that required fields are present
    expected = ['alias', 'sampleId', 'experimentId', 'filePath']
    missing =  [i for i in expected if i not in header]
    if len(missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(missing)))
    else:
        # required fields are present, read the content of the file
        content = infile.read().rstrip().split('\n')
        for S in content:
            S = S.split('\t')
            # missing values are not permitted
            assert len(header) == len(S), 'missing values should be "" or NA'
            # extract variables from line
            if 'fileName' not in header:
                # upload file under the same name
                L = ['alias', 'sampleId', 'experimentId', 'filePath']
                alias, sample_alias, experimentId, file_path = [S[header.index(L[i])] for i in range(len(L))]
                assert os.path.isdir(file_path) == False
                file_name = os.path.basename(file_path)                
            else:
                # file name is supplied at least for some runs, upload as fileName if provided
                L = ['alias', 'sampleId', 'experimentId', 'filePath', 'fileName']
                alias, sample_alias, experimentId, file_path, file_name = [S[header.index(L[i])] for i in range(len(L))]
                # get fileName from path if fileName not provided for that alias
                if file_name in ['', 'NULL', 'NA']:
                    file_name = os.path.basename(file_path)
            # check if alias already recorded ( > 1 files for this alias)
            if alias not in D:
                # create inner dict, record sampleAlias and create files dict
                D[alias] = {}
                D[alias]['alias'] = alias
                D[alias]['sampleId'] = sample_alias
                D[alias]['experimentId'] = experimentId
                D[alias]['files'] = {}
                D[alias]['files'][file_path] = {'filePath': file_path, 'fileName': file_name}
            else:
                assert D[alias]['alias'] == alias
                # check that aliass is the same
                assert D[alias]['sampleId'] == sample_alias
                # record file info, filepath shouldn't be recorded already 
                assert file_path not in D[alias]['files']
                D[alias]['files'][file_path] = {'filePath': file_path, 'fileName': file_name}
    infile.close()
    return D


def add_dataset_info(credential_file, submission_database, metadata_database, table,
                   alias, policy, description, title, dataset_typeIds, accessions,
                   datasets_links, attributes, box):
    '''
    (str, str, str, str, str, str, str, str, list, str, str, str, str) -> None
   
    Adds dataset information to the Dataset Table of the EGAsub database
    Precondition: can only add infor for a single dataset at one time
    
    Parameters
    ----------    
    - credential_file (str): File with EGA boxes and database credentials
    - submission_database (str): Database storing required information for registration of EGA objects 
    - metadata_database (str): Database storing registered metadata     
    - table (str): Table with dataset information
    - alias (str): Unique identifier for the dataset
    - policy (str): Policy Id. Must start with EGAP
    - description (str): Description. Will be published on the EGA website
    - title (str): Short title. Will be published on the EGA website
    - dataset_typeIds (list): List of Dataset IDs. Controlled vocabulary available from EGA enumerations https://ega-archive.org/submission-api/v1/enums/dataset_types'
    - accessions (str): File with analyses and/or runs IDs. Entries must start with EGAR and/or EGAZ
    - datasets_links (str): Optional file with dataset URLs
    - attributes (str): Optional file with dataset attributes
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # check if accessions are valid
    infile = open(accessions)
    accessionIds = infile.read().rstrip().split('\n')
    infile.close()
    # check if accessions contains information
    if len(accessionIds) == 0:
        print('Accessions are required')
    else:
        # record dataset information only if Runs and/or Analyses accessions have been provided
        if False in list(map(lambda x: x.startswith('EGAZ') or x.startswith('EGAR'), accessionIds)):
            print('Accessions should start with EGAR or EGAZ')
    
    # check if dataset links are provided
    links = []
    valid_URLs = True
    if datasets_links:
        # check if valid file
        infile = open(datasets_links)
        for line in infile:
            if 'https' in line:
                line = line.rstrip().split()
                links.append({"label": line[0], "url": line[1]})
            else:
                valid_URLs = False
        infile.close()
        
    # check if attributes are provided
    dataset_attributes = []
    valid_attributes = True
    if attributes:
        infile = open(attributes)
        for line in infile:
            line = line.rstrip()
            if line != '':
                line = line.split('\t')
                if len(line) == 2:
                    dataset_attributes.append({"tag": line[0], "value": line[1]})
                else:
                    valid_attributes = False
        infile.close()
        
    # check if provided data is valid
    if valid_attributes and valid_URLs and len(accessionIds) != 0 and False not in list(map(lambda x: x.startswith('EGAZ') or x.startswith('EGAR'), accessionIds)):
        # create table if table doesn't exist
        tables = show_tables(credential_file, submission_database)
        # connect to submission database
        conn = connect_to_database(credential_file, submission_database)
        cur = conn.cursor()
        if table not in tables:
            fields = ["alias", "datasetTypeIds", "policyId", "runsReferences",
                      "analysisReferences", "title", "description", "datasetLinks",
                      "attributes", "Json", "submissionStatus", "errorMessages", "Receipt",
                      "CreationTime", "egaAccessionId", "egaBox", "Status"]
            # format colums with datatype
            columns = []
            for i in range(len(fields)):
                if fields[i] == 'Status':
                    columns.append(fields[i] + ' TEXT NULL')
                elif fields[i] in ['Json', 'Receipt']:
                    columns.append(fields[i] + ' MEDIUMTEXT NULL,')
                elif fields[i] in ['runsReferences', 'analysisReferences']:
                    columns.append(fields[i] + ' LONGTEXT NULL,')
                elif fields[i] == 'alias':
                    columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
                else:
                    columns.append(fields[i] + ' TEXT NULL,')
            # convert list to string    
            columns = ' '.join(columns)        
            # create table with column headers
            cur = conn.cursor()
            cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
            conn.commit()
        else:
            # get the column headers from the table
            cur.execute("SELECT * FROM {0}".format(table))
            fields = [i[0] for i in cur.description]
    
        # create a string with column headers
        column_names = ', '.join(fields)
    
        # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
        # create a dict {alias: accession}
        cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
        recorded = [i[0] for i in cur]
        # pull down dataset alias and egaId from metadata db, alias should be unique
        # create a dict {alias: accession} 
        registered = extract_accessions(credential_file, metadata_database, box, table)
    
        # check if alias is recorded or registered
        if alias in recorded:
            # skip, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
        elif alias in registered:
            # skip, already registered in EGA
            print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
        else:
            # sort Runs and Analyses Id
            runs_references = [i.strip() for i in accessionIds if i.startswith('EGAR')]
            analysis_references = [i.strip() for i in accessionIds if i.startswith('EGAZ')]
            
            # make a list of data ordered according to columns
            D = {"alias": args.alias, "datasetTypeIds": ';'.join(dataset_typeIds),
                    "policyId": policy, "runsReferences": ';'.join(runs_references),
                    "analysisReferences": ';'.join(analysis_references), "title": title,
                    "description": description, "datasetLinks": ';'.join(links),
                    "attributes": ';'.join(attributes), 'Status': 'start', 'egaBox': box}            
            # list values according to the table column order
            L = [D[field] if field in D else '' for field in fields]
            # convert data to strings, converting missing values to NULL
            values = format_data(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
            conn.commit()
        conn.close()            

 
def add_experiment_info(credential_file, submission_database, metadata_database, table,
                      information, title, study, description, instrument, selection,
                      source, strategy, protocol, library, box):
    '''
    (str, str, str, str, str, str, str, str, str, str, str, str, str, str, str) -> None
    
    Adds experiment information to the Experiment Table of the EGASUB database 
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - submission_database (str): Database storing information required for registration of EGA objects
    - metadata_database (str): Database storing registered metadata
    - table (str): Table with experiments information
    - information (str): File with library and sample information
    - title (str): Short title
    - study (str): Study alias or EGA accession Id
    - description (str): Library description
    - instrument (str): Instrument model. Controlled vocabulary from EGA
    - selection (str): Library selection. Controlled vocabulary from EGA
    - source (str): Library source. Controlled vocabulary from EGA
    - strategy (str): Library strategy. Controlled vocabulary from EGA
    - protocol (str): Library construction protocol
    - library (str): 0 for paired and 1 for single end sequencing
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    if table not in tables:
        fields = ["alias", "title", "instrumentModelId", "librarySourceId",
                  "librarySelectionId", "libraryStrategyId", "designDescription",
                  "libraryName", "libraryConstructionProtocol", "libraryLayoutId",
                  "pairedNominalLength", "pairedNominalSdev", "sampleId", "studyId",
                  "Json", "submissionStatus", "errorMessages", "Receipt", "CreationTime",
                  "egaAccessionId", "egaBox", "Status"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'Status':
                columns.append(fields[i] + ' TEXT NULL')
            elif fields[i] in ['Json', 'Receipt', 'files']:
                columns.append(fields[i] + ' MEDIUMTEXT NULL,')
            elif fields[i] == 'alias':
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    registered = extract_accessions(credential_file, metadata_database, box, table)
    
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    recorded = [i[0] for i in cur]
    
    # parse data from the input table
    data = parse_experiment_input_table(information)
    
    # record objects only if input table has been provided with required fields
    if len(data) != 0:
        # check that experiments are not already in the database for that box
        for D in data:
            # get experiment alias
            alias = list(D.keys())[0]
            if alias in registered:
                # skip already registered in EGA
                print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
            elif alias in recorded:
                # skip already recorded in submission database
                print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
            else:
                # add fields from the command
                D[alias]['title'], D[alias]['studyId']  = title, study              
                D[alias]['designDescription'], D[alias]["instrumentModelId"] = description, instrument
                D[alias]["librarySourceId"], D[alias]["librarySelectionId"] = source, selection
                D[alias]["libraryStrategyId"], D[alias]["libraryConstructionProtocol"] = strategy, protocol
                D[alias]["libraryLayoutId"], D[alias]['egaBox'] = library, box
                # set Status to start
                D[alias]["Status"] = "start"
                # list values according to the table column order
                L = [D[alias][field] if field in D[alias] else '' for field in fields]
                # convert data to strings, converting missing values to NULL                    L
                values = format_data(L)        
                cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
                conn.commit()
    conn.close()            
    
    
def add_sample_info(credential_file, metadata_database, submission_database, table, info_file, attributes, box):
    '''
    (str, str, str, str, str, str, str) -> None
    
    Adds sample information from the info_file to Sample table in the submission database
    Precondition: samples must not already be registered
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing sample information
    - info_file (str): Inout file with sample information to be added to the sample table    
    - attributes (str): Primary key in the SamplesAttributes table
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # pull down sample alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accession} 
    registered = extract_accessions(credential_file, metadata_database, box, table)
    # parse input table
    data = parse_sample_input_table(info_file)
    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    
    if table not in tables:
        fields = ["alias", "caseOrControlId", "genderId", "organismPart", "cellLine",
                  "region", "phenotype", "subjectId", "anonymizedName", "bioSampleId",
                  "sampleAge", "sampleDetail", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "AttributesKey", "Status"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'Status':
                columns.append(fields[i] + ' TEXT NULL')
            elif fields[i] in ['Json', 'Receipt', 'files']:
                columns.append(fields[i] + ' MEDIUMTEXT NULL,')
            elif fields[i] == 'alias':
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if len(data) != 0:
        # check that analyses are not already in the database for that box
        for D in data:
            # get analysis alias
            alias = list(D.keys())[0]
            if alias in registered:
                # skip analysis, already registered in EGA
                print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
            elif alias in recorded:
                # skip analysis, already recorded in submission database
                print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
            else:
                # add fields from the command
                D[alias]['AttributesKey'] = attributes
                D[alias]['egaBox'] = box 
                # add alias
                D[alias]['sampleAlias'] = alias    
                # set Status to start
                D[alias]["Status"] = "start"
                # list values according to the table column order
                L = [D[alias][field] if field in D[alias] else '' for field in fields]
                # convert data to strings, converting missing values to NULL                    L
                values = format_data(L)        
                cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
                conn.commit()
    conn.close()            



def add_sample_attributes(credential_file, metadata_database, submission_database, table, info_file, box):
    '''
    (str, str, str, str, str, str) -> None
    
    Adds sample attributes information to the SamplesAttributes Table of the EGASUBsub database
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing sample attributes information
    - info_file (str): Input file with sample attributes information
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # parse attribues table
    D = parse_sample_attributes_table(info_file)
    # create a list of tables
    tables = show_tables(credential_file, submission_database)
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    
    if table not in tables:
        fields = ["alias", "title", "description", "attributes"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'attributes':
                columns.append(fields[i] + ' MEDIUMTEXT NULL')
            elif fields[i] == "alias":
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)       
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # pull down alias from submission db. alias must be unique
    cur.execute('SELECT {0}.alias from {0}'.format(table))
    recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    required_fields = {"alias", "title", "description"}
    if required_fields.intersection(set(D.keys())) == required_fields:
        # get alias
        if D['alias'] in recorded:
            # skip sample, already recorded in submission database
            print('{0} is already recorded for in {1}'.format(D['alias'], table))
        else:
            # format attributes if present
            if 'attributes' in D:
                # format attributes
                attributes = [D['attributes'][j] for j in D['attributes']]
                attributes = ';'.join(list(map(lambda x: str(x), attributes))).replace("'", "\"")
                D['attributes'] = attributes
            # list values according to the table column order, use empty string if not present
            L = [D[field] if field in D else '' for field in fields]
            # convert data to strings, converting missing values to NULL                    L
            values = format_data(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
            conn.commit()
    conn.close()            



def add_analyses_attributes_projects(credential_file, metadata_database, submission_database, 
                                  table, info_file, data_type, box):
    '''
    (str, str, str, str, str, str, str) -> None
    
    Adds attributes information to the AnalysesAttributes or AnalysesProjects Table of the EGASUBsub database
    if alias not already present
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing sample attributes information
    - info_file (str): Input file with sample attributes information
    - data_type (str): Add Projects or Attributes info to submission database
                       Accepted values: Projects or Attributes
    - box (str): EGA submission box (ega-box-xxx)
    '''

    # parse attribues input table
    D = parse_analyses_accessory_tables(info_file, data_type)
    
    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)

    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    
    if table not in tables:
        if data_type == 'Attributes':
            fields = ["alias", "title", "description", "genomeId", "attributes", "StagePath", "platform", "chromosomeReferences"]
        elif data_type == 'Projects':
            fields = ['alias', 'analysisCenter', 'studyId', 'Broker', 'analysisTypeId',
                    'experimentTypeId', 'ProjectId', 'StudyTitle', 'StudyDesign'] 
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'chromosomeReferences' or fields[i] == 'StudyDesign':
                columns.append(fields[i] + ' MEDIUMTEXT NULL')
            elif fields[i] == 'StagePath':
                columns.append(fields[i] + ' MEDIUMTEXT NOT NULL,')
            elif fields[i] == "alias":
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)       
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # pull down alias from submission db. alias must be unique
    cur.execute('SELECT {0}.alias from {0}'.format(table))
    recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if data_type == 'Attributes':
        required_fields = {"alias", "title", "description", "genomeId", "StagePath"}
    elif data_type == 'Projects':
        required_fields = {'alias', 'analysisCenter', 'studyId', 'Broker', 'analysisTypeId', 'experimentTypeId'}
    if required_fields.intersection(set(D.keys())) == required_fields:
        # get alias
        if D['alias'] in recorded:
            # skip analysis, already recorded in submission database
            print('{0} is already recorded for in {1}'.format(D['alias'], table))
        else:
            # format attributes if present
            if 'attributes' in D:
                # format attributes
                attributes = [D['attributes'][j] for j in D['attributes']]
                attributes = ';'.join(list(map(lambda x: str(x), attributes))).replace("'", "\"")
                D['attributes'] = attributes
            # list values according to the table column order, use empty string if not present
            L = [D[field] if field in D else '' for field in fields]
            # convert data to strings, converting missing values to NULL                    L
            values = format_data(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
            conn.commit()
    conn.close()            


# use this function to add data to the analysis table
def add_analyses_info(credential_file, metadata_database, submission_database, table, info_file, projects, attributes, box):
    '''
    (str, str, str, str, str, str, str, str) -> None
    
    Adds analysis information to the Analysis Table of the EGAsub database if files are not already registered
       
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing sample attributes information
    - info_file (str): Input file with sample attributes information
    - projects (str): Primary key in the AnalysesProjects table
    - attributes (str): Primary key in the AnalysesAttributes table
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # pull down analysis alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    registered = extract_accessions(credential_file, metadata_database, box, table)
            
    # parse input table [{alias: {'sampleAlias':[sampleAlias], 'files': {filePath: {'filePath': filePath, 'fileName': fileName}}}}]
    data = parse_analysis_input_table(info_file)

    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    
    if table not in tables:
        fields = ["alias", "sampleReferences", "analysisDate",
                  "files", "WorkingDirectory", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "ProjectKey",
                  "AttributesKey", "Status"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'Status':
                columns.append(fields[i] + ' TEXT NULL')
            elif fields[i] in ['Json', 'Receipt', 'files']:
                columns.append(fields[i] + ' MEDIUMTEXT NULL,')
            elif fields[i] == 'alias':
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # pull down analysis alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if len(data) != 0:
        # check that analyses are not already in the database for that box
        for D in data:
            # get analysis alias
            alias = list(D.keys())[0]
            # double undersocre is not allowed because alias and file names are
            # retrieved from job name split on double underscore for checking upload and encryption
            if '__' in alias:
                print('double underscore is not allowed in alias bame')
            else:               
                if alias in registered:
                    # skip analysis, already registered in EGA
                    print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
                elif alias in recorded:
                    # skip analysis, already recorded in submission database
                    print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
                else:
                    # add fields from the command
                    D[alias]['ProjectKey'], D[alias]['AttributesKey'], D[alias]['egaBox'] = projects, attributes, box 
                    # check if analysisDate is provided in input table
                    if 'analysisDate' not in D[alias]:
                        D[alias]['analysisDate'] = ''
                    # add fileTypeId to each file
                    for file_path in D[alias]['files']:
                        extension, file_typeId = '', ''
                        extension = file_path[file_path.rfind('.'):].lower()
                        if extension == '.gz':
                            file_typeId = file_path[-6:].replace('.gz', '')
                        elif extension == '.tsv':
                            file_typeId = 'tab'
                        else:
                            file_typeId = extension.replace('.', '')
                        assert file_typeId in ['bam', 'bai', 'vcf', 'tab'], 'valid fileTypeId are bam, vcf, bai and tab'
                        # add fileTypeId to dict
                        assert 'fileTypeId' not in D[alias]['files'][file_path] 
                        D[alias]['files'][file_path]['fileTypeId'] = file_typeId
                    # check if multiple sample alias/Ids are used. store sample aliases/Ids as string
                    sampleIds = ';'.join(list(set(D[alias]['sampleReferences'])))
                    D[alias]["sampleReferences"] = sampleIds    
                    # set Status to start
                    D[alias]["Status"] = "start"
                    # list values according to the table column order
                    L = [D[alias][field] if field in D[alias] else '' for field in fields]
                    # convert data to strings, converting missing values to NULL                    L
                    values = format_data(L)        
                    cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
                    conn.commit()
    conn.close()            



def add_study_info(credential_file, metadata_database, submission_database, table, info_file, box):
    '''
    (str, str, str, str, str, str) -> None
       
    Adds study information into the Study Table of the EGAsub database
    
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing study information
    - info_file (str): Input file with study information
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    if table not in tables:
        fields = ["alias", "studyTypeId", "shortName", "title", "studyAbstract",
                  "ownTerm", "pubMedIds", "customTags", "Json", "submissionStatus",
                  "errorMessages", "Receipt", "CreationTime", "egaAccessionId", "egaBox",  "Status"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'Status':
                columns.append(fields[i] + ' TEXT NULL')
            elif fields[i] in ['Json', 'Receipt', 'files', 'pubMedIds', 'studyAbstract']:
                columns.append(fields[i] + ' MEDIUMTEXT NULL,')
            elif fields[i] == 'alias':
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # parse input file
    data = parse_study_input_table(info_file)
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    registered = extract_accessions(credential_file, metadata_database, box, table)
            
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if len(data) != 0:
        # get alias
        alias = data['alias']
        if alias in registered:
            # skip analysis, already registered in EGA
            print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
        elif alias in recorded:
            # skip analysis, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
        else:
            data["Status"] = "start"
            data["egaBox"] = box
            # list values according to the table column order
            L = [str(data[field]) if field in data else '' for field in fields]
            # convert data to strings, converting missing values to NULL                    L
            values = format_data(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
            conn.commit()
    conn.close()            


def add_dac_info(credential_file, metadata_database, submission_database, table, alias, info_file, title, box):
    '''
    (str, str, str, str, str, str, str, str) -> None
    
    Adds DAC information into the DAC table of the EGAsub database
    
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing policies information
    - alias (str): Alias for the DAC
    - info_file (str): File with DAC information
    - title (str): DAC title
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    
    if table not in tables:
        fields = ["alias", "title", "contacts", "Json", "submissionStatus", "errorMessages",
                  "Receipt", "CreationTime", "egaAccessionId", "egaBox",  "Status"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'Status':
                columns.append(fields[i] + ' TEXT NULL')
            elif fields[i] in ['Json', 'Receipt', 'title', 'contacts']:
                columns.append(fields[i] + ' MEDIUMTEXT NULL,')
            elif fields[i] == 'alias':
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # parse input file
    data = list(map(lambda x: str(x), parse_dac_input_table(info_file)))    
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    registered = extract_accessions(credential_file, metadata_database, box, table)
            
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if len(data) != 0:
        # check if alias is unique
        if alias in registered:
            # skip, already registered in EGA
            print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
        elif alias in recorded:
            # skip, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
        else:
            # create dict and add command line arguments
            D = {'alias': alias, 'title': title, 'contacts': ';'.join(data), 'egaBox': box, 'Status': 'start'}
            # list values according to the table column order
            L = [str(D[field]) if field in D else '' for field in fields]
            # convert data to strings, converting missing values to NULL                    L
            values = format_data(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
            conn.commit()
    conn.close()            

def add_policy_info(credential_file, metadata_database, submission_database, table,
                  alias, dacid, title, policyfile, policytext, url, box):
    '''
    (str, str, str, str, str, str, str, str, str, str, str) -> None
        
    Adds policy information into the Policies Table of the EGASUB database
    
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing policies information
    - alias (str): Alias for the Policy
    - dacid (str): DAC Id or DAC alias
    - title (str): Policy title
    - policyfile (str): File with policy text
    - policytext (str): Policy text
    - url (str): Url
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    if table not in tables:
        fields = ["alias", "dacId", "title", "policyText", "url", "Json",
                  "submissionStatus", "errorMessages", "Receipt", "CreationTime",
                  "egaAccessionId", "egaBox",  "Status"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'Status':
                columns.append(fields[i] + ' TEXT NULL')
            elif fields[i] in ['Json', 'Receipt', 'title']:
                columns.append(fields[i] + ' MEDIUMTEXT NULL,')
            elif fields[i] == 'policyText':
                columns.append(fields[i] + ' LONGTEXT NULL,')             
            elif fields[i] == 'alias':
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    registered = extract_accessions(credential_file, metadata_database, box, table)
            
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    recorded = [i[0] for i in cur]
    
    # check if alias is unique
    if alias in registered:
        # skip, already registered in EGA
        print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
    elif alias in recorded:
        # skip, already recorded in submission database
        print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
    else:
        # create a dict to store fields
        data = {}
                
        # add fields from the command
        # create dict and add command line arguments
        # get policy text from command or file
        if policyfile:
            infile = open(policyfile)
            policyText = infile.read().rstrip()
            infile.close()
        elif policytext:
            policyText = policytext
        else:
            raise ValueError('Missing policy text')
            
        if url:
            data['url'] = url
            
        data['alias'], data['dacId'], data['egaBox'] = alias, dacid, box
        data['title'], data['policyText'] = title, policyText
            
        # set status --> start
        data['Status'] = 'start'
        # list values according to the table column order
        L = [str(data[field]) if field in data else '' for field in fields]
        # convert data to strings, converting missing values to NULL
        values = format_data(L)        
        cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
        conn.commit()
    conn.close()            


def add_runs_info(credential_file, metadata_database, submission_database, table,
                info_file, file_type, stage_path, box):
    '''
    (str, str, str, str, str, str, str, str) -> None
    
    Adds runs information to the Runs Table of the EGAsub database
        
    Parameters
    ----------
    - credential_file (str): File with EGA boxes and database credentials
    - metadata_database (str): Database storing information about registered EGA objects
    - submission_database (str): Database storing required information for registration of EGA objects
    - table (str): Table storing runs information
    - info_file (str): Input file with runs information
    - file_type (str): Controlled vocabulary decribing the file type.
                       Accepted values: "One Fastq file (Single)" or "Two Fastq files (Paired)"
    - stage_path (str): Path to the directory on the staging where runs files will be uploaded
    - box (str): EGA submission box (ega-box-xxx)
    '''
    
    # create table if table doesn't exist
    tables = show_tables(credential_file, submission_database)
    # connect to submission database
    conn = connect_to_database(credential_file, submission_database)
    cur = conn.cursor()
    
    # create table if it doesn't exist
    if table not in tables:
        fields = ["alias", "sampleId", "runFileTypeId", "experimentId", "files",
                  "WorkingDirectory", "StagePath", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "Status"]
        # format colums with datatype
        columns = []
        for i in range(len(fields)):
            if fields[i] == 'Status':
                columns.append(fields[i] + ' TEXT NULL')
            elif fields[i] in ['Json', 'Receipt', 'files']:
                columns.append(fields[i] + ' MEDIUMTEXT NULL,')
            elif fields[i] == 'alias':
                columns.append(fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                columns.append(fields[i] + ' TEXT NULL,')
        # convert list to string    
        columns = ' '.join(columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(table, columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(table))
        fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    column_names = ', '.join(fields)
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    registered = extract_accessions(credential_file, metadata_database, box, table)
        
    # pull down alias from submission db. alias may be recorded but not submitted yet.
    # aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(table, box))
    recorded = [i[0] for i in cur]
    
    # parse input table [{alias: {'sampleAlias':[sampleAlias], 'files': {filePath: {'filePath': filePath, 'fileName': fileName}}}}]
    try:
        data = parse_run_info(info_file)
        # make a list of dictionary holding info for a single alias
        data = [{alias: data[alias]} for alias in data]
    except:
        data = []
        
    # record objects only if input table has been provided with required fields
    if len(data) != 0:
        # check that runs are not already in the database for that box
        for D in data:
            # get run alias
            alias = list(D.keys())[0]
            # double underscore is not allowed in alias name because alias and file name
            # are retrieved from job name split on double underscore for verificatrion of upload and encryption 
            if '__' in alias:
                print('double underscore is not allowed in alias name')
            else:
                if alias in registered:
                    # skip, already registered in EGA
                    print('{0} is already registered in box {1} under accession {2}'.format(alias, box, registered[alias]))
                elif alias in recorded:
                    # skip, already recorded in submission database
                    print('{0} is already recorded for box {1} in the submission database'.format(alias, box))
                else:
                    # add fields from the command
                    D[alias]['runFileTypeId'], D[alias]['egaBox'], D[alias]['StagePath'] = file_type, box, stage_path
                    # set Status to start
                    D[alias]["Status"] = "start"
                    # list values according to the table column order
                    L = [D[alias][field] if field in D[alias] else '' for field in fields]
                    # convert data to strings, converting missing values to NULL                    L
                    values = format_data(L)        
                    cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table, column_names, values))
                    conn.commit()
    conn.close()            



if __name__ == '__main__':

    # create top-level parser
    parent_parser = argparse.ArgumentParser(prog = 'Gaea.py', description='A tool to manage submission of genomics data to the EGA', add_help=False)
    parent_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    parent_parser.add_argument('-md', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    parent_parser.add_argument('-sd', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    parent_parser.add_argument('-b', '--Box', dest='box', choices=['ega-box-12', 'ega-box-137', 'ega-box-1269', 'ega-box-1843', 'ega-box-499'], help='Box where objects will be registered', required=True)
    
    # create main parser
    main_parser = argparse.ArgumentParser(prog = 'Gaea.py', description='manages EGA submissions')
    subparsers = main_parser.add_subparsers(title='sub-commands', description='valid sub-commands', dest= 'subparser_name', help = 'sub-commands help')

    # create info parser
    info_parser = subparsers.add_parser('add_info', help ='Add information in EGASUB tables')
    subsubparsers = info_parser.add_subparsers(title='add info sub-commands', description='valid sub-commands', dest= 'subsubparser_name', help = 'sub-commands help')

    # list files on the staging servers
    StagingServerParser = subparsers.add_parser('staging_server', help ='List file info on the staging servers', parents = [parent_parser])
    StagingServerParser.add_argument('-rt', '--RunsTable', dest='runstable', default='Runs', help='Submission database table. Default is Runs')
    StagingServerParser.add_argument('-at', '--AnalysesTable', dest='analysestable', default='Analyses', help='Submission database table. Default is Analyses')
    StagingServerParser.add_argument('-st', '--StagingTable', dest='stagingtable', default='StagingServer', help='Submission database table. Default is StagingServer')
    StagingServerParser.add_argument('-ft', '--FootprintTable', dest='footprinttable', default='FootPrint', help='Submission database table. Default is FootPrint')

    # form json with metadata and register objects through the API       
    RegisterParser = subparsers.add_parser('register', help ='Register EGA objects through the EGA API', parents = [parent_parser])
    RegisterParser.add_argument('-k', '--Keyring', dest='keyring', default='/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/PROJECTS/EGA/publickeys/public_keys.gpg', help='Path to the keys used for encryption. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/PROJECTS/EGA/publickeys/public_keys.gpg')
    RegisterParser.add_argument('-d', '--DiskSpace', dest='diskspace', default=15, type=int, help='Free disk space (in Tb) after encyption of new files. Default is 15TB')
    RegisterParser.add_argument('-f', '--FootPrint', dest='footprint', default='FootPrint', help='Database Table with footprint of registered and non-registered files. Default is Footprint')
    RegisterParser.add_argument('-w', '--WorkingDir', dest='workingdir', default='/scratch2/groups/gsi/bis/EGA_Submissions', help='Directory where subdirectories used for submissions are written. Default is /scratch2/groups/gsi/bis/EGA_Submissions')
    RegisterParser.add_argument('-mm', '--Mem', dest='memory', default='10', help='Memory allocated to encrypting files. Default is 10G')
    RegisterParser.add_argument('-mx', '--Max', dest='maxuploads', default=8, type=int, help='Maximum number of files to be uploaded at once. Default is 8')
    RegisterParser.add_argument('-mxf', '--MaxFootPrint', dest='maxfootprint', default=15, type=int, help='Maximum footprint of non-registered files on the box\'s staging sever. Default is 15Tb')
    RegisterParser.add_argument('-p', '--Portal', dest='portal', default='https://ega.crg.eu/submitterportal/v1', help='EGA submission portal. Default is https://ega.crg.eu/submitterportal/v1')
    RegisterParser.add_argument('--Remove', dest='remove', action='store_true', help='Delete encrypted and md5 files when analyses are successfully submitted. Do not delete by default')
    RegisterParser.add_argument('-sat', '--SamplesAttributesTable', dest='samples_attributes_table', default='SamplesAttributes', help='Database Table with samples attributes information. Default is SamplesAttributes')
    RegisterParser.add_argument('-aat', '--AnalysisAttributesTable', dest='analysis_attributes_table', default='AnalysesAttributes', help='Database Table with analyses attributes information. Default is AnalysesAttributes')
    RegisterParser.add_argument('-pt', '--ProjectsTable', dest='projects_table', default='AnalysesProjects', help='Database Table with analyses projects information. Default is AnalysesProjects')
        
    # check encryption
    CheckEncryptionParser = subparsers.add_parser('check_encryption', help='Check that encryption is done for a given alias', parents = [parent_parser])
    CheckEncryptionParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    CheckEncryptionParser.add_argument('-a', '--Alias', dest='alias', help='Object alias', required=True)
    CheckEncryptionParser.add_argument('-o', '--Object', dest='object', choices=['analyses', 'runs'], help='Object files to encrypt', required=True)
    CheckEncryptionParser.add_argument('-j', '--Jobs', dest='jobnames', help='Colon-separated string of job names used for encryption and md5sums of all files under a given alias', required=True)
    CheckEncryptionParser.add_argument('-w', '--WorkingDir', dest='workingdir', default='/scratch2/groups/gsi/bis/EGA_Submissions', help='Directory where subdirectories used for submissions are written. Default is /scratch2/groups/gsi/bis/EGA_Submissions')
    
    # check upload
    CheckUploadParser = subparsers.add_parser('check_upload', help='Check that files under a given alias are successfully uploaded', parents = [parent_parser])
    CheckUploadParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    CheckUploadParser.add_argument('-a', '--Alias', dest='alias', help='Object alias', required=True)
    CheckUploadParser.add_argument('-j', '--Jobs', dest='jobnames', help='Colon-separated string of job names used for uploading all files under a given alias', required=True)
    CheckUploadParser.add_argument('-o', '--Object', dest='object', choices=['analyses', 'runs'], help='EGA object to register (runs or analyses', required=True)
    CheckUploadParser.add_argument('-at', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
    
    # re-upload registered files that cannot be archived       
    ReUploadParser = subparsers.add_parser('reupload', help ='Encrypt and re-upload files that are registered but cannot be archived', parents = [parent_parser])
    ReUploadParser.add_argument('-at', '--AnalysisTable', dest='analysistable', help='Analysis Database table', default='Analyses')
    ReUploadParser.add_argument('-rt', '--RunsTable', dest='runstable', help='Runs Database table', default='Runs')
    ReUploadParser.add_argument('-a', '--Alias', dest='aliasfile', help='Two-column tab-delimited file with aliases and egaAccessionId of files that need to be re-uploaded')
    ReUploadParser.add_argument('-w', '--WorkingDir', dest='working_dir', default='/scratch2/groups/gsi/bis/EGA_Submissions', help='Directory containing sub-directories with submission information. Default is /scratch2/groups/gsi/bis/EGA_Submissions')

    # collect metadata
    CollectParser = subparsers.add_parser('collect', help ='Collect registered metadata and add relevant information in EGA database', parents = [parent_parser])
    CollectParser.add_argument('-ch', '--ChunkSize', dest='chunksize', type=int, default=500, help='Size of each chunk of data to download at once')
    CollectParser.add_argument('-u', '--URL', dest='URL', default="https://ega-archive.org/submission-api/v1", help='URL of the API to download metadata of registered objects')

    # add samples to Samples Table
    AddSamplesParser = subsubparsers.add_parser('samples', help ='Add sample information to Samples Table', parents=[parent_parser])
    AddSamplesParser.add_argument('-t', '--Table', dest='table', default='Samples', help='Samples table. Default is Samples')
    AddSamplesParser.add_argument('-a', '--Attributes', dest='attributes', help='Primary key in the SamplesAttributes table', required=True)
    AddSamplesParser.add_argument('-i', '--Info', dest='info', help='Table with sample information to load to submission database', required=True)
    
    # add sample attributes to SamplesAttributes Table
    AddSamplesAttributesParser = subsubparsers.add_parser('samples_attributes', help ='Add sample attributes information to SamplesAttributes Table', parents=[parent_parser])
    AddSamplesAttributesParser.add_argument('-t', '--Table', dest='table', default='SamplesAttributes', help='SamplesAttributes table. Default is SamplesAttributes')
    AddSamplesAttributesParser.add_argument('-i', '--Info', dest='info', help='File with sample attributes information', required=True)
    
    # add datasets to Datasets Table
    AddDatasetsParser = subsubparsers.add_parser('datasets', help ='Add datasets information to Datasets Table', parents = [parent_parser])
    AddDatasetsParser.add_argument('-t', '--Table', dest='table', default='Datasets', help='Datasets table. Default is Datasets')
    AddDatasetsParser.add_argument('-a', '--Alias', dest='alias', help='Alias for the dataset', required=True)
    AddDatasetsParser.add_argument('-p', '--Policy', dest='policy', help='Policy Id. Must start with EGAP', required=True)
    AddDatasetsParser.add_argument('-ds', '--Description', dest='description', help='Description. Will be published on the EGA website', required=True)
    AddDatasetsParser.add_argument('-tl', '--Title', dest='title', help='Short title. Will be published on the EGA website', required=True)
    AddDatasetsParser.add_argument('-di', '--DatasetId', dest='dataset_typeIds', nargs='*', help='Dataset Id. A single string or a list. Controlled vocabulary available from EGA enumerations https://ega-archive.org/submission-api/v1/enums/dataset_types', required=True)
    AddDatasetsParser.add_argument('-acs', '--Accessions', dest='accessions', help='File with analyses accession Ids. Must contains EGAR and/or EGAZ accessions. Can also be provided as a command parameter but accessions passed in a file take precedence')
    AddDatasetsParser.add_argument('-dl', '--DatasetsLinks', dest='datasets_links', help='Optional file with dataset URLs')
    AddDatasetsParser.add_argument('-at', '--Attributes', dest='attributes', help='Optional file with attributes')
    
    # add Run info to Runs Table
    AddRunsParser = subsubparsers.add_parser('runs', help ='Add run information to Runs Table', parents = [parent_parser])
    AddRunsParser.add_argument('-t', '--Table', dest='table', default='Runs', help='Run table. Default is Runs')
    AddRunsParser.add_argument('-i', '--Info', dest='information', help='Table with required run information', required=True)
    AddRunsParser.add_argument('-f', '--FileTypeId', dest='file_type', help='Controlled vocabulary decribing the file type. Example: "One Fastq file (Single)" or "Two Fastq files (Paired)"', required=True)
    AddRunsParser.add_argument('-sp', '--StagePath', dest='stage_path', help='Directory on the staging server where files are uploaded', required=True)
    
    # add experiments to Experiments Table
    AddExperimentParser = subsubparsers.add_parser('experiments', help ='Add experiments information to Experiments Table', parents = [parent_parser])
    AddExperimentParser.add_argument('-t', '--Table', dest='table', default='Experiments', help='Experiments table. Default is Experiments')
    AddExperimentParser.add_argument('-i', '--Info', dest='information', help='Table with library and sample information', required=True)
    AddExperimentParser.add_argument('-tl', '--Title', dest='title', help='Short title', required=True)
    AddExperimentParser.add_argument('-st', '--StudyId', dest='study', help='Study alias or EGA accession Id', required=True)
    AddExperimentParser.add_argument('-d', '--Description', dest='description', help='Library description', required=True)
    AddExperimentParser.add_argument('-in', '--Instrument', dest='instrument', help='Instrument model. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('-s', '--Selection', dest='selection', help='Library selection. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('-sc', '--Source', dest='source', help='Library source. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('-sg', '--Strategy', dest='strategy', help='Library strategy. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('-p', '--Protocol', dest='protocol', help='Library construction protocol.', required=True)
    AddExperimentParser.add_argument('-la', '--Layout', dest='library', help='0 for paired and 1 for single end sequencing', required=True)

    # add Policy info to Policy Table
    AddPolicyParser = subsubparsers.add_parser('policy', help ='Add Policy information to Policies Table', parents = [parent_parser])
    AddPolicyParser.add_argument('-t', '--Table', dest='table', default='Policies', help='Policy table. Default is Policies')
    AddPolicyParser.add_argument('-a', '--Alias', dest='alias', help='Alias for the Policy', required=True)
    AddPolicyParser.add_argument('-d', '--DacId', dest='dacid', help='DAC Id or DAC alias', required=True)
    AddPolicyParser.add_argument('-tl', '--Title', dest='title', help='Policy title', required=True)
    AddPolicyParser.add_argument('-pf', '--PolicyFile', dest='policyfile', help='File with policy text')
    AddPolicyParser.add_argument('-pt', '--PolicyText', dest='policytext', help='Policy text')
    AddPolicyParser.add_argument('-u', '--Url', dest='url', help='Url')
    
    # add Study in to Studies table
    AddStudyParser = subsubparsers.add_parser('study', help ='Add Study information to Studies Table', parents = [parent_parser])
    AddStudyParser.add_argument('-t', '--Table', dest='table', default='Studies', help='Studies table. Default is Studies')
    AddStudyParser.add_argument('-i', '--Input', dest='information', help='Table with required study information', required=True)

    # add DAC info to DACs Table
    AddDACsParser = subsubparsers.add_parser('dac', help ='Add DAC information to DACs Table', parents = [parent_parser])
    AddDACsParser.add_argument('-t', '--Table', dest='table', default='Dacs', help='DACs table. Default is Dacs')
    AddDACsParser.add_argument('-i', '--Info', dest='information', help='Table with contact information', required=True)
    AddDACsParser.add_argument('-a', '--Alias', dest='alias', help='Alias for the DAC', required=True)
    AddDACsParser.add_argument('-tl', '--Title', dest='title', help='Short title for the DAC', required=True)
    
    # add analyses to Analyses Table
    AddAnalysesParser = subsubparsers.add_parser('analyses', help ='Add analysis information to Analyses Table', parents = [parent_parser])
    AddAnalysesParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Analyses table. Default is Analyses')
    AddAnalysesParser.add_argument('-i', '--Info', dest='information', help='Table with analysis info to load to submission database', required=True)
    AddAnalysesParser.add_argument('-p', '--Project', dest='projects', help='Primary key in the AnalysesProjects table', required=True)
    AddAnalysesParser.add_argument('-a', '--Attributes', dest='attributes', help='Primary key in the AnalysesAttributes table', required=True)
    
    # add analyses attributes or projects to corresponding Table
    AddAttributesProjectsParser = subsubparsers.add_parser('analyses_attributes', help ='Add information to AnalysesAttributes or AnalysesProjects Tables', parents = [parent_parser])
    AddAttributesProjectsParser.add_argument('-t', '--Table', dest='table', choices = ['AnalysesAttributes', 'AnalysesProjects'], help='Database Tables AnalysesAttributes or AnalysesProjects', required=True)
    AddAttributesProjectsParser.add_argument('-i', '--Info', dest='information', help='File with attributes or projects information to load to submission database', required=True)
    AddAttributesProjectsParser.add_argument('-d', '--DataType', dest='datatype', choices=['Projects', 'Attributes'], help='Add Projects or Attributes infor to db')
    
    # get arguments from the command line
    args = main_parser.parse_args()
       
    if args.subparser_name == 'staging_server':
        file_info_staging_server(args.credential, args.metadatadb, args.subdb, args.analysestable, args.runstable, args.stagingtable, args.footprinttable, args.box)
    elif args.subparser_name == 'reupload':
        reupload_registered_files(args.credential, args.metadatadb, args.subdb, args.analysistable, args.runstable, args.working_dir, args.aliasfile, args.box)
    elif args.subparser_name == 'register':
        register_ega_objects(args.credential, args.subdb, args.metadatadb, args.workingdir, args.keyring, args.memory, args.diskspace, args.footprint, args.samples_attributes_table, args.analysis_attributes_table, args.projects_table, args.maxuploads, args.maxfootprint, args.remove, args.portal, args.box)
    elif args.subparser_name == 'check_encryption':
        check_encryption(args.credential, args.subdb, args.table, args.box, args.alias, args.object, args.jobnames, args.workingdir)
    elif args.subparser_name == 'check_upload':
        check_upload(args.object, args.credential, args.subdb, args.table, args.box, args.alias, args.jobnames, args.attributes)
    elif args.subparser_name == 'collect':
        collect_registered_metadata(args.credential, args.box, args.chunksize, args.URL, args.metadatadb)
    elif args.subparser_name == 'add_info':
        if args.subsubparser_name == 'samples':
            add_sample_info(args.credential, args.metadatadb, args.subdb, args.table, args.info, args.attributes, args.box)
        elif args.subsubparser_name == 'samples_attributes':
            add_sample_attributes(args.credential, args.metadatadb, args.subdb, args.table, args.info, args.box)
        elif args.subsubparser_name == 'datasets':
            add_dataset_info(args.credential, args.subdb, args.metadatadb, args.table, args.alias, args.policy, args.description, args.title,
                             args.dataset_typeIds, args.accessions, args.datasets_links, args.attributes, args.box)
        elif args.subsubparser_name == 'runs':
            add_runs_info(args.credential, args.metadatadb, args.subdb, args.table, args.information, args.file_type, args.stage_path, args.box)
        elif args.subsubparser_name == 'experiments':
            add_experiment_info(args.credential, args.subdb, args.metadatadb, args.table, args.information, args.title, args.study, 
                                args.description, args.instrument, args.selection, args.source, args.strategy, args.protocol, args.library, args.box)
        elif args.subsubparser_name == 'policy':
            add_policy_info(args.credential, args.metadatadb, args.subdb, args.table, args.alias, args.dacid, args.title, args.policyfile, args.policytext, args.url, args.box)
        elif args.subsubparser_name == 'study':
            add_study_info(args.credential, args.metadatadb, args.subdb, args.table, args.information, args.box)
        elif args.subsubparser_name == 'dac':
            add_dac_info(args.credential, args.metadatadb, args.subdb, args.table, args.alias, args.information, args.title, args.box)
        elif args.subsubparser_name == 'analyses':
            add_analyses_info(args.credential, args.metadatadb, args.subdb, args.table, args.information, args.projects, args.attributes, args.box)
        elif args.subsubparser_name == 'analyses_attributes':
            add_analyses_attributes_projects(args.credential, args.metadatadb, args.subdb, args.table, args.information, args.datatype, args.box)
        
