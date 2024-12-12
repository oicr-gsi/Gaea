# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 12:56:22 2024

@author: rjovelin
"""


# use this script to upload files to EGA inbox




import os
import argparse
import subprocess
import uuid
import sqlite3
from datetime import datetime




import json
import time
import requests
import gzip





def create_table(database, table):
    '''
    (str, str) -> None
    
    Creates a table in database
    
    Parameters
    ----------
    - database (str): Name of the database
    - table (str): Table name
    '''

    column_types = ['VARCHAR(572)', 'TEXT', 'TEXT', 'VARCHAR(128)', 'INT', 'INT', 'VARCHAR(572)', 'VARCHAR(128)', 'VARCHAR(128)'],
    column_names = ['alias', 'directory', 'filepath', 'filename', 'file_size', 'run_time', 'error' 'ega-box', 'status']
                    
    # define table format including constraints    
    table_format = ', '.join(list(map(lambda x: ' '.join(x), list(zip(column_names, column_types)))))

    # connect to database
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    # create table
    cmd = 'CREATE TABLE {0} ({1})'.format(table, table_format)
    cur.execute(cmd)
    conn.commit()
    conn.close()


def initiate_db(database, table = 'ega_uploads'):
    '''
    (str) -> None
    
    Create tables in database
    
    Parameters
    ----------
    - database (str): Path to the database file
    '''
    
    # check if table exists
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    tables = [i[0] for i in tables]    
    conn.close()
    
    if table not in tables:
        create_table(database, table)



def connect_to_db(database):
    '''
    (str) -> sqlite3.Connection
    
    Returns a connection to SqLite database prov_report.db.
    This database contains information extracted from FPR
    
    Parameters
    ----------
    - database (str): Path to the sqlite database
    '''
    
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    return conn


def get_file_size(file):
    '''
    (str) -> int

    Returns the file size

    Parameters
    ----------
    - file (str): File path
    '''
    
    file_size = int(subprocess.check_output('ls -l {0}'.format(file), shell=True).decode('utf-8').rstrip().split()[4])
    
    return file_size



def insert_data(database, table, data, column_names):
    '''
    (str, str, list, list) -> None
    
    Inserts data into the database table with column names 
    
    Parameters
    ----------
    - database (str): Path to the database file
    - table (str): Table in database
    - data (list): List of data to be inserted
    - column_names (list): List of table column names
    '''
       
    # connect to db
    conn = sqlite3.connect(database)
    # add data
    vals = '(' + ','.join(['?'] * len(data[0])) + ')'
    conn.executemany('INSERT INTO {0} {1} VALUES {2}'.format(table, tuple(column_names), vals), data)
    conn.commit()
    conn.close()


def get_column_names(database, table):
    '''
    (str, str) -> list
    
    Returns a list of column headers in the database table
    
    Parameters
    ----------
    - database (str): Path to the sqlite database
    - table (str): Name of table in database
    '''

    conn = connect_to_db(database)
    data = conn.execute('select * from {0}'.format(table))
    columns = list(map(lambda x: x[0], data.description))
    conn.close()

    return columns



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


def get_files_to_upload(files):
    '''
    (str) -> dict

    Returns a dictionary with files to upload
    
    Parameters
    ----------
    - files (str): Path to the 2 columns, tab-separated file with alias and file paths      
    '''
    
    D = {}
    
    infile = open(files)
    for line in infile:
        line = line.rstrip()
        if line:
            line = line.split('\t')
            alias = line[0]
            file = line[1]
            if alias in D:
                D[alias].append(file)
            else:
                D[alias] = [file]
    
    infile.close()
    return D



def count_uploading_files(database, table, box):
    '''
    (str, str, str) -> int
    
    Returns the number of files currently uploading for a given box
    
    Parameters
    ----------
    - database (str):
    - table (str):
    - box (str): 
    '''
       
    conn = connect_to_db(database)
    data = conn.execute('SELECT * FROM {0} WHERE box = \"{1}\" AND status = \"uploading\"'.format(table, box))
    data = list(set(data))
    conn.close()
    return len(data)




def collect_files(database, table, box, max_upload, uploading_files):
    '''
    (str, str, str, int) -> list
    
    Returns a list of dictionaries of files to upload
    
    Parameters
    ----------
    - database (str):
    - table (str): Name of table in database
    - box (str): Submission box
    - max_uploads (int): Maximum number of files to upload at once
    '''
    
    conn = connect_to_db(database)
    data = conn.execute('SELECT * FROM {0} WHERE ega-box=\"{1}\" AND status = \"upload\"'.format(table, box))
    if data:
        m = max_upload - uploading_files
        if m < 0:
            m = 0
        data = data[:m]
    conn.close()

    return data    




def get_box_footprint(host, box, password):
    '''
    (str, str, str) -> int
    
    Returns the footprint of the data uploaded to the ega-box
    
    Parameters
    ----------
    - host (str): xfer host
    - box (str): EGA submission box
    - password (str): Password of the ega-box
    '''
    
    L = subprocess.check_output("ssh {0} \"lftp -u {1},{2} -e \\\"cd to-encrypt;ls -l;bye;\\\" sftp://inbox.ega-archive.org\"".format(host, box, password), shell=True).rstrip().decode('utf-8').split('\n')
    for i in range(len(L)):
        L[i] = L[i].split()
        L[i] = int(L[i][4])

    return sum(L)


















# def add_working_directory(credential_file, database, table, box, working_dir):
#     '''
#     (str, str, str, str, str) --> None
    
#     Create unique directories in file system for each alias in table and given Box
#     and record working directory in database table

#     Parameters
#     ----------
#     - credential_file (str): Path to the file with the database and EGA box credentials
#     - database (str): Name of the database
#     - table (str): Table name in database
#     - box (str): EGA box
#     - working_dir (str): Directory where sub-directories used for submissions are written
#     '''
    
#     # check if table exists
#     tables = show_tables(credential_file, database)
    
#     if table in tables:
#         # connect to db
#         conn = connect_to_database(credential_file, database)
#         cur = conn.cursor()
#         # get the alias with valid status
#         cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"valid\" and {0}.egaBox=\"{1}\"'.format(table, box))
#         data = cur.fetchall()
        
#         if len(data) != 0:
#             # loop over alias
#             for i in data:
#                 alias = i[0]
#                 # create working directory with random unique identifier
#                 UID = str(uuid.uuid4())             
#                 # record identifier in table, create working directory in file system
#                 cur.execute('UPDATE {0} SET {0}.WorkingDirectory=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, UID, alias, box))  
#                 conn.commit()
#                 # create working directories
#                 working_directory = get_working_directory(UID, working_dir)
#                 os.makedirs(working_directory)
#         conn.close()
        
#         # check that working directory was recorded and created
#         conn = connect_to_database(credential_file, database)
#         cur = conn.cursor()
#         # get the alias and working directory with valid status
#         cur.execute('SELECT {0}.alias, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"valid\" and {0}.egaBox=\"{1}\"'.format(table, box))
#         data = cur.fetchall()
        
#         if len(data) != 0:
#             for i in data:
#                 error = []
#                 alias = i[0]
#                 working_directory = get_working_directory(i[1], working_dir)
#                 if i[1] in ['', 'NULL', '(null)']:
#                     error.append('Working directory does not have a valid Id')
#                 if os.path.isdir(working_directory) == False:
#                     error.append('Working directory not generated')
#                 # check if error message
#                 if len(error) != 0:
#                     # error is found, record error message, keep status valid --> valid
#                     cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(table, ';'.join(error), alias, box))  
#                     conn.commit()
#                 else:
#                     # no error, update Status valid --> start
#                     cur.execute('UPDATE {0} SET {0}.Status=\"encrypt\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(table, alias, box))  
#                     conn.commit()
#         conn.close()            



def update_message_status(database, table, new_status, alias, box, file, column):
    '''
    (str, str, str, str, str, str, str) -> None
    
    Update the uploading status or the error message of the file with associated alias
    and box to the new status in the database table
    
    Parameters
    ----------
    
    
    
    '''

    conn = connect_to_database(database)
    conn.execute('UPDATE {0} SET {0}.{1}=\"{2}\" WHERE {0}.alias=\"{3}\" AND {0}.ega-box=\"{4}\" AND {0}.filepath = \"{5}\";'.format(table, column, new_status, alias, box, file))
    conn.commit()
    conn.close()



    


def write_qsubs(alias, file, box, password, workingdir, mem, host, database, table):
    '''
    
    
    Parameters
    ----------
    - alias (str): Alias associated with the file
    - file (str): Path of the file to upload
    
    
    
    
    
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
    
    if os.path.isdir(workingdir) == False:
        os.makedirs(workingdir, exist_ok=True)
    qsubdir = os.path.join(workingdir, 'qsubs')
    os.makedirs(qsubdir, exist_ok=True)
    logdir = os.path.join(qsubdir, 'log')
    os.makedirs(logdir, exist_ok=True)
    
    # create parallel lists to store the job names and exit codes
    job_exits, job_names = [], []
    
    uploadcmd = "ssh {0} \"lftp -u {1},{2} -e \\\"cd to-encrypt;mput {3};bye;\\\" sftp://inbox.ega-archive.org\""
    qsubcmd = "qsub -b y -P gsi -l h_vmem={0}g -N {1} -e {2} -o {2} \"bash {3}\""
    
    # write bash script
    filename = os.path.basename(file)
    bashscript = os.path.join(qsubdir, alias + '.' + filename + '.upload.sh')
    with open(bashscript, 'w') as newfile:
        newfile.write(uploadcmd.format(host, box, password, file))
    qsubscript = os.path.join(qsubdir, alias + '.' + filename + '.upload.qsub')
    jobname = alias + '.upload.' + filename
    myqsubcmd = qsubcmd.format(mem, jobname, logdir, bashscript)
    with open(qsubscript, 'w') as newfile:
        newfile.write(myqsubcmd)
    # launch job and collect job exit status and job name
    job = subprocess.call(myqsubcmd, shell=True)
    job_exits.append(job)
    job_names.append(jobname)
    
    # update status to uploading
    update_message_status(database, table, 'uploading', alias, box, file, 'status')
    # update error message
    update_message_status(database, table, 'NULL', alias, box, file, 'error')         
    
    # launch check upload job
    
    
    myscript = '/u/rjovelin/SOFT/anaconda3/bin/python3.6 /scratch2/groups/gsi/bis/rjovelin/EGA_submissions_portal/ega_upload_files.py'

    checkcmd = 'sleep 60; {0} check_upload -w {1} -b {2} -f {3} -d {4} -t {5}'.format(myscript, workingdir, box, file, database, table)  
     
    bashscript2 = os.path.join(qsubdir, alias + '.' + filename + '.check_upload.sh')
    with open(bashscript2, 'w') as newfile:
        newfile.write(checkcmd)
    
    jobname2 = alias + '.checkupload.' + filename 
    # launch job when previous job is done
    
    qsubcmd2 = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(job_names[-1], mem, jobname2, logdir, bashscript2)
    qsubscript2 = os.path.join(qsubdir, alias + '.' + filename + '.check_upload.qsub')
    with open(qsubscript2, 'w') as newfile:
        newfile.write(qsubcmd2)
    job = subprocess.call(qsubcmd2, shell=True)
    # store the exit code (but not the job name)
    job_exits.append(job)          
    
    # check if upload launched properly
    if not (len(set(job_exits)) == 1 and list(set(job_exits))[0] == 0):
        # record error message, reset status same uploading --> upload
        # update status uploading -- > upload
        update_message_status(database, table, 'upload', alias, box, file, 'status')
        # update error message
        update_message_status(database, table, 'Could not launch upload jobs', alias, box, file, 'error')
 


def get_most_recent_log(logdir):
    '''
    
    
    '''
    
    logfiles = [os.path.join(logdir, i) for i in os.lidtdir() if 'upload' in i]
    if logfiles:
        D = {}
        for i in logfiles:
            jobnum = int(i[i.rfind('.') + 2:])
            if jobnum in D:
                D[jobnum].append(i)
                D[jobnum].sort()
            else:
                D[jobnum] = [i]
        # sort the job numbers
        jobs = sorted(list(D.keys()))
        # get the latest upload job
        latest = jobs[-1]
        # get the latest out and error log files
        errorlog, outlog = D[latest]
        return errorlog, outlog
    else:
        return '',''
    




def check_logfiles(outlog, errorlog):
    '''
    
    
    '''
    
    infile = open(outlog)
    content_out = infile.read().rstrip()
    infile.close()
    
    infile = open(errorlog)
    content_err = infile.read().rstrip()
    infile.close()
    
    if content_err or content_out:
        content = ';',join([content_out, content_err])
    else:
        content = ''
        
    return content
        


def get_job_exit_status(jobnum):
    '''
    (int) -> str
    
    Returns the exit code and the error message of a job number after it finished running 
    ('0' indicates a normal, error-free run and '1' or another value inicates an error)
    
    Parameters
    ----------
    - jobnum (int): Job number
    '''
    
    try:
        content = subprocess.check_output('qacct -j {0}'.format(jobnum), shell=True).decode('utf-8').rstrip().split('\n')
    except:
        content = ''
            
    # check if accounting file with job has been found
    if content == '':
        exit_status = '1'
        error_message = 'cannot check job'        
    else:
        d = {}
        for i in content:
            if 'exit_status' in i:
                exit_status = i.split()[-1]
            elif 'failed' in i:
                error_message = i.split()[-1]
    if exit_status == '0':
        error_message = ''
    return exit_status, error_message



def get_run_time(database, table, box, file, alias):
    '''
    
    
    '''
    
    conn = connect_to_db(database)
    data = conn.execute('SELECT * FROM {0} WHERE ega-box=\"{1}\" AND filepath = \"{2}\" AND alias = \"{3}\"'.format(table, box, file, alias))
    assert len(data) == 1
    conn.close()
    
    runtime = int(data[0]['run_time'])
    
    return runtime



    
    
def check_upload_files(args):
    
    
    '''
    
    (workingdir, box, file, database, table)
    
    '''
    
    # set up boolean to be updated if uploading is not complete
    uploaded = True
    
    # get the log directory
    logdir = os.path.join(workingdir, 'qsubs/log')
    # get the most recent logfiles
    errorlog, outlog = get_most_recent_log(logdir)

    # check the out logs for each file
    if outlog and errorlog:
        content = check_logfiles(outlog, errorlog)
        if content:
            uploaded = False
            exit_code = '1'
            error_message = content
            
        # get the jobnumber
        jobnum = os.path.basename(outlog)
        jobnum = jobnum[jobnum.rfind('.')+2:]
        exit_code, error_message = get_job_exit_status(jobnum)
        if exit_code != '0':
            uploaded = False
    else:
        exit_code = '1'
        error_message = 'cannot check log files'
        uploaded = False
    
    if uploaded:
        # update status uploading --> uploaded
        update_message_status(database, table, 'uploaded', alias, box, file, 'status')
        # update error message
        update_message_status(database, table, '', alias, box, file, 'error')
    else:
        # update status uploading -- > upload
        update_message_status(database, table, 'upload', alias, box, file, 'status')
        # update error message
        update_message_status(database, table, error_message, alias, box, file, 'error')
        # increase running time in hours
        runtime = get_run_time(database, table, box, file, alias)
        new_runtime = runtime + 5
        update_message_status(database, table, new_runtime, alias, box, file, 'run_time')



def add_file_info(args):
    '''
    (str, str, str, str) -> None

    Add file information in the database table

    Parameters
    ----------
    - files_to_upload (str): Path to the list of files to upload
    - database (str): Path to the sqlite database
    - table (str): Name of table in database
    - box (str): ega-box of interest
    - workingdir (str): Working directory where subdirctories and qsubs are written
    '''

    # create database if it doesn't exist
    if os.path.isfile(args.database) == False:
        initiate_db(args.database, 'ega_uploads')
        
    # get the files to upload
    files = get_files_to_upload(args.files_to_upload)

    # make a list of data to insert in the database
    newdata = []
       
    # get the column names
    column_names = get_column_names(args.database, 'ega_uploads')
       
    for alias in files:
        for file in files[alias]:
            # get the file size
            assert os.path.isfile(file)
            file_size = get_file_size(file)
            filename = os.path.basename(file)
            filedir = os.path.join(args.workingdir, str(uuid.uuid4()))
            status = 'upload'
            newdata.append([alias, filedir, file, filename, file_size, args.box, status])        
        
    # add data
    insert_data(args.database, 'ega_uploads', newdata, column_names)


                                 

def upload_files(args):
    '''
    
    
    
    '''
    
    # parse credentials and get password
    credentials = extract_credentials(args.credential)
    password = credentials[args.box]
    
    # get the files to upload
    # count the number of uploading files
    uploading_files = count_uploading_files(args.database, 'ega_uploads', args.box)
    files = collect_files(args.database, 'ega_uploads', args.box, args.max_upload, uploading_files)
    
    # check if files are ready for upload
    if files:
        # get the footprint on the box
        footprint = get_box_footprint(args.host, args.box, password)
        # convert to terabytes
        footprint = footprint / 1e12
        # get the size of the data to upload
        upload_size = sum([i['file_size'] for i in files]) / 1e12
    
        # check if uploading is below the allowed quota
        if footprint + upload_size < args.quota:
            # can upload. write qsubs. launch jobs
            for i in files:
                alias = i['alias']
                workingdir = i['workingdir']
                filepath = i['filepath']
                write_qsubs(alias, filepath, args.box, password, workingdir, args.mem, args.host, args.database, 'ega_uploads')








if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'ega_upload_files.py', description='A script to generate qsubs to upload files to the EGA inbox')
    subparsers = parser.add_subparsers(help='sub-command help', dest='subparser_name')
    
    # add files to database 
    file_parser = subparsers.add_parser('add_files', help="Add files info to the database")
    file_parser.add_argument('-f', '--files', dest='files', help='Path to the file containing the list of files to upload. The input file is a 2-column tab separated table with alias and file path', required=True)
    file_parser.add_argument('-d', '--database', dest='database',
                             default = '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/PROJECTS/EGA/EGA_uploads.db',
                             help='Path to the sqlite database storing file information. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/PROJECTS/EGA/EGA_uploads.db')
    file_parser.add_argument('-w', '--workingdir', dest='workingdir', 
                             default = '/scratch2/groups/gsi/bis/EGA_Submissions',
                             help='Path to the working directory where subdirectories qnd qsubs are written. Default is /scratch2/groups/gsi/bis/EGA_Submissions')
    file_parser.add_argument('-b', '--Box', dest='box', help='EGA submission box', required=True)
    file_parser.set_defaults(func=add_file_info)

    # create top-level parser
    upload_parser = argparse.ArgumentParser(prog = 'ega_upload_files.py', description='A script to generate qsubs to upload files to the EGA inbox', add_help=False)
    upload_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    upload_parser.add_argument('-f', '--Files', dest='files', help='Path to the file containing the list of files to upload. The input file is a 2-column tab separated table with alias and file path', required=True)
    upload_parser.add_argument('-h', '--Host', dest='host', default = 'xfer.hpc.oicr.on.ca', help='xfer host used to upload the files. Default is xfer.hpc.oicr.on.ca')
    upload_parser.add_argument('-w', '--WorkingDir', dest='workingdir', help='Directory used for the submission and containing the qsubs and log directory', required=True)
    upload_parser.add_argument('-m', '--Mem', dest='mem', default='10', help='Memory allocated to uploading files. Default is 10G')
    upload_parser.add_argument('-b', '--Box', dest='box', help='EGA submission box', required=True)
    upload_parser.set_defaults(func=upload_files)


    # get arguments from the command line
    args = upload_parser.parse_args()
    # pass the args to the default function
    args.func(args)
    




    
