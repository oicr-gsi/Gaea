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
import pymysql



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
    
    Open a connection to the database by parsing the CredentialFile
    
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



def create_table(database, credential_file, table):
    '''
    (str, str) -> None
    
    Creates a table in database
    
    Parameters
    ----------
    - database (str): Name of the database
    - credential_file (str): Path to the file containing database and EGA box passwords
    - table (str): Table name
    '''

    column_types = ['VARCHAR(572)', 'TEXT', 'TEXT', 'VARCHAR(128)', 'INT', 'INT', 'VARCHAR(572)', 'VARCHAR(128)', 'VARCHAR(128)'],
    column_names = ['alias', 'directory', 'filepath', 'filename', 'file_size', 'run_time', 'error' 'ega-box', 'status']
                    
    # define table format including constraints    
    table_format = ', '.join(list(map(lambda x: ' '.join(x), list(zip(column_names, column_types)))))

    # connect to database
    tables  = show_tables(credential_file, database)
    if table not in tables:
        conn = connect_to_database(credential_file, database)
        cur = conn.cursor()
        # create table
        cmd = 'CREATE TABLE {0} ({1})'.format(table, table_format)
        cur.execute(cmd)
        conn.commit()
        conn.close()


# def initiate_db(database, table = 'ega_uploads'):
#     '''
#     (str) -> None
    
#     Create tables in database
    
#     Parameters
#     ----------
#     - database (str): Path to the database file
#     '''
    
#     # check if table exists
#     conn = sqlite3.connect(database)
#     cur = conn.cursor()
#     cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
#     tables = cur.fetchall()
#     tables = [i[0] for i in tables]    
#     conn.close()
    
#     if table not in tables:
#         create_table(database, table)



# def connect_to_db(database):
#     '''
#     (str) -> sqlite3.Connection
    
#     Returns a connection to SqLite database prov_report.db.
#     This database contains information extracted from FPR
    
#     Parameters
#     ----------
#     - database (str): Path to the sqlite database
#     '''
    
#     conn = sqlite3.connect(database)
#     conn.row_factory = sqlite3.Row
#     return conn


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



def insert_data(database, credential_file,  table, data, column_names):
    '''
    (str, str, str, list, list) -> None
    
    Inserts data into the database table with column names 
    
    Parameters
    ----------
    - database (str): Path to the database file
    - credential_file (str): Path to the file containing database and EGA box passwords
    - table (str): Table in database
    - data (list): List of data to be inserted
    - column_names (list): List of table column names
    '''
       
    # connect to db
    conn = connect_to_database(credential_file, database)
    # add data
    vals = '(' + ','.join(['?'] * len(data[0])) + ')'
    conn.executemany('INSERT INTO {0} {1} VALUES {2}'.format(table, tuple(column_names), vals), data)
    conn.commit()
    conn.close()


def get_column_names(database, credential_file, table):
    '''
    (str, str, str) -> list
    
    Returns a list of column headers in the database table
    
    Parameters
    ----------
    - database (str): Path to the sqlite database
    - credential_file (str): Path to the file containing database and EGA box passwords
    - table (str): Name of table in database
    '''

    conn = connect_to_database(credential_file, database)
    data = conn.execute('select * from {0}'.format(table))
    columns = list(map(lambda x: x[0], data.description))
    conn.close()

    return columns


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



def count_uploading_files(database, credential_file, table, box):
    '''
    (str, str, str) -> int
    
    Returns the number of files currently uploading for a given box
    
    Parameters
    ----------
    - database (str): Name of the database
    - credential_file (str): Path to the file containing database and EGA box passwords
    - table (str): Table storing the file information
    - box (str): EGA submission box
    '''
       
    conn = connect_to_database(credential_file, database)
    data = conn.execute('SELECT * FROM {0} WHERE box = \"{1}\" AND status = \"uploading\"'.format(table, box))
    data = list(set(data))
    conn.close()
    return len(data)




def collect_files(database, credential_file, table, box, max_upload, uploading_files):
    '''
    (str, str, str, str, int) -> list
    
    Returns a list of dictionaries of files to upload
    
    Parameters
    ----------
    - database (str): Name of the database
    - credential_file (str): Path to the file containing database and EGA box passwords
    - table (str): Name of table in database
    - box (str): Submission box
    - max_uploads (int): Maximum number of files to upload at once
    '''
    
    conn = connect_to_database(credential_file, database)
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


def update_message_status(database, credential_file, table, new_status, alias, box, file, column):
    '''
    (str, str, str, str, str, str, str, str) -> None
    
    Update the uploading status or the error message of the file with associated alias
    and box to the new status in the database table
    
    Parameters
    ----------
    - database (str): Name of the database
    - credential_file (str): Path to the file containing database and EGA box passwords
    - table (str): Name of table in database storing file information
    - new_status (str) Status of the file
    - alias (str): Unique identifier associated with the file
    - box (str): EGA submission box
    - file (str): Path to the file to be uploaded
    - column (str): Name of the column to update
    '''

    conn = connect_to_database(credential_file, database)
    conn.execute('UPDATE {0} SET {0}.{1}=\"{2}\" WHERE {0}.alias=\"{3}\" AND {0}.ega-box=\"{4}\" AND {0}.filepath = \"{5}\";'.format(table, column, new_status, alias, box, file))
    conn.commit()
    conn.close()

   
def write_qsubs(alias, file, box, password, workingdir, mem, host, database, credential_file, table):
    '''
    (str, str, str, str, str, int, str, str, str) -> None
    
    Write and launch qsubs to upload the files
        
    Parameters
    ----------
    - alias (str): Alias associated with the file
    - file (str): Path of the file to upload
    - box (str): EGA submission box (ega-box-xxx)
    - password (str): Password of the ega submission box
    - working_dir (str): Path to the workfing direcvtory containing the qsubs 
    - mem (int): Job memory requirement
    - host (str): xfer host
    - database (str): Name of the submission database
    - table (str): Name of the table storing the file information
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
    update_message_status(database, credential_file, table, 'uploading', alias, box, file, 'status')
    # update error message
    update_message_status(database, credential_file, table, 'NULL', alias, box, file, 'error')         
    
    # launch check upload job
    
    
    myscript = '/u/rjovelin/SOFT/anaconda3/bin/python3.6 /scratch2/groups/gsi/bis/rjovelin/EGA_submissions_portal/ega_upload_files.py'

    checkcmd = 'sleep 60; {0} check_upload -w {1} -b {2} -f {3} -db {4} -t {5} -a {6} -c {7}'.format(myscript, workingdir, box, file, database, table, alias, credential_file)  
     
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
        update_message_status(database, credential_file, table, 'upload', alias, box, file, 'status')
        # update error message
        update_message_status(database, credential_file, table, 'Could not launch upload jobs', alias, box, file, 'error')
 


def get_most_recent_log(logdir):
    '''
    (str) -> tuple

    Returns the most recent error and out logs in logdir if they exist

    Parameters
    ----------
    - logdir (str): Path to the log directory     
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
    (str, str) -> str
    
    Returns the content of the error and out logs
    
    Parameters
    ----------
    - outlog (str): Path to the out log
    - errorlog (str): Path to the error log
    '''
    
    infile = open(outlog)
    content_out = infile.read().rstrip()
    infile.close()
    
    infile = open(errorlog)
    content_err = infile.read().rstrip()
    infile.close()
    
    if content_err or content_out:
        content = ';'.join([content_out, content_err])
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
        for i in content:
            if 'exit_status' in i:
                exit_status = i.split()[-1]
            elif 'failed' in i:
                error_message = i.split()[-1]
    if exit_status == '0':
        error_message = ''
    return exit_status, error_message



def get_run_time(database, credential_file, table, box, file, alias):
    '''
    database, credential_file, table, box, file, alias
    
    Returns the run time allocated to the uploading job 
        
    Parameters
    ----------
    - database (str): Name of the database
    - credential_file (str): Path to the file containing database and EGA box passwords
    - table (str): Name of table in database storing file information
    - box (str): EGA submission box
    - file (str): File to upload
    - alias (str): Alias associated with the file
    '''
    
    conn = connect_to_database(credential_file, database)
    data = conn.execute('SELECT * FROM {0} WHERE ega-box=\"{1}\" AND filepath = \"{2}\" AND alias = \"{3}\"'.format(table, box, file, alias))
    assert len(data) == 1
    conn.close()
    
    runtime = int(data[0]['run_time'])
    
    return runtime



def add_file_info(args):
    '''
    (str, str, str, str) -> None

    Add file information in the database table

    Parameters
    ----------
    - files_to_upload (str): Path to the list of files to upload
    - database (str): Path to the sqlite database
    - table (str): Name of table in database storing file information
    - box (str): ega-box of interest
    - workingdir (str): Working directory where subdirctories and qsubs are written
    - credential_file (str): Path to the file containing the database and EGA passwords
    '''

    # get the files to upload
    files = get_files_to_upload(args.files_to_upload)

    # make a list of data to insert in the database
    newdata = []
       
    # get the column names
    column_names = get_column_names(args.database, args.credential_file, args.table)
       
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
    insert_data(args.database, args.credential_file, args.table, newdata, column_names)


                                 

def upload_files(args):
    '''
    (str, str, int, str, str, str, int, int) -> None
      
    Upload files to the submission box 
    
    Parameters
    ----------
    - credential_file (str): File with database credentials
    - host (str): xfer host used to upload the files
    - mem (int): Memory allocated to uploading files
    - box (str): EGA submission box
    - table (str): Table storing the file information in the database
    - database (str): Name of the database
    - max_upload (int): Maximum number of co-occuring uploads
    - quota (int): Maximum footprint allowed in the submission box
    '''
    
    # parse credentials and get password
    credentials = extract_credentials(args.credential_file)
    password = credentials[args.box]
    
    # get the files to upload
    # count the number of uploading files
    uploading_files = count_uploading_files(args.database, args.credential_file, args.table, args.box)
    files = collect_files(args.database, args.credential_file, args.table, args.box, args.max_upload, uploading_files)
    
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
                write_qsubs(alias, filepath, args.box, password, workingdir, args.mem, args.host, args.database, args.credential_file, args.table)


def check_upload_files(args):
    '''
    (str, str, str, str, str, str, str) -> None
    
    Check that file was successfully uploaded
       
    Parameters
    ----------
    - credential_file (str): File with database credentials
    - box (str): EGA submission box
    - database (str): Name of the database
    - table (str): Table storing the file information in the database
    - alias (str): Unique identifier associated with file
    - file (str): Pato the file to upload
    - workingdir (str): Path to the working directory containing logs and qsubs
    '''
    
    # set up boolean to be updated if uploading is not complete
    uploaded = True
    
    # get the log directory
    logdir = os.path.join(args.workingdir, 'qsubs/log')
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
        update_message_status(args.database, args.credential_file, args.table, 'uploaded', args.alias, args.box, args.file, 'status')
        # update error message
        update_message_status(args.database, args.credential_file, args.table, '', args.alias, args.box, args.file, 'error')
    else:
        # update status uploading -- > upload
        update_message_status(args.database, args.credential_file, args.table, 'upload', args.alias, args.box, args.file, 'status')
        # update error message
        update_message_status(args.database, args.credential_file, args.table, error_message, args.alias, args.box, args.file, 'error')
        # increase running time in hours
        runtime = get_run_time(args.database, args.credential_file, args.table, args.box, args.file, args.alias)
        new_runtime = runtime + 5
        update_message_status(args.database, args.credential_file, args.table, new_runtime, args.alias, args.box, args.file, 'run_time')







if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'ega_upload_files.py', description='A script to generate qsubs to upload files to the EGA inbox')
    subparsers = parser.add_subparsers(help='sub-command help', dest='subparser_name')
    
    # add files to database 
    file_parser = subparsers.add_parser('add_files', help="Add files info to the database")
    file_parser.add_argument('-f', '--files', dest='files', help='Path to the file containing the list of files to upload. The input file is a 2-column tab separated table with alias and file path', required=True)
    file_parser.add_argument('-d', '--database', dest='database', default = 'EGASUB', help='Name of the EGA submission database. Default is EGASUB')
    file_parser.add_argument('-w', '--workingdir', dest='workingdir', 
                             default = '/scratch2/groups/gsi/bis/EGA_Submissions',
                             help='Path to the working directory where subdirectories qnd qsubs are written. Default is /scratch2/groups/gsi/bis/EGA_Submissions')
    file_parser.add_argument('-b', '--box', dest='box', help='EGA submission box', required=True)
    file_parser.add_argument('-c', '--credential_file', dest='credential_file', help='Path to the file containing the passwords', required=True)
    file_parser.add_argument('-t', '--table', dest='table', default = 'ega_uploads', help='Table storing the file information in the database. Default is ega_uploads')
    file_parser.set_defaults(func=add_file_info)

    # upload parser
    upload_parser = subparsers.add_parser('upload_files', help="Launch jobs for uploading files")
    upload_parser.add_argument('-c', '--credential_file', dest='credential_file', help='File with database credentials', required=True)
    upload_parser.add_argument('-h', '--host', dest='host', default = 'xfer.hpc.oicr.on.ca', help='xfer host used to upload the files. Default is xfer.hpc.oicr.on.ca')
    upload_parser.add_argument('-m', '--mem', dest='mem', default='10', help='Memory allocated to uploading files. Default is 10G')
    upload_parser.add_argument('-b', '--box', dest='box', help='EGA submission box', required=True)
    upload_parser.add_argument('-t', '--table', dest='table', default = 'ega_uploads', help='Table storing the file information in the database. Default is ega_uploads')
    upload_parser.add_argument('-db', '--database', dest='database', default = 'EGASUB', help='Name of the database. Default is EGASUB')
    upload_parser.add_argument('-mx', '--max', dest='max_upload', default = 4, help='Maximum number of co-occuring uploads. Default is 4')
    upload_parser.add_argument('-q', '--quota', dest='quota', default = 8, help='Maximum footprint allowd in the submission box. Default is 8T')
    upload_parser.set_defaults(func=upload_files)


    # check upload parser
    check_parser = subparsers.add_parser('check_upload', help="Check upload succeess")
    check_parser.add_argument('-w', '--workingdir', dest='workingdir', help='Directory used for the submission and containing the qsubs and log directory', required=True)
    check_parser.add_argument('-db', '--database', dest='database', default = 'EGASUB', help='Name of the EGA submission database. Default is EGASUB')
    check_parser.add_argument('-t', '--table', dest='table', default = 'ega_uploads', help='Table storing the files for upload')
    check_parser.add_argument('-b', '--box', dest='box', help='EGA submission box', required=True)
    check_parser.add_argument('-c', '--credential_file', dest='credential_file', help='file with database credentials', required=True)
    check_parser.add_argument('-a', '--alias', dest='alias', help='Alias of the file to upload', required=True)
    check_parser.add_argument('-f', '--file', dest='file', help='Path to the file to upload', required=True)
    check_parser.set_defaults(func=check_upload_files)

    # get arguments from the command line
    args = upload_parser.parse_args()
    # pass the args to the default function
    args.func(args)
    




