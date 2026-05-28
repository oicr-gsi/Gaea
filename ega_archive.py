# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 16:41:39 2026

@author: rjovelin
"""


import argparse
import os
import sys
import subprocess
import time
import json
import requests


def is_sequencing(workflow):
    '''
    (str) -> bool
    
    Returns True if workflow is a fastq generating workflow
    
    Parameters
    ----------
    - workflow (str): Name of the workflow
    '''

    sequencing_workflows = ['casava', 'bcl2fastq', 'fileimportforanalysis', 'fileimport', 'import_fastq']
    
    return workflow.lower() in sequencing_workflows
    

def is_call_ready(workflow):
    '''
    (str) -> bool
    
    Returns True if workflow is a call ready workflow (star of bmpp)
       
    Parameters
    ----------
    - workflow (str): Name of the workflow
    '''
        
    return 'star_call_ready' in workflow.lower() or 'bammergepreprocessing' in workflow.lower()


def is_qc(workflow):
    '''
    (str) -> bool
    
    Returns True if workflow is a QC workflow 
       
    Parameters
    ----------
    - workflow (str): Name of the workflow
    '''
    
    return 'qc' in workflow.lower() or 'callability' in workflow or \
        'metrics' in workflow.lower() or 'contamination' in workflow.lower() or \
        'collector' in workflow.lower() or 'bcl2barcode' in workflow.lower() or \
        'tmbanalysis' in workflow.lower() or 'fingerprint' in workflow.lower()
    

def define_workflow_type(workflow):
    '''
    (str) -> str
    
    Returns the category in which the workflow needs to be archived (qc, analysis, callready or fastq)
    
    Parameters
    ----------
    - workflow (str): Name of the workflow
    '''
    
    if is_sequencing(workflow):
        workflow_type = 'fastq'
    elif is_call_ready(workflow):
        workflow_type = 'callready'
    elif is_qc(workflow):
        workflow_type = 'qc'
    else:
        workflow_type = 'analysis'
    
    return workflow_type



def load_data(provenance_data_file):
    '''
    (str) -> list
    
    Returns the list of data contained in the provenance_data_file
    
    Parameters
    ----------
    - provenance_data_file (str): Path to the file with production data extracted from Shesmu
    '''

    infile = open(provenance_data_file, encoding='utf-8')
    provenance_data = json.load(infile)
    infile.close()
    
    return provenance_data


def is_case_info_incomplete(case_data):
    '''
    (dict) -> bool
    
    Returns True if the case information is complete
    
    Parameters
    ----------
    - case_data (dict): Dictionary with case information from production
    '''
    
    incomplete = [len(case_data[i]) == 0 for i in case_data]
    return any(incomplete)


def clean_up_provenance(provenance_data):
    '''
    (list) -> list, list
    
    Returns a list of dictionaries removing cases for which some information is not defined
    
    Parameters
    ----------
    - provenance_data (list): List of dictionaries with production data for cases
    '''    
    
    to_remove = [i for i in provenance_data if is_case_info_incomplete(i)]
    for i in to_remove:
        provenance_data.remove(i)
    
    return provenance_data, to_remove



def extract_file_info(case_data, datatype):
    '''
    (dict, list) -> dict
    
    Returns a dictionary with file information for each file of a case
    
    Parameters
    ----------
    - case_data (dict): Dictionary with case production data
    - datatype (list): List of data to include. Choices include fastq, callready, analysis
    '''

    D = {}
    
    projects = [{i['project']: i['deliverables']} for i in case_data['project_info']]
    
    for d in case_data['workflow_runs']:
        files = json.loads(d['files'])
        lims = d['limsIds'].split(',')
        workflow = d['wf']
        # identify the type of workflow (qc, fastq, call ready or analysis)
        workflow_type = define_workflow_type(workflow)
        # check datatype to include
        if workflow_type in datatype:
            wfrun_id = d['wfrunid']
            version = d['wfv']
            for k in files:
                file = k['path']
                # make sure the file exists
                if os.path.isfile(file):
                    deleted = 'NO'
                else:
                    deleted = 'YES'
                md5sum = k['md5']
                accession = k['accession']
                file_attributes = json.loads(k['file_attributes'])
                assert file not in D
                D[file] = {'lims': lims, 'workflow': workflow, 'wfrun_id': wfrun_id,
                           'version': version, 'md5sum': md5sum, 'accession': accession,
                           'attributes': file_attributes, 'case_id': case_data['case'],
                           'project': projects, 'workflow_type': workflow_type, 'deleted': deleted}
                
    return D                   



def extract_sample_info(case_data):
    '''
    (dict) -> dict
    
    Returns a dictionary with sample information for each lims_id of a case
    
    Parameters
    ----------
    - case_data (dict): Dictionary with case production data
    '''
        
    D = {}
    
    for d in case_data['sample_info']:
        lims_id = d['limsId']
        barcode = d['barcode']
        donor = d['donor']
        external_id = d['externalId']
        if d['groupId']:
            group_id = d['groupId']
        else:
            group_id = 'NA'
        if d['groupDesc']:
            group_description = d['groupDesc']
        else:
            group_description = 'NA'
        lane = d['lane']
        library = d['library']
        library_design = d['libraryDesign']
        run = d['run']
        sample_id = d['sampleId']
        tissue_origin = d['tissueOrigin']
        tissue_type = d['tissueType']
        instrument = d['instrument']
        
        assert lims_id not in D
        D[lims_id] = {'lims_id': lims_id, 'barcode': barcode, 'donor': donor,
                      'external_id': external_id, 'group_id': group_id,
                      'group_description': group_description, 'lane': lane,
                      'library': library, 'library_design': library_design,
                      'run': run, 'sample_id': sample_id, 'tissue_origin': tissue_origin,
                      'tissue_type': tissue_type, 'instrument': instrument}
            
    return D    


def add_sample_info(file_info, sample_info):
    '''
    (dict, dict) -> dict
    
    Returns a dictionary with file information including the corresponding sample
    information for all files in a case
    
    Parameters
    ----------
    - file_info (dict): Dictionary with information about all files in case
    - sample_info (dict): Dictionarty with information about all samples in case
    '''
    
    for file in file_info:
        for limsid in file_info[file]['lims']:
            assert limsid in sample_info
            if 'samples'  not in file_info[file]:
                file_info[file]['samples'] = [sample_info[limsid]] 
            else:
                if sample_info[limsid] not in file_info[file]['samples']:
                    file_info[file]['samples'].append(sample_info[limsid])
            
    return file_info            
 


def extract_project_data(provenance_data_file, project, datatype, valid_cases):
    '''
    (str, str, list, list | None) -> dict
  
    Returns a dictionary with file info extracted from the provenance_reporter json from FPR for a given project
    and given donors if specified
                
    Parameters
    ----------
    - provenance (str): Path to File Provenance Report
    - project (str): Project name as it appears in File Provenance Report. 
    - datatype (list): List of data to include. Choices include fastq, callready, analysis
    - valid_cases (list | None): List of cases to include
    '''


    # load data from file
    provenance_data = load_data(provenance_data_file)
    print('loaded data')
    # clean up data
    provenance_data, deleted_cases = clean_up_provenance(provenance_data)
    print('removed {0} incomplete cases'.format(len(deleted_cases)))
    
    
    D = {}
        
    for case_data in provenance_data:
        case_id = case_data['case']
        # check that case belong to specified project
        if case_data['projects'] == project:
            # check if case is specified
            if valid_cases and case_id not in valid_cases:
                print('{0} is not in the list of provided cases')
            else:
                # extract file info, sample info and map samples to files
                file_info = extract_file_info(case_data, datatype)
                sample_info = extract_sample_info(case_data)
                file_info = add_sample_info(file_info, sample_info)
                # update dict 
                if file_info:
                    assert case_id not in D
                    D[case_id] = file_info
                
    return D                  
    
       
def write_manifest(data, project, projectdir):
    '''
    (dict, str, str) -> None
    
    Parameters
    ----------
    - data (dict): Dictionary with file information for a given project extracted from FPR
    - project (str): Name of project oif interest
    - projectdir (str): Project directory where data is organized
    '''

    header = ['workflow_run_id',
              'workflow',
              'case',
              'donor',
              'file_path',
              'file_name',
              'file_swid',
              'md5',
              'platform',
              'external_name',
              'sample_id',
              'limskey',
              'groupid',
              'groupdesc',
              'library',
              'library_source',
              'tissue_type',
              'tissue_origin',
              'deleted']

    current_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    manifest = os.path.join(projectdir, '{0}.MANIFEST.{1}.txt'.format(project, current_time))
    newfile = open(manifest, 'w') 
    newfile.write('\t'.join(header) + '\n')                    
    
    for case_id in data:
        for file in data[case_id]:
            donor = ';'.join(sorted(list(set([i['donor'] for i in data[case_id][file]['samples']]))))
            instrument = ';'.join(sorted(list(set([i['instrument'] for i in data[case_id][file]['samples']]))))
            external_name = ';'.join(sorted(list(set([i['external_id'] for i in data[case_id][file]['samples']]))))
            samples = ';'.join(sorted(list(set([i['sample_id'] for i in data[case_id][file]['samples']]))))
            limskeys = ';'.join(sorted(list(set([i['lims_id'] for i in data[case_id][file]['samples']]))))
            group_id = ';'.join(sorted(list(set([i['group_id'] for i in data[case_id][file]['samples']]))))
            group_description = ';'.join(sorted(list(set([i['group_description'] for i in data[case_id][file]['samples']]))))
            library = ';'.join(sorted(list(set([i['library'] for i in data[case_id][file]['samples']]))))
            library_source = ';'.join(sorted(list(set([i['library_design'] for i in data[case_id][file]['samples']]))))
            tissue_type = ';'.join(sorted(list(set([i['tissue_type'] for i in data[case_id][file]['samples']]))))
            tissue_origin = ';'.join(sorted(list(set([i['tissue_origin'] for i in data[case_id][file]['samples']]))))
               
            L = [data[case_id][file]['wfrun_id'],
                 data[case_id][file]['workflow'],
                 case_id,
                 donor,
                 file,
                 os.path.basename(file),
                 data[case_id][file]['accession'],         
                 data[case_id][file]['md5sum'],   
                 instrument,
                 external_name, 
                 samples,
                 limskeys,
                 group_id, 
                 group_description, 
                 library,
                 library_source,
                 tissue_type,
                 tissue_origin,
                 data[case_id][file]['deleted']]
                 
                
            newfile.write('\t'.join(L) + '\n')
    
    newfile.close()    
    
    

def link_files(data, stagedir):
    '''
    (dict, str) -> None
    
    Link files of a given project in a specific data structure:
                
        case --|
                | datatype --|
                             | workflow_id -- |
                                              | files
    Parameters
    ----------
    - data (dict): Dictionary with file information for a given project
    - stagedir (str): Directory where data is organized
    '''
    
    for case_id in data:
        for file in data[case_id]:
            if data[case_id][file]['deleted'] == 'NO':
                assert os.path.isfile(file)
                if ' ' in case_id:
                    case_id = case_id.replace(' ', '_')
                wfrunid = data[case_id][file]['wfrun_id']
                workflow_type = data[case_id][file]['workflow_type']
                # create donor directory
                casedir = os.path.join(stagedir, case_id)
                os.makedirs(casedir, exist_ok=True)
                # organize data by fastq, call ready and analysis
                datatypedir = os.path.join(casedir, workflow_type)
                os.makedirs(datatypedir, exist_ok=True)
                # keep only the alphanumerical string of the workflow run id
                wfrundir = os.path.join(datatypedir, os.path.basename(wfrunid))
                os.makedirs(wfrundir, exist_ok=True)
                # create link
                filename = os.path.basename(file)
                link = os.path.join(wfrundir, filename)
                if os.path.isfile(link) == False:
                    os.symlink(file, link)



def ticket_format(d):
    '''
    (dict) -> list
    
    Returns a list of tickets associated with release
    
    Parameters
    ----------
    - d (dict): Dictionary extracted from nabu for a specific case
    '''
    
    comment = d['comment']
    if comment and comment.startswith('G') and '-' in comment:
        comment = comment.split('-')
        c = ['-'.join([comment[0], comment[i]]) for i in range(1, len(comment))]
    else:
        if comment:
            c = [d['comment']]
        else:
            c = d['comment']
    
    return c



def extract_nabu_signoff(cases, nabu_key_file, nabu='https://nabu.gsi.oicr.on.ca/case/sign-off'):
    '''
    (list, str, str) -> dict
    
    Returns a dictionary of signoffs for each case in cases
        
    Parameters
    ----------
    - cases (list): List of case identifiers
    - nabu_key_file (str): File storing the nabu API key
    - nabu (str): URL to access the signoffs in Nabu
    '''
    
    infile = open(nabu_key_file)
    nabu_key = infile.read().rstrip()
    infile.close()
    
    headers = {'accept': 'application/json',
               'X-API-KEY': nabu_key,}
    
    D = {}
    
    response = requests.get(nabu, headers=headers)
    if response.status_code == 200:
        for d in response.json():
            case_id = d['caseIdentifier']
            if case_id in cases:
                ticket = ticket_format(d)
                d['comment'] = ticket
                if case_id not in D:
                    D[case_id] = {}
                step = d['signoffStepName']
                step = ' '.join(list(map(lambda x: x.lower().capitalize(), step.split('_'))))
                if step in D[case_id]:
                    D[case_id][step].append(d)
                else:
                    D[case_id][step] = [d]
    return D




def keep_signoffed_cases(cases, nabu_key_file, nabu='https://nabu.gsi.oicr.on.ca/case/sign-off'):
    '''
    (list, str, str) -> lisr
    
    Returns a list of case identifiers for which release signoff (except EGA) has been completed
    
    Parameters
    ----------
    - cases (list): List of case identifiers
    - nabu_key_file (str): File storing the nabu API key
    - nabu (str): URL to access the signoffs in Nabu
    '''
    
    signoffs = extract_nabu_signoff(cases, nabu_key_file, nabu='https://nabu.gsi.oicr.on.ca/case/sign-off')

    keep = []

    for case_id in signoffs:
        if case_id in signoffs:
            if 'Release' in signoffs[case_id]:
                L = []        
                for d in signoffs[case_id]['Release']:
                    if 'fastq' in d['deliverable'].lower() or 'pipeline' in d['deliverable'].lower():
                        L.append(d['qcPassed'])
                if all(L):
                    keep.append(case_id)
                        
    return keep


def organize_data(args):
    '''
    (str, str, str, list | None, str | None) -> None
    
    Extract project data from FPR and organize links in the EGA stage directory
    
    Parameters
    ----------
    - ega_stage (str): Directory where the links are organized
    - provenance (str): Path to the provenance_reporter.json
    - project (str): Project of interest
    - cases (list | None): List of cases
    - casefile (str | None): File with list of cases
    - signoff_only (bool): Keep only cases with complete release signoff if True
    - nabu (str): Nabu case signoff endpoint
    - nabu_key_file (str): Path to the nabu key file
    - datatype (list | None): Restrict the data to sequences, analysis and/or call ready bams.
                              Choices are: 'fastqs' and/or 'callready' and/or 'analysis'
    '''

    # check options
    if args.cases and args.casefile:
        sys.exit('-c and -cf are mutually exclusive')

    # create project dir
    projectdir = os.path.join(args.ega_stage, args.project)
    os.makedirs(projectdir, exist_ok=True)
    # create directory where to link the files
    stagedir = os.path.join(projectdir, 'stage_folders')
    os.makedirs(stagedir, exist_ok=True)
    
    # make a list of valid cases
    if args.cases:
        valid_cases = args.cases
    elif args.casefile:
        infile = open(args.casefile)
        valid_cases = infile.read().rstrip().split('\n')
        infile.close()
    else:
        valid_cases = []
        
    # extract data
    if args.datatype:
        # restrict the data to the type of workflows included (fastq, analysis, callready)
        data_type = args.datatype
    else:
        # include all data
        data_type = ['fastq', 'analysis', 'callready']
    print('Includes {0} data'.format(', '.join(data_type)))    
     
    data = extract_project_data(args.provenance, args.project, data_type, valid_cases)
    # count files
    file_counts = []
    for case_id in data:
        file_counts.extend(list(data[case_id].keys()))
    file_counts = len(list(set(file_counts)))
    print('extracted {0} files for {1} cases for project {2}'.format(file_counts, len(data), args.project))
    
    if data:
        # check if only cases with release signoff should be kept
        if args.signoff_only:
            print('keeping only cases with release signoff')
            # make a list of cases to keep
            keep_cases = keep_signoffed_cases(list(data.keys()), args.nabu_key_file, args.nabu)
            print('cases with release signoff: {0}'.format(len(keep_cases)))
            print('discarding {0} cases'.format(len(data) - len(keep_cases)))
            # remove cases without signoff
            to_remove = [i for i in data if i not in keep_cases]
            for i in to_remove:
                del data[i]
        if data:
            # link files if files have been deleted
            link_files(data, stagedir)
            print('linked data to {0}'.format(stagedir))
            # write manifest with file information
            write_manifest(data, args.project, projectdir)
            print('wrote manifest in {0}'.format(projectdir))
   
    
   
def encrypt_folder(folder, case_id, gsi_age_key, it_age_key, archivedir, qsubdir, logdir, memory, runtime):
    '''
    (str, str, str, str, str, str, str, int, int)    
    
    Write and launch jobs to tar and encrypt the linked donor data
        
    Parameters
    ----------
    - folder (str): Directory with linked data to data and encrypt
    - case_id (str): Case identifier
    - gsi_age_key (str): GSI age public encrytion key
    - it_age_key (str): IT age public encryption key
    - archivedir (str): Output directory where the encrypted tarball is written
    - qsubdir (str): Directory where asub and bash scripts are written
    - logdir (str): Directory where logs are written
    - memory (int): Job memory
    - runtime (int): Job run time in hours
    '''
    
    
    encryptcmd = "module load ega-archive; tar -cvhz -C {0} {1} | age -r {2} -r {3} > {4}"
    qsubcmd = "qsub -cwd -b y -P gsi -l h_vmem={0}g,h_rt={1}:0:0 -N {2} -e {3} -o {3} \"bash {4}\""
    # age output: encrypted tarball
    encrypted_file = os.path.join(archivedir, '{0}.tar.gz.age'.format(case_id))
    # get the encryption command
    parent_folder = os.path.dirname(folder)
    foldername = os.path.basename(folder)
    myencryptcmd = encryptcmd.format(parent_folder, foldername, gsi_age_key, it_age_key, encrypted_file)
    # write bash and qsub scripts
    bashscript = os.path.join(qsubdir, '{0}.encrypt.sh'.format(case_id))
    with open(bashscript, 'w') as newfile:
        newfile.write(myencryptcmd)
    qsubscript = os.path.join(qsubdir, '{0}.encrypt.qsub'.format(case_id))
    myqsubcmd = qsubcmd.format(memory, runtime, '{0}.encrypt'.format(case_id), logdir, bashscript)
    with open(qsubscript, 'w') as newfile:
        newfile.write(myqsubcmd)
    # launch job 
    subprocess.call(myqsubcmd, shell=True)

    
def encrypt_file(file, gsi_age_key, it_age_key, archivedir, qsubdir, logdir, memory, runtime):
    '''     
    (str, str, str, str, str, str, int, int) -> None
        
    Write and launch jobs to encrypt a single file
        
    Parameters
    ----------
    - file (str): File to encrypt
    - gsi_age_key (str): GSI age public encrytion key
    - it_age_key (str): IT age public encryption key
    - archivedir (str): Output directory where the encrypted file is written
    - qsubdir (str): Directory where qsub script is written
    - logdir (str): Directory where logs are written
    - memory (int): Job memory
    - runtime (int): Job run time in hours
    '''
    
    encryptcmd = "module load ega-archive; age -r {1} -r {2} > {3}"
    qsubcmd = "qsub -cwd -b y -P gsi -l h_vmem={0}g,h_rt={1}:0:0 -N {2} -e {3} -o {3} \"{4}\""
    
    filename = os.path.basename(file) 
    
    # age output: encrypted tarball
    encrypted_file = os.path.join(archivedir, filename + '.age')
    # get the encryption command
    myencryptcmd = encryptcmd.format(gsi_age_key, it_age_key, encrypted_file)
    
    qsubscript = os.path.join(qsubdir, '{0}.encrypt.qsub'.format(filename))
    myqsubcmd = qsubcmd.format(memory, runtime, '{0}.encrypt'.format(filename), logdir, myencryptcmd)
    with open(qsubscript, 'w') as newfile:
        newfile.write(myqsubcmd)
    # launch job 
    subprocess.call(myqsubcmd, shell=True)




def decrypt_file(encrypted_file, age_key, outputdir):
    '''     
    (str, str, str) -> None
        
    Decrypt a single file
        
    Parameters
    ----------
    - encrypted_file (str): File to decrypt
    - age_key (str): age decrypting secret key
    - outputdir (str): Path to the directory where decrypted files are written
    '''
    
    decryptcmd = "age --decrypt -i {0} -o {1} {2}"
        
    # get outputfile
    filename = os.path.basename(encrypted_file) 
    filename = filename.replace('.age', '')
    decrypted_file = os.path.join(outputdir, filename)
    # get the encryption command
    mydecryptcmd = decryptcmd.format(age_key, decrypted_file, encrypted_file)
    subprocess.call(mydecryptcmd, shell=True)


def encrypt_data(args):
    '''
    (str, str, str, str, str, str, str, list | None, str | None, int, int) -> None
    
    Encrypt data (single file, single folder or arcive with subfolders)
        
    Parameters
    ----------
    - project (str): Name of project of interest
    - ega_stage (str): Directory where the links are organized
    - file (str): Path to the file to encrypt
    - directory (str): Path to the directory to tar and encrypt
    - archive (str): Path to the directory containing subfolders to with linked donor data to tar and encrypt
    - gsi_age_pub_key (str): Path to the GSI age public key
    - it_age_pub_key (str): Path to the IT age public key
    - cases (list | None): List of donors
    - casefile (str | None): File with list of donors
    - memory (int): Encryption job memory. Default is 20G
    - runtime (in): Encryption job runtime
    '''
    
    # check options
    if args.casefile and args.cases:
        sys.exit('-cf and -c are mutually exclusive')
    if args.file:
        a = [args.directory, args.archive, args.cases, args.casefile]
        if any(a):
            c = ['-d', '-a', '-c', '-cf']
            err = ','.join([c[i] for i in range(len(c)) if a[i]])
            sys.exit('-f cannot be used with options {0}'.format(err))
    elif args.directory:
        a = [args.file, args.archive]
        if any(a):
            c = ['-f', '-a']
            err = ','.join([c[i] for i in range(len(c)) if a[i]])
            sys.exit('-d cannot be used with options {0}'.format(err))
    elif args.archive:
        a = [args.file, args.directory]
        if any(a):
            c = ['-f', '-d']
            err = ','.join([c[i] for i in range(len(c)) if a[i]])
            sys.exit('-a cannot be used with options {0}'.format(err))
        
    # get age public keys
    infile = open(args.gsi_age_pub_key)
    gsi_age_key = infile.readline().rstrip()
    infile.close()
    infile = open(args.it_age_pub_key)
    it_age_key = infile.readline().rstrip()
    infile.close()
    
    # create project dir
    projectdir = os.path.join(args.ega_stage, args.project)
    os.makedirs(projectdir, exist_ok=True)
    # create directory for encrypted data
    archivedir = os.path.join(projectdir, 'encrypted')
    os.makedirs(archivedir, exist_ok=True)
    
    # create qsubs dir
    qsubdir = os.path.join(projectdir, 'qsubs')
    os.makedirs(qsubdir, exist_ok=True)
    # create log dir
    logdir = os.path.join(qsubdir, 'logs')
    os.makedirs(logdir, exist_ok=True)
    
    
    # make a list of valid donors
    if args.cases:
        valid_cases = args.cases
    elif args.casefile:
        infile = open(args.casefile)
        valid_cases = infile.read().rstrip().split('\n')
        infile.close()
    else:
        valid_cases = []
       
    if valid_cases:
        valid_cases = list(map(lambda x: x.replace(' ', '_'), valid_cases))
     
    # archive a single folder
    if args.directory:
        case_id = os.path.basename(args.directory)
        # check that donor is valid
        if valid_cases and case_id not in valid_cases:
            print('case {0} is not in the provided list of valid cases'.format(case_id))
        else:
            print('ecrypting data for {0}'.format(case_id))
            encrypt_folder(args.directory, case_id, gsi_age_key, it_age_key, archivedir, qsubdir, logdir, args.memory, args.runtime)
    # archive all folders in directory
    elif args.archive:
        # get all the directories in the folder
        L = [os.path.join(args.archive, i) for i in os.listdir(args.archive) if os.path.isdir(os.path.join(args.archive, i))]
        for i in L:
            # get the case name
            case_id = os.path.basename(i)
            if valid_cases and case_id not in valid_cases:
                print('case {0} is not in the provided list of valid cases'.format(case_id))
            else:
                print('ecrypting data for {0}'.format(case_id))
                encrypt_folder(i, case_id, gsi_age_key, it_age_key, archivedir, qsubdir, logdir, args.memory, args.runtime)
                
    # archive a single file
    elif args.file:
        encrypt_file(args.file, gsi_age_key, it_age_key, archivedir, qsubdir, logdir, args.memory, args.runtime)



def decrypt_data(args):
    '''
    (str, str, str) -> None
    
    Decrypt data (single file, single folder or arcive with subfolders)
        
    Parameters
    ----------
    - file (str): Path to the file to decrypt
    - outputdir (str): Directory where the encrypted tarballs are decrypted
    - age_key (str): Path to the age decrypting key
    '''
    
    # create outputdir
    os.makedirs(args.outputdir, exist_ok=True)
           
    # decrypt a sigle file
    decrypt_file(args.file, args.age_key, args.outputdir)

        

   
if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'ega_archive.py', description='A tool to organize data to be moved to EGA stage')
    subparsers = parser.add_subparsers(help='sub-command help', dest='subparser_name')
       
    o_parser = subparsers.add_parser('link', help="Link files to release")
    o_parser.add_argument('-es', '--ega_stage', dest='ega_stage', default = '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA_STAGE', help='Directory where the links are organized. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA_STAGE')
    o_parser.add_argument('-pv', '--provenance', dest='provenance', default='/scratch2/groups/gsi/production/pr_refill_v2/provenance_reporter.json', help='Path to the json with production data. Default is /scratch2/groups/gsi/production/pr_refill_v2/provenance_reporter.json')
    o_parser.add_argument('-p', '--project', dest='project', help='Name of project of interest', required=True)
    o_parser.add_argument('-c', '--cases', dest='cases', nargs = '*', help='List of cases')
    o_parser.add_argument('-cf', '--casefile', dest='casefile', help='File with list of cases')
    o_parser.add_argument('--release_signedoff', dest='signoff_only', action='store_true', help='Keep only cases with complete release signoff')
    o_parser.add_argument('-nabu', '--nabu', dest='nabu', default='https://nabu.gsi.oicr.on.ca/case/sign-off', help='Nabu case signoff endpoint')
    o_parser.add_argument('-nk', '--nabu_key', dest='nabu_key_file', default='/.mounts/labs/gsi/secrets/nabu-prod_case-etl_api-key', help='Path to the nabu key file. Default is /.mounts/labs/gsi/secrets/nabu-prod_qc-gate-etl_api-key')
    o_parser.add_argument('-dt', '--data_type', dest='datatype', nargs= '*', choices = ['fastq', 'callready', 'analysis'], help='Restrict the data to sequences, analysis and/or call ready bams')
    o_parser.set_defaults(func=organize_data)
    
    e_parser = subparsers.add_parser('encrypt', help="Encrypt data")
    e_parser.add_argument('-p', '--project', dest='project', help='Name of project of interest', required=True)
    e_parser.add_argument('-es', '--ega_stage', dest='ega_stage', default = '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA_STAGE', help='Directory where the links are organized. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA_STAGE')
    e_parser.add_argument('-f', '--file', dest='file', help='Path to the file to encrypt')
    e_parser.add_argument('-d', '--directory', dest='directory', help='Path to the directory to tar and encrypt')
    e_parser.add_argument('-a', '--archive', dest='archive', help='Path to the directory containing subfolders to with linked donor data to tar and encrypt')
    e_parser.add_argument('-gk', '--gsikey', dest='gsi_age_pub_key', default = '/.mounts/labs/gsi/secrets/GSI_AGE_PUB_KEY', help='Path to the GSI age public key. Default is /.mounts/labs/gsi/secrets/GSI_AGE_PUB_KEY')
    e_parser.add_argument('-ik', '--itkey', dest='it_age_pub_key', default = '/.mounts/labs/gsi/secrets/IT_AGE_PUB_KEY', help='Path to the IT age public key. Default is /.mounts/labs/gsi/secrets/IT_AGE_PUB_KEY')
    e_parser.add_argument('-m', '--memory', dest='memory', default = '20', help='Encryption job memory. Default is 20G')
    e_parser.add_argument('-r', '--runtime', dest='runtime', default = '5', help='Encryption job runtime. Default is 5 hours')
    e_parser.add_argument('-c', '--cases', dest='cases', nargs = '*', help='List of cases')
    e_parser.add_argument('-cf', '--casefile', dest='casefile', help='File with list of cases')
    e_parser.set_defaults(func=encrypt_data)
        
    d_parser = subparsers.add_parser('decrypt', help="Decrypt data")
    d_parser.add_argument('-o', '--outputdir', dest='outputdir', help='Path to the output directory where decrupted files are written', required = True)
    d_parser.add_argument('-f', '--file', dest='file', help='Path to the encrypted file to decrypt')
    d_parser.add_argument('-ak', '--agekey', dest='age_key', default = '/.mounts/labs/gsi/secrets/gsi_drachive.age', help='Path to age key. Default is /.mounts/labs/gsi/secrets/gsi_drachive.age')
    d_parser.set_defaults(func=decrypt_data)
    
    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)
    
    
