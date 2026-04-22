# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 16:41:39 2026

@author: rjovelin
"""


import argparse
import os
import gzip



def is_gzipped(file):
    '''
    (str) -> bool

    Return True if file is gzipped

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


def get_project_records(project, provenance):
    '''
    (str, str) -> list
    
    Returns a list with all the records from the File Provenance Report for a given project.
    Each individual record in the list is a list of fields    
    
    Parameters
    ----------
    - project (str): Name of a project or run as it appears in File Provenance Report
    - provenance (str): Path to File Provenance Report.
    '''
        
    # get the records for a single project
    records = []
    # open provenance for reading. allow gzipped file or not
    if is_gzipped(provenance):
        infile = gzip.open(provenance, 'rt', errors='ignore')
    else:
        infile = open(provenance)
    for line in infile:
        if project in line:
            line = line.rstrip().split('\t')
            if project == line[1]:
                records.append(line)
    infile.close()
    return records


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
        'tmbanalysis' in workflow.lower()
    

def define_workflow_type(workflow):
    '''
    (str) -> str
    
    Returns the category in which the workflow needs to be archived (analysis, call_ready or fastq)
    
    Parameters
    ----------
    - workflow (str): Name of the workflow
    '''
    
    if is_sequencing(workflow):
        workflow_type = 'fastq'
    elif is_call_ready(workflow):
        workflow_type = 'call_ready'
    elif is_qc(workflow):
        workflow_type = 'qc'
    else:
        workflow_type = 'analysis'
    
    return workflow_type




def extract_project_data(provenance, project):
    '''
    (str, str, list, str | None) -> dict
  
    Returns a dictionary with file info extracted from FPR for a given project 
    and a given workflow if workflow is speccified. 
            
    Parameters
    ----------
    - provenance (str): Path to File Provenance Report
    - project (str): Project name as it appears in File Provenance Report. 
    - workflow (list): List of workflows used to generate the output files.
    - prefix (str | None): Prefix used to recover file full paths when File Provevance contains relative paths.
    '''
    
    # create a dict {file_swid: {file info}}
    D  = {}
    
    # get all the records for a single project
    records = get_project_records(project, provenance)
    
    # parse the records and get all the files for a given project
    for i in records:
        # keep records for project
        if project == i[1]:
            workflow = i[30]
            # get the workflow type
            workflow_type = define_workflow_type(workflow)
            # skip qc worfklows
            if workflow_type != 'qc':
                # check if file is deleted
                deleted = i[45]
                # get file path
                file_path = i[46]
                # get md5sum
                md5 = i[47]
                # get file name
                file_name = os.path.basename(file_path)
                # get file swid
                file_swid = i[44]
                # get workdlow swid
                workflow_run_id = i[36]
                # get donor
                donor = i[7]
                # get library aliases
                library = i[13]
                # get lims key
                limskey = i[56]
                # get platform
                platform = i[22]
                geo = i[12]
                if geo:
                    geo = {k.split('=')[0]:k.split('=')[1] for k in geo.split(';')}
                else:
                    geo = {}
                for j in ['geo_external_name', 'geo_group_id', 'geo_group_id_description',
                          'geo_targeted_resequencing', 'geo_library_source_template_type',
                          'geo_tissue_type', 'geo_tissue_origin']:
                    if j not in geo:
                        geo[j] = 'NA'
                    if j == 'geo_group_id':
                        # removes misannotations
                        geo[j] = geo[j].replace('&2011-04-19', '').replace('2011-04-19&', '')
       
                sample_id = donor + '_' + geo['geo_tissue_origin']+ '_' + geo['geo_tissue_type'] + '_' + geo['geo_library_source_template_type'] + '_' + geo['geo_group_id']
         
                d = {'workflow': workflow,
                     'file_path': file_path,
                     'deleted': deleted,
                     'file_name': file_name,
                     'workflow_type': workflow_type,
                     'donor': donor,
                     'md5': md5,
                     'platform': platform,
                     'workflow_run_id': workflow_run_id,
                     'file_swid': file_swid,
                     'external_name': geo['geo_external_name'],
                     'library_source': [geo['geo_library_source_template_type']],
                     'limskey': [limskey],
                     'library': [library],
                     'tissue_type': [geo['geo_tissue_type']],
                     'tissue_origin': [geo['geo_tissue_origin']],
                     'groupdesc': [geo['geo_group_id_description']],
                     'groupid': [geo['geo_group_id']],
                     'sample_id': [sample_id]}
            
            
            if file_swid not in D:
                D[file_swid] = d
            else:
                assert D[file_swid]['file_path'] == file_path
                assert D[file_swid]['external_name'] == geo['geo_external_name']
                D[file_swid]['sample_id'].append(sample_id)
                D[file_swid]['donor'].append(donor)
                D[file_swid]['limskey'].append(limskey)
                D[file_swid]['library'].append(library)
                D[file_swid]['tissue_type'].append(geo['geo_tissue_type'])
                D[file_swid]['tissue_origin'].append(geo['geo_tissue_origin'])
                D[file_swid]['library_source'].append(geo['geo_library_source_template_type'])
                D[file_swid]['groupdesc'].append(geo['geo_group_id_description'])
                D[file_swid]['groupid'].append(geo['geo_group_id'])
                
    
    return D    
        
            



def write_manifest(data, projectdir):
    '''
    (dict, str) -> None
    
    Parameters
    ----------
    - data (dict): Dictionary with file information for a given project extracted from FPR
    - projectdir (str): Project directory where data is organized
    '''

    header = ['workflow_run_id',
              'workflow',
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

    manifest = os.path.join(projectdir, '{0}.MANIFEST.txt')
    newfile = open(manifest, 'w') 
    newfile.write('\t'.join(header) + '\n')                    
        
    for file_swid in data:
        L = [data[file_swid]['workflow_run_id'],
             data[file_swid]['workflow'],
             data[file_swid]['donor'],
             data[file_swid]['file_path'],
             data[file_swid]['file_name'],
             data[file_swid]['file_swid'],
             data[file_swid]['md5'],
             data[file_swid]['platform'],
             data[file_swid]['external_name'],
             ';'.join(sorted(list(set(data[file_swid]['sample_id'])))),
             ';'.join(sorted(list(set(data[file_swid]['limskey'])))),
             ';'.join(sorted(list(set(data[file_swid]['groupid'])))),
             ';'.join(sorted(list(set(data[file_swid]['groupdesc'])))),
             ';'.join(sorted(list(set(data[file_swid]['library'])))),
             ';'.join(sorted(list(set(data[file_swid]['library_source'])))),
             ';'.join(sorted(list(set(data[file_swid]['tissue_type'])))),
             ';'.join(sorted(list(set(data[file_swid]['tissue_origin']))))]
        
        if 'deleted' in data[file_swid]['deleted']:
            L.append(data[file_swid]['deleted'])
        else:
            L.append('NO')
                
        newfile.write('\t'.join(L) + '\n')
    
    newfile.close()
    
    

def link_files(data, stagedir):
    '''
    (dict, str) -> None
    
    Link files of a given project in a specific data structure:
                
        donor --|
                | datatype --|
                             | workflow_id -- |
                                              | files
    Parameters
    ----------
    - data (dict): Dictionary with file information for a given project extracted from FPR
    - stagedir (str): Directory where data is organized
    '''
    
    for file_swid in data:
        # check if file is deleted 
        if 'deleted' not in data[file_swid]['deleted']:
            donor = data[file_swid]['donor']
            wfrunid = data[file_swid]['workflow_run_id']
            workflow_type = data[file_swid]['workflow_type']
            # create donor directory
            donordir = os.path.join(stagedir, donor)
            os.makedirs(donordir, exist_ok=True)
            # organize data by fastq, call ready and analysis
            datatypedir = os.path.join(donordir, workflow_type)
            os.makedirs(datatypedir, exist_ok=True)
            wfrundir = os.path.join(datatypedir, wfrunid)
            os.makedirs(wfrundir, exist_ok=True)
            # create link
            filename = data[file_swid]['file_name']
            link = os.path.join(wfrundir, filename)
            file = data[file_swid]['file_path']
            if os.path.isfile(link) == False:
                os.symlink(file, link)



def organize_data(args):
    '''
    (str, str, str) -> None
    
    Extract project data from FPR and organize links in the EGA stage directory
    
    Parameters
    ----------
    - ega_stage (str): Directory where the links are organized
    - fpr (str): Path to the File Provenance Report
    - project (str): Project of interest
    '''

    # create project dir
    projectdir = os.path.join(args.ega_stage, args.project)
    os.makedirs(projectdir, exist_ok=True)
    # create directory where to link the files
    stagedir = os.path.join(projectdir, 'stage_folders')
    os.makedirs(stagedir, exist_ok=True)
    
    # extract data
    data = extract_project_data(args.fpr, args.project)
    print('extracted {0} files for project {1}'.format(len(data), args.project))
    if data:
        # link files if files have been deleted
        link_files(data, stagedir)
        print('linked data to {0}'.format(stagedir))
        # write manifest with file information
        write_manifest(data, projectdir)
        print('wrote manifest in {0}'.format(projectdir))
    
   
if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'ega_archive.py', description='A tool to organize data to be moved to EGA stage')
    parser.add_argument('-es', '--ega_stage', dest='ega_stage', default = '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA_STAGE', help='Directory where the links are organized. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA_STAGE')
    parser.add_argument('-fpr', '--fpr', dest='fpr', default = '/scratch2/groups/gsi/production/vidarr/vidarr_files_report_latest.tsv.gz', help='Path to File Provenance Report. Default is /scratch2/groups/gsi/production/vidarr/vidarr_files_report_latest.tsv.gz')
    parser.add_argument('-p', '--project', dest='project', help='Name of project of interest', required=True)
    parser.set_defaults(func=organize_data)
    
    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)