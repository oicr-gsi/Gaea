# Gaea #

`Gaea` is a tool for the semi-automated registration of genomics data to the European Genome-Phenome Archive (EGA).
It is developed primarily around the OICR infrastructure. 

Script `register_EGA_metadata_Gaea.sh` runs `Gaea` to:
- collect metadata registered at EGA
- evaluates the footprint on the staging server
- register new metadata for each EGA object


# Adding data to the EGA database #

## 1. Adding study information ##

usage: ```Gaea.py add_info study -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -i INFORMATION```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with study information | Studies | required                                    |
| -i | Table with analysis info to be added to EGASUB |  | required                                    |

The input information table is a a list of colon-separated key value pairs. 

- alias: unique identifier associated with that study
- studyTypeId: EGA-controlled vocabulary. choose from Cancer Genomics, Epigenetics, Exome Sequencing, Forensic or Paleo-genomics, Gene Regulation Study, Metagenomics, Other, Pooled Clone Sequencing, Population Genomics, RNASeq, Resequencing, Synthetic Genomics, Transcriptome Analysis, Transcriptome Sequencing, Whole Genome Sequencing
- title: Study title 
- studyAbstract: Abstract describing the study

*Example:*

alias:AML_error_modeling
studyTypeId:Cancer Genomics
title:Integration of intra-sample contextual error modeling for improved detection of somatic mutations
studyAbstract:Sensitive mutation detection by next generation sequencing is of great importance for early cancer detection, monitoring minimal residual disease (MRD), and guiding precision oncology. Nevertheless, due to technical artefacts introduced during library preparation and sequencing steps as well as sub-optimal mutation calling analysis, the detection of variants with low allele frequency at high specificity is still problematic. Herein we validate a new practical error modeling technique for improved detection of single nucleotide variant (SNV) from hybrid-capture and targeted next generation sequencing.


## 2. Adding DAC information ##

The DAC lists the names and contact information of each person accredited to authorize data access.

usage: ```Gaea.py add_info dac -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -i INFORMATION -a ALIAS -tl TITLE```


Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with DAC information | Dacs | required                                    |
| -a | unique alias for the DAC |  | required                                    |
| -tl | Title of the DAC |  | required                                    |
| -i | File with DAC information |  | optional                                    |


Input information table should contain the following columns:

- contactName: First and last name
- email: email address
- organisation: institution
- phoneNumber: phone number of the contact
- mainContact: true/false if the person is the primary contact

*Example:*

contactName	email	organisation	phoneNumber	mainContact
John Smith	xxx.xxx@institution.ca	Ontario Institute for Cancer Research	xxx-xxx-xxxx	true
Jane Doe	xxx.xxx@institution.ca	Ontario Institute for Cancer Research	xxx-xxx-xxxx	false


## 3. Adding policy information ##


usage: ```Gaea.py add_info policy -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -a ALIAS -d DACID -tl TITLE -pf POLICYFILE -pt POLICYTEXT -u URL```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with policy information | Policies | required                                    |
| -a | unique alias for the policy |  | required                                    |
| -d | EGA accession ID for the corresponding DAC |  | required                                    |
| -tl | Title of the policy |  | required                                    |
| -pf | File with the template policy text |  | optional                                    |
| -pt | Policy text |  | optional                                    |
| -u | URL of the DAC or study  |  | optional                                    |

`-pt` or `pf` must be used.


## 4. Adding samples information ##

### Adding samples information ###

usage: ```Gaea.py add_info samples -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -a ATTRIBUTES -i INFO```
 
Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with samples information | Samples | required                                    |
| -i | Table with samples info to be added to EGASUB |  | required                                    |
| -a | Primary key in the SamplesAttributes table |  | required                                    |


The `-i` information table contains the following columns:

- alias: unique sample identifier
- caseOrControlId: EGA-controlled vocabulary, choose from case or control
- genderId: EGA-controlled vocabulary, choose from male, female and unknown
- phenotype: sample phenotype
- subjectId: sample Id

*Example:*

| alias | caseOrControlId | genderId | phenotype |  subjectId |
| ----- | --------------- | -------- | --------- |  --------- |
| CB1 | control | female | Cord blood |  CB1 |
| CB2 | control | male | Cord blood |  CB2 |
| CB3 | control | male | Cord blood |  CB3 |


### Adding samples attributes information ###

Sample attributes are assigned to a group of samples. It is required to split samples into subsets if different attributes must be associated with a group of samples.

usage: ```Gaea.py add_info samples_attributes -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -i INFO```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with sample attributes information | SamplesAttributes | required                                    |
| -i | Table with sample attributes info to be added to EGASUB |  | required                                    |

Table information is a colon-separated list of key, value pairs: 

- alias: unique attribute alias. must be the same value passed to `-a` when adding sample information
- title: sample title
- description: short description of the samples 

It is possible to add custom attributes using tags of colon-separated key, value pairs preceded by "attributes".
For instance, add the following line to specify the origin of a group of subjects:
`attributes:origin:canada`

*Example:*

alias:AMLErrModel
title:AML
description:Intra-sample error modeling


## 5. Adding analyses information ##

### Adding analysis information ### 

usage: ```Gaea.py add_info analyses -c CREDENTIAL -md EGA -sd EGASUB -b BOX -t TABLE -i INFO -p PROJECTS -a ATTRIBUTES```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with analyses information | Analyses | required                                    |
| -i | Table with analysis info to be added to EGASUB |  | required                                    |
| -p |  Primary key in the AnalysesProjects table |  | required                                    |
| -a | Primary key in the AnalysesAttributes table |  | required                                    |

Information to register `analyses` objects is stored in `Analyses`, `AnalysesProjects` and `AnalysesAttributes` tables.
Primary keys to tables `AnalysesProjects` and `AnalysesAttributes` are required parameters to link all tables.
Analysis project and attributes store information that be can be re-used for multiple submissions.

The information table should be formatted following the examples below and follows these rules:
- alias: a unique record for the file or group of files
- sampleReferences: alias of the registered sample of sample accession ID
                    can be a unique alias or ID or semi-colon separated aliases or sample IDs if the file contains data from multiple samples
- filePath: path of the file on the file system
- fileName (optional): name of the uploaded file at EGA (file basename of omitted)
- analysisDate (optional): date the file was generated

Group files by using the same alias on separate lines. Columns fileName and analysisDate are optional.
 

*Example 1:*

| alias | sampleReferences | filePath | fileName | analysisDate |
| ------- | ------- | ------- | ------------------------------------------ | ------- |
| PCSI_0024_Pa_P_PCSI_SC_WGS | PCSI_0024_Pa_P | /path_to/PCSI_0024_Pa_P.bam | PCSI_0024_Pa_P_whole_genome.bam | 02/08/21 |
| PCSI_0102_Ly_R_PCSI_SC_WGS | PCSI_0102_Ly_R | /path_to/PCSI_0102_Ly_R.bam | PCSI_0102_Ly_R_whole_genome.bam | 02/08/21 |


*Example 2:*

| alias | sampleReferences | filePath |
| ------- | ------- | ------- |
| CPCG0001-B1_realigned_recalibrated_merged_reheadered_WGS | CPCG0001-B1 | /path_to/CPCG0001-B1_realigned_recalibrated_merged_reheadered.bam |
| CPCG0001-B1_realigned_recalibrated_merged_reheadered_WGS | CPCG0001-B1 | /path_to/CPCG0001-B1_realigned_recalibrated_merged_reheadered.bam.bai |
| CPCG0001-F1_realigned_recalibrated_merged_reheadered_WGS | CPCG0001-F1 | /path_to/CPCG0001-F1_realigned_recalibrated_merged_reheadered.bamh |
| CPCG0001-F1_realigned_recalibrated_merged_reheadered_WGS | CPCG0001-F1 | /path_to/CPCG0001-F1_realigned_recalibrated_merged_reheadered.bam.bai |


### Adding analysis project information ###

usage: ```Gaea.py add_info analyses_attributes -c CREDENTIAL -md EGA -sd EGASUB -b BOX -t TABLE -i INFO -d DATATYPE```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with project information | AnalysesProjects | required                                    |
| -i | Table with project info to be added to EGASUB |  | required                                    |
| -d | Datatype  | Projects  | required                                    |

Input table is is a colon-separated list of key, value pairs.
`analysisTypeId` and `experimentTypeId` are EGA-controlled vocabulary describing the analysis and experiment.

- alias: alias of the project. must be the same value passed to `-p` when adding analysis information 
- analysisCenter:OICR
- studyId: EGA study accession ID EGASxxxxx
- Broker: EGA
- analysisTypeId: EGA-controlled vocabulary. choose from Reference Alignment (BAM), Sequence variation (VCF), Sample Phenotype
- experimentTypeId: EGA-controlled vocabulary. choose from Curation, Exome sequencing, Genotyping by array, Whole genome sequencing, transcriptomics

*Example:*

alias:PanCurX_SC_WGS
analysisCenter:OICR
studyId:EGAS00001002543
Broker:EGA
analysisTypeId:Reference Alignment (BAM)
experimentTypeId:Whole genome sequencing


### Adding analysis attributes information ###

Attributes are assigned to a group of files. It is required to split analysis files into subsets of different attributes must be associated.

usage: ```Gaea.py add_info analyses_attributes -c CREDENTIAL -md EGA -sd EGASUB -b BOX -t TABLE -i INFO -d DATATYPE```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with project information | AnalysesAttributes | required                                    |
| -i | Table with project info to be added to EGASUB |  | required                                    |
| -d | Datatype  | Attributes  | required                                    |

Input table is a colon-separated list key, value pairs.

- alias: alias associated with the analysis attributes. must be the same value passed to `-a` when adding analysis information
- title: title associated with the submission
- description: short description of the data being registered
- genomeId: EGA-controlled vocabulary. choose between GRCh37, GRCh38
- StagePath: directory on the staging server where files will be uploaded

It is possible to add custom attributes using tags of colon-separated key, value pairs preceded by "attributes".
For instance, add the following line to specify the aligner used in the analysis:
`attributes:aligner:BWA`

*Example:*

alias:PanCurX_SC_WGS
title:WGS alignment
description:Whole genome sequence data was aligned against the human reference genome (GRCh37)
genomeId:GRCh37
StagePath:PCSI/SC/wgs
attributes:aligner:BWA
attributes:aligner_ver:0.6.2



## 6. Adding runs information ##

usage: ```Gaea.py add_info runs -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -i INFORMATION -f FILE_TYPE -sp STAGE_PATH```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with runs information | Runs | required                                    |
| -i | Table with runs info to be added to EGASUB |  | required                                    |
| -f |  File type. EGA-controlled vocabulary. choose from "One Fastq file (Single)", "Two Fastq files (Paired)" |  | required                                    |
| -sp | Directory on the staging server where files are uploaded |  | required                                    |

The `-i` information table should contain the the following columns

- alias: unique identifier for the file or group of files. double underscore "__" is not allowed in alias
- sampleId: alias used to register the same or sample accession ID 
- experimentId: alias used to register the experiment or experiment accession ID 
- filePath: path to file on the file system
- fileName: name of the file to be uploaded at EGA. File basename is used if this column is omitted

Group files by using the same alias on separate lines. Columns fileName is optional.

*Example:*

| alias | sampleId | experimentId | filePath                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| PCSI_0106_Pa_P_526_rnaseq | PCSI_0106_Pa_P_526 | PCSI_0106_Pa_P_526.rnaseq.libA.1 | /path_to/PCSI_0106_Pa_P_526_unmapped_R1.fastq.gz                                    |
| PCSI_0106_Pa_P_526_rnaseq | PCSI_0106_Pa_P_526 | PCSI_0106_Pa_P_526.rnaseq.libA.1 | /path_to/PCSI_0106_Pa_P_526_unmapped_R2.fastq.gz                                   |
| PCSI_0224_Pa_P_526_rnaseq | PCSI_0224_Pa_P_526 | PCSI_0224_Pa_P_526.rnaseq.libA.1 | /path_to/PCSI_0224_Pa_P_526_unmapped_R1.fastq.gz                                    |
| PCSI_0224_Pa_P_526_rnaseq | PCSI_0224_Pa_P_526 | PCSI_0224_Pa_P_526.rnaseq.libA.1 | /path_to/PCSI_0224_Pa_P_526_unmapped_R2.fastq.gz                                   |


## 7. Adding experiments information ##
                 
usage: ```Gaea.py add_info experiments -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -i INFORMATION -tl TITLE -st STUDY -d DESCRIPTION -in INSTRUMENT -s SELECTION -sc SOURCE -sg STRATEGY -p PROTOCOL -la LIBRARY```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with experiments information | Experiments | required                                    |
| -i | Table with runs info to be added to EGASUB |  | required                                    |
| -tl |  Short experiment title |  | required                                    |
| -st | Study alias or study accession Id |  | required                                    |
| -d | Library description  |  | required                                    |
| -in | Instrument model. EGA-controlled vocabulary  |  | required                                    |
| -s | Library selection. EGA-controlled vocabulary |  | required                                    |
| -sc | Library source. EGA-controlled vocabulary |  | required                                    |
| -sg | Library strategy. EGA-controlled vocabulary |  | required                                    |
| -p | Library construction protocol. can be empty string  |  | required                                    |
| -la | Library 0 for paired and 1 for single end sequencing |  | required                                    |


`-s` Library selection is an EGA-controlled vocabulary.
Choose from: 5-methylcytidine antibody, CAGE, ChIP, ChIP-Seq, DNase, HMPR, Hybrid Selection, 
Inverse rRNA, Inverse rRNA selection, MBD2 protein methyl-CpG binding domain, MDA,
MF, MNase, MSLL, Oligo-dT, PCR, PolyA, RACE, RANDOM, RANDOM PCR, RT-PCR, Reduced Representation,
Restriction Digest, cDNA, cDNA_oligo_dT, cDNA_randomPriming, other, padlock probes capture method,
repeat fractionation, size fractionation, unspecified

`-sc` Library source is an EGA-controlled vocabulary.
Choose from: GENOMIC SINGLE CELL, METAGENOMIC, METATRANSCRIPTOMIC, OTHER, SYNTHETIC, TRANSCRIPTOMIC, TRANSCRIPTOMIC SINGLE CELL, VIRAL RNA

`-sg` Library strategy is an EGA-controlled vocabulary.
Choose from AMPLICON, ATAC-seq, Bisulfite-Seq, CLONE, CLONEEND, CTS, ChIA-PET, ChIP-Seq, DNase-Hypersensitivity,
EST, FAIRE-seq, FINISHING, FL-cDNA, Hi-C, MBD-Seq, MNase-Seq, MRE-Seq, MeDIP-Seq, OTHER,
POOLCLONE, RAD-Seq, RIP-Seq, RNA-Seq, SELEX, Synthetic-Long-Read, Targeted-Capture, Tethered Chromatin Conformation Capture,
Tn-Seq, VALIDATION, WCS, WGA, WGS, WXS, miRNA-Seq, ncRNA-Seq, ssRNA-seq 

`-in` Instrument is an EGA-controlled vocabulary. Choose from: 
    - ABI SOLID Models, AB 5500 Genetic Analyzer, AB 5500xl Genetic Analyzer, AB 5500xl-W Genetic Analysis System, AB SOLiD 3 Plus System, AB SOLiD 4 System, AB SOLiD 4hq System, AB SOLiD PI System, AB SOLiD System, AB SOLiD System 2.0, AB SOLiD System 3.0, AB 310 Genetic Analyzer, CAPILLARY Models, AB 3130 Genetic Analyzer, AB 3130xL Genetic Analyzer, AB 3500 Genetic Analyzer, AB 3500xL Genetic Analyzer, AB 3730 Genetic Analyzer, AB 3730xL Genetic Analyzer
    - COMPLETE GENOMICS Models, Complete Genomics
    - HELICOS Models, Helicos HeliScope 
    - ILLUMINA Models, HiSeq X Five, HiSeq X Ten, Illumina Genome Analyzer, Illumina Genome Analyzer II, ILLUMINA Models, Illumina Genome Analyzer IIx, Illumina HiScanSQ, Illumina HiSeq 1000, Illumina HiSeq 1500, Illumina HiSeq 2000, Illumina HiSeq 2500, Illumina HiSeq 3000, Illumina HiSeq 4000, Illumina MiSeq, Illumina MiniSeq, Illumina NovaSeq 6000, NextSeq 500, NextSeq 550
    - ION TORRENT Models, Ion Torrent PGM, Ion Torrent Proton, Ion Torrent S5, Ion Torrent S5 XL
    - LS454 Models, 454 GS, 454 GS 20, 454 GS FLX, 454 GS FLX Titanium, 454 GS FLX+, 454 GS Junior
    - OXFORD NANOPORE Models, GridION, MinION, PromethION
    - PACBIO SMRT Models, PacBio RS, PacBio RS II, Sequel

*Example:*

| sampleId | alias | libraryName |
| ------- | ------- | ------- |
| PCSI_0106_Pa_P_526 | PCSI_0106_Pa_P_526.rnaseq.libA.1 | PCSI_0106_Pa_P_526.rnaseq.libA |
| PCSI_0224_Pa_P_526 | PCSI_0224_Pa_P_526.rnaseq.libA.1 | PCSI_0224_Pa_P_526.rnaseq.libA |
| PCSI_0384_Pa_P_526 | PCSI_0384_Pa_P_526.rnaseq.libA.1 | PCSI_0384_Pa_P_526.rnaseq.libA |

      
## 8. Adding datasets information ##


usage: ```Gaea.py add_info datasets -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -a ALIAS -p POLICY -ds DESCRIPTION -tl TITLE -di DATASET_TYPEIDS -acs ACCESSIONS -dl DATASETS_LINKS -at ATTRIBUTES```


Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Database with submission metadata | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with datasets information | Datasets | required                                    |
| -a | Unique alias for the dataset |  | required                                    |
| -p | Policy alias or accession ID |  | required                                    |
| -ds | Dataset short description |  | required                                    |
| -tl | Datset short title |  | required                                    |
| -di | Dataset type Id. EGA-controlled vocabulary |  | required                                    |
| -acs | File with analyses and/or runs accession IDs |  | required                                    |
| -dl | File with dataset URLs |  | optional                                    |
| -at | File with dataset attributes |  | optional                                    |


`-di` Dataset type Id is an EGA-controlled vocabulary. Choose from:
Amplicon sequencing, Chip-Seq, Chromatin accessibility profiling by high-throughput sequencing, Exome sequencing, Genomic variant calling, Genotyping by array, Histone modification profiling by high-throughput sequencing, Methylation binding domain sequencing, Methylation profiling by high-throughput sequencing, Phenotype information, Study summary information, Transcriptome profiling by array, Transcriptome profiling by high-throughput sequencing, Whole genome sequencing 

`-acs` File with analyses and/or runs accessions is a one-column table formatted as follow:

EGAR00001589680
EGAR00001589682
EGAR00001589683
EGAZ00001312940
EGAZ00001312942
EGAZ00001312943

