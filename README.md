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
| -sd | Project directory | EGASUB | required                                    |
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



## 2. Adding policy information ##


usage: ```Gaea.py add_info policy -c CREDENTIAL -md METADATADB -sd SUBDB -b BOXNAME -t TABLE -a ALIAS -d DACID -tl TITLE -pf POLICYFILE -pt POLICYTEXT -u URL```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Project directory | EGASUB | required                                    |
| -b | EGA submission box |  | required                                    |
| -t | Table with policy information | Policies | required                                    |
| -a | alias for the policy |  | required                                    |
| -d | EGA accession ID for the corresponding DAC |  | required                                    |
| -tl | Title of the policy |  | required                                    |
| -pf | File with the template policy text |  | optional                                    |
| -pt | Policy text |  | optional                                    |
| -u | URL of the DAC or study  |  | optional                                    |

`-pt` or `pf` must be used.
    

## 3. Adding analyses information ##

### Adding analysis information ### 

usage: ```Gaea.py add_info analyses -c CREDENTIAL -md EGA -sd EGASUB -b BOX -t TABLE -i INFO -p PROJECTS -a ATTRIBUTES```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Project directory | EGASUB | required                                    |
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
| -sd | Project directory | EGASUB | required                                    |
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

usage: ```Gaea.py add_info analyses_attributes -c CREDENTIAL -md EGA -sd EGASUB -b BOX -t TABLE -i INFO -d DATATYPE```

Parameters

| argument | purpose | default | required/optional                                    |
| ------- | ------- | ------- | ------------------------------------------ |
| -c | File with database and box credentials |  | required                                    |
| -md | Database collecting metadata | EGA | required                                    |
| -sd | Project directory | EGASUB | required                                    |
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
