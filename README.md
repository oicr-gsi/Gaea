# Gaea #

`Gaea` is a tool for the semi-atomated registration of genomics data to the European Genome-Phenome Archive (EGA).
It is developed primarily around the OICR infrastructure. 

Script `register_EGA_metadata_Gaea.sh` runs `Gaea` to:
- collect metadata registered at EGA
- evaluates the footprint on the staging server
- register new metadata for each EGA object


# Adding data to the EGA database #

## 1. Adding analyses information ##

### Adding analysis information ### 

usage: ```Gaea.py add_info analyses -c CREDENTIAL -md EGA -sd EGASUB -b BOX -t TABLE -i INFO -p PROJECTS -a ATTRIBUTES

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
Primary keys to tables `AnalysesProjects` and `AnalysesAttributes` are required parameters to link all tables

The information table should be formatted following the example below and follows these rules:
- alias should be a unique record for the file or group of files
- group files by using the same alias on separate lines
- columns fileName and analysisDate are optional
- column fileName is used to rename the files as they appear at EGA. File basename is used if omitted 

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
