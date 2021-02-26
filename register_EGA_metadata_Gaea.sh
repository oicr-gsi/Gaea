# declare array with boxes
declare -a boxes=("ega-box-12" "ega-box-137" "ega-box-1269" "ega-box-499" "ega-box-1843")

# load gaea module
module load gaea;

# provide path to credential file
credentials=/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/PROJECTS/EGA/Submission_Tools/.EGA_metData

# provide path to encryption key
EncryptionKeys=/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/PROJECTS/EGA/publickeys/public_keys.gpg

# provide portal URL
submission_portal=https://ega.crg.eu/submitterportal/v1
metadata_portal=https://ega-archive.org/submission-api/v1

for boxname in "${boxes[@]}"; do
	# download EGA metadata
	echo "downloading metadata for "$boxname""
	Gaea collect -c $credentials -b $boxname -md EGA -sd EGASUB -ch 500 -u $metadata_portal;
        # list files on the staging server
	echo "listing files in staging server for "$boxname""
	Gaea staging_server -c $credentials -b $boxname -md EGA -sd EGASUB -rt Runs -at Analyses -st StagingServer -ft FootPrint;
	# register all EGA objects 
	echo "register EGA objects in "$boxname""
	Gaea register -c $credentials -md EGA -sd EGASUB -b $boxname -k $EncryptionKeys -d 15 -f FootPrint -mm 10 -mx 8 -mxf 15 -p $submission_portal --Remove -sat SamplesAttributes -aat AnalysesAttributes -pt AnalysesProjects;
done;







