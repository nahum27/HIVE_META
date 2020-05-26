#!bin/bash
####################################################
## Analyze_Class
## 기능 :FOR Analyze Funtion Class
##     
####################################################

source /home/ETL_SRC/analyze_job/analyze_util/analyze_properties

function get_system_name() 
   {
    job_list=(`psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -At -c "select etl_list from etl_system_list order by etl_list"`)
  
    str=""
    for job in ${job_list[@]}
        do 
	        str+="'"$job"'"","
        done
    in_str="("${str:0: `expr ${#str}` -1}")"
    echo $in_str
   }

function check_type() 
   {
    declare -i value
	
	IFS='=' read -r KEY VALUE <<< "$1"
	
	value=$VALUE
	
	if [[ $value -ne 0 ]]; then
	    echo "$KEY=$VALUE"
	else
	    echo "$KEY='$VALUE'"
	fi
   }
	
function on_partition() {
    echo "analyze table $1 partition ($2) compute statistics;"
}

function two_partition() {
    echo "analyze table $1 partition ($2, $3} compute statistis;"
}

function three_partition() {
    echo "analyze table $1 partition ($1, $2, $3) compute statistics;"
}	
	