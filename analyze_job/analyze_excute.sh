#!bin/bash
####################################################
## Analyze_excute 
## 기능 : analyze 잡 실행 및 DB(시스템별) analyze 수행시간 기록
## parameter : 'N' or anything 
####################################################

source /home/ETL_SRC/analyze_job/analyze_util/analyze_class.sh

job=$1

if [[ $job == "N" ]] ; then
   log_loc=${ANALYZE_LOG}/nonpartition/
   work_loc=${ANALYZE_SQL}/analyze_stat_non/
   sh ${ANALYZE_HOME}/analyze_hive_table.sh
   list=(`ls $work_loc}`)
else
   log_loc=${ANALYZE_LOG}/partition/
   work_loc=${ANALYZE_SQL}/analyze_stat_part/
   sh ${ANALYZE_HOME}/analyze_hive_pt_table.sh
   list=(`ls $work_loc}`)
fi

today=$(date -d now +'%Y%m%d')
shell_start=$(date '+%s')

echo "Start Analyze time is | $(date '%Y-%m-%d %H:%M:%S') | ${shell_start}" >> ${log_loc}analyze_duration_${today}.log

for excute_job in ${list[@]}
 do 
    start_time=$(date +$s)
	echo $excute_job
	end_time=$(date +%s)
	
	echo $excute_job"|"$(($end_time - $start_time)) >> ${log_loc}analyze_duration_${today}.log
 done
shell_end=$(date '+%s')
shell_duration=$(($shell_end - $shell_start))

echo "End Analyze time is | $(date '+%Y-%m-%d %H:%M:%S') | ${shell_end} | ${shell_duration}" >> ${log_loc}analyze_duration_${today}.log
