#!bin/bash
####################################################
## Analyze_Partition Table
## 기능 : 전체 파티션 테이블 analyze 쿼리문 생성 및 analyze_sql -> analyze_history 이동
##      partition table key type에 따른 쿼리문 생성 분류 자동화 
####################################################


source /home/ETL_SRC/analyze_job/analyze_util/analyze_class.sh

mv ${ANALYZE_SQL}/analyze_stat_part/* ${ANALYZE_SQL}/analyze_stat_history/ 2>&1

today=$(date -d now +'%Y%m%d')
system=$(get_system_name)


IFS=$'\n' table_list=(`psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -At <<!
SELECT    DB."NAME"
         ,TBL."TBL_NAME"
		 ,PT."PART_NAME" 
  FROM "TBLS" TBL, "DBS" DB 
                               WHERE TBL."DB_ID" = DB."DB_ID"
							     AND TBL."TBL_ID" NOT IN (SELECT "TBL_ID" FROM "PARTITIONS" GROUP BY "TBL_ID")
								 AND DB."NAME" IN ${SYSTEM}
		                         AND DB."DB_LOCATION_URI" LIKE '%{}%'
						   OREDER BY DB."NAME";
!`)


#PART_NAME[0] : DB_NAME
#PART_NAME[1] : TABLE_NAME
#PART_NAME[2] : PART_NAME

for record in ${table_list[@]}
    do
	    gen_analyze=""
		IFS='|' read -ra PART_NAME <<< "$record"
		IFS='/' read -ra check_part_cnt <<< "${PART_NAME[2]}"
		
		part_cnt=${#check_part_cnt[@]}
		
		if [[ $part_cnt -eq 1 ]] ; then
		    gen_analyze=$(one_partition ${PART_NAME[0]"."${PART_NAME[1]} $(check_type ${check_part_cnt[0]}))
		elif [[ $part_cnt -eq 2 ]] ; then 	
     	    gen_analyze=$(one_partition ${PART_NAME[0]"."${PART_NAME[1]} $(check_type ${check_part_cnt[0]}) $(check_type ${check_part_cnt[1]}))
		elif [[ $part_cnt -eq 3 ]] ; then 	
     	    gen_analyze=$(one_partition ${PART_NAME[0]"."${PART_NAME[1]} $(check_type ${check_part_cnt[0]}) $(check_type ${check_part_cnt[1]}) $(check_type ${check_part_cnt[2]}))
		fi
        
        echo $gen_analyze >> ${ANALYZE_SQL}/analyze_stat_part/part_${PART_NAME[0]}_${today}.sql
    done
echo $gen_analyze

	
