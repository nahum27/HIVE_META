# HIVE_META_STORE

HIVE TABLE TOTAL STAT AND AUTO ANALYZE PROCESS

- HIVE_STAT.md : 

  1. DB기준 : 전체 테이블 수, 연계중인 테이블수, 전체 컬럼수, 연계중인 컬럼 수, 레코드 수, 용량 산출 

  - 전체 시스템(DB)별 데이터 현황을 체크 할 수 있다.  
  - 데이터 연계 프로젝트라면 연계 현황을 한눈에 파악이 가능하다. 
  - 집계에 필요 시스템 혹은 테이블만 선정하여 집계 가능하다
  - 매일 같이 자동화 집계하여 변동 현황을 모니터링할 수 있다. -> 테이블 별 적재 용량, 레코드 변화량 분석마트 생성

- analyze_job project :

  1. Hive 통계량 최신 정보 유지를 위해 전체 관리 테이블 대상 analyze 명령어 생성 

  - log directory : 명령어 수행 시간 기록
  - sql directory : DB별 SQL파일 적재 directory 및 history directory
  - util directory : 새로운 기능을 할 project 추가 directory

  2. 개선 사항  

