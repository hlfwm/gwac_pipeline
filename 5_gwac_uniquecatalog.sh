#!/bin/bash
#usage: ./5_gwac_uniquecatalog.sh starimg stopimg
#       ./5_gwac_uniquecatalog.sh 1 10

source pipeline.conf;

if [ -e "$LOG_FILE" ]; then
        timestamp=`date "+%F-%R" --reference=$LOG_FILE`
        backupFile="$LOG_FILE.$timestamp"
        mv $LOG_FILE $LOG_DIR/$backupFile
fi

echo ""
echo "***********************************************"
echo "*          GWAC pipeline on MonetDB           *"
echo "***********************************************"
echo "                                               " 
echo "See $LOG_FILE for more details of query errors."
echo ""

radius=`bc -l <<<6/3600`
imgid_t=$1

echo "du $DBFARM -sh"|tee -a $LOG_FILE
du $DBFARM -sh 2>&1|tee -a $LOG_FILE

if [ $1 = 1 ]
then
    echo "mclient gwacdb -e -i 1_create_allschema.sql"; mclient gwacdb -e -i "1_create_allschema.sql" 2>&1|tee -a $LOG_FILE
    ((++imgid_t))
fi
START0=$(date +%s)
START1=$(date +%s)

#before everyday pipeline, evacuate the biggest temp table tempuniquecatalog
echo "DROP TABLE tempuniquecatalog; CREATE TABLE tempuniquecatalog"
mclient gwacdb -e -s "DROP TABLE tempuniquecatalog CASCADE;" 2>&1|tee -a $LOG_FILE
mclient gwacdb -e -s "create table tempuniquecatalog(uniqueid bigint, targetid bigint, distance_arcsec double, datapoints int, zone smallint, ra_avg double, decl_avg double, x double, y double, z double, flux_ref double, flux_tgs double, inactive BOOLEAN);" 2>&1|tee -a $LOG_FILE
mclient gwacdb -e -i 2_create_all_procedure.sql 2>&1|tee -a $LOG_FILE

while [ $imgid_t -le $2 ]
do
  echo $imgid_t |tee -a $LOG_FILE
  START=$(date +%s)
  date |tee -a $LOG_FILE
  #$TIME_CMD "Time: CALL insert_tempuniquecatalog %e" mclient -d gwacdb -e -s " CALL insert_tempuniquecatalog($imgid_t,$radius);" 2>&1 |tee -a $LOG_FILE  
  mclient gwacdb -e -s "DELETE FROM tempuniquecatalog;" 2>&1 |tee -a $LOG_FILE
  $TIME_CMD "Time: INSERT INTO tempuniquecatalog SELECT associates() %e"  mclient gwacdb -e -i -s "INSERT INTO tempuniquecatalog SELECT * FROM associates($imgid_t,$radius)" 2>&1 |tee -a $LOG_FILE
  #sed -i "s/SET imageno=[[:digit:]]\+/SET imageno=$imgid_t/" q1.sql
  #mclient gwacdb -i -e q1.sql 2>&1 |tee -a $LOG_FILE
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  $TIME_CMD "Time: CALL flag_n_to_m_tempunique() %e"            mclient -d gwacdb -e -s " CALL flag_n_to_m_tempunique();"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL insert_1_to_n_unique() %e"       mclient -d gwacdb -e -s " CALL  insert_1_to_n_unique(); "  2>&1 |tee -a $LOG_FILE 
  #$TIME_CMD "Time: mclient gwacdb -i -e q3.sql %e"        mclient gwacdb -i -e q3.sql 2>&1 |tee -a $LOG_FILE
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL flag_1_to_many_inactive_unique() %e"  mclient -d gwacdb -e -s " CALL  flag_1_to_many_inactive_unique(); "  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  $TIME_CMD "Time: mclient gwacdb -e -i onetomany-par.sql %e"   mclient -d gwacdb -e -i onetomany-par.sql 2>&1 |tee -a $LOG_FILE
  #$TIME_CMD "Time: CALL insert_new_1_to_n_assoc2() %e"   mclient -d gwacdb -e -s " CALL  insert_new_1_to_n_assoc2();"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL insert_1_to_n_replacement_assoc2() %e" mclient -d gwacdb -e -s " CALL  insert_1_to_n_replacement_assoc2();"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL delete_1_to_n_inactive_assoc() %e"  mclient -d gwacdb -e -s " CALL  delete_1_to_n_inactive_assoc();"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL flag_1_to_n_inactive_tempuniq() %e" mclient -d gwacdb -e -s " CALL  flag_1_to_n_inactive_tempuniq();"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL insert_1_to_1_assoc($imgid_t)   %e" mclient -d gwacdb -e -s " CALL  insert_1_to_1_assoc($imgid_t);"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  $TIME_CMD "Time: CALL update_1_to_1_uniq() %e"            mclient -d gwacdb -e -s " CALL  update_1_to_1_uniq();"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL insert_new_uniq($imgid_t) %e"       mclient -d gwacdb -e -s " CALL  insert_new_uniq($imgid_t);"  2>&1 |tee -a $LOG_FILE 
  mclient gwacdb -e -s "DELETE FROM newsrc;" 2>&1 |tee -a $LOG_FILE
  $TIME_CMD "Time: insert_new_uniq($imgid_t) %e"       mclient -d gwacdb -e -s "INSERT INTO newsrc SELECT t.id AS targetid FROM targets3 t LEFT OUTER JOIN tempuniquecatalog tuc ON t.id = tuc.targetid WHERE t.imageid = $imgid_t AND tuc.targetid IS NULL ORDER BY t.id;"  2>&1 |tee -a $LOG_FILE 
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  $TIME_CMD "Time: mclient gwacdb -e -i newsource-par.sql %e"    mclient -d gwacdb -e -i newsource-par.sql   2>&1 |tee -a $LOG_FILE  
  #du /scratch/meng/gwac/dbfarm -sh 2>&1|tee -a $LOG_FILE  
  #$TIME_CMD "Time: CALL delete_inactive_unique() %e"        mclient -d gwacdb -e -s " CALL delete_inactive_unique();" 2>&1 |tee -a $LOG_FILE

  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Time for image$imgid_t $DIFF seconds" | tee -a $LOG_FILE

  if [ `echo "$imgid_t % 2400" | bc` -eq 0 ]
  then DIFF=$(( $END - $START1 )) 
       DAY=`echo "$imgid_t / 2400" | bc`
       echo "Total time $DIFF seconds for DAY $DAY" | tee -a $LOG_FILE

       #echo "final sort association/uidlegacy table" | tee -a $LOG_FILE
       #$TIME_CMD "Time: mclient gwacdb -e -i finalsort.sql %e" mclient gwacdb -e -i finalsort.sql 2>&1 |tee -a $LOG_FILE

       #echo "profiling pipeline.log of for DAY $DAY: ./postpro.sh" 
       #./postpro.sh

       echo "insert jd into image $1 to $2" |tee -a $LOG_FILE
       python 3_insertImage.py $1 $2 2>&1 >/dev/null |tee -a $LOG_FILE

       echo "lightcurve test:" |tee -a $LOG_FILE
       ./lighttest-par.sh 1 2>&1 |tee -a $LOG_FILE
        
       START1=$(date +%s)
  fi
  ((++imgid_t))
done

echo "du $DBFARM -sh"|tee -a $LOG_FILE
du $DBFARM -sh 2>&1|tee -a $LOG_FILE
END0=$(date +%s)
DIFF=$(( $END0 - $START0 ))
echo "Total time $DIFF seconds" | tee -a $LOG_FILE

echo "***********************************************"
echo ""
