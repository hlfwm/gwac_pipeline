--1.
--CREATE PROCEDURE insert_tempuniquecatalog(imageno int, radius double)
--BEGIN
--    DELETE FROM tempuniquecatalog;
--    declare table u0_zone (id int, targetid int, ra_avg double, decl_avg double, flux_ref double, datapoints int,zone smallint,x double, y double, z double);
--    --DELETE FROM u0_zone;
--    insert into u0_zone select id,targetid,ra_avg,decl_avg,flux_ref,datapoints,zone,x,y,z from uniquecatalog3 order by zone;
--
--    insert into tempuniquecatalog
--           (uniqueid
--,targetid
--,distance_arcsec
--,datapoints
--,zone
--,ra_avg
--,decl_avg
--,x
--,y
--,z
--,flux_ref
--,flux_tgs
--)
--SELECT 
--uniqueid,
--targetid,
--distance_arcsec,
--datapoints,
--zone,
--ra_avg,
--decl_avg,
--x,
--y,
--z,
--flux_ref,
--flux_tgs 
--FROM (
--with x as (select targets.id,
--                  targets."dec" - radius as decmin,
--                  targets."dec" + radius as decmax,
--                  targets.ra,
--                  targets."dec",
--                  targets.x,
--                  targets.y,
--                  targets.z,
--                  targets.flux
--                  from targets3 as targets
--           where targets.imageid = imageno),
--
--     smallt as (select x.id,
--                  x.decmin,
--                  x.decmax,
--                  cast(floor(x.decmin / radius) as integer) as zonemin,
--                  cast(floor(x.decmax / radius) as integer) as zonemax,
--                  x.ra,
--                  x."dec",
--                  x.x,
--                  x.y,
--                  x.z,
--                  x.flux
--                from x)
--
--SELECT u0.id AS uniqueid,
--t0.id AS targetid,
----3600*DEGREES(acos(round(u0.x*t0.x+u0.y*t0.y+u0.z*t0.z,12))) AS distance_arcsec,
----180/3.1415926*3600*acos( round(sin(radians(u0.decl_avg))*sin(radians(t0."dec")) + cos(radians(u0.decl_avg))*cos(radians(t0."dec"))*cos(radians(u0.ra_avg)-radians(t0.ra)),12) ) AS distance_arcsec,
--3600*DEGREES(2*ASIN(SQRT( (u0.x - t0.x) * (u0.x - t0.x)+
--                          (u0.y - t0.y) * (u0.y - t0.y)+
--                          (u0.z - t0.z) * (u0.z - t0.z)
--                        )/2)
--             ) AS distance_arcsec,
--u0.datapoints+1 AS datapoints,
--u0.zone AS zone,
--((datapoints-1)*u0.ra_avg+t0.ra)/datapoints AS ra_avg,
--((datapoints-1)*u0.decl_avg+t0."dec")/datapoints AS decl_avg,
--COS(RADIANS(decl_avg)) * COS(RADIANS(ra_avg)) AS x,
--COS(RADIANS(decl_avg)) * SIN(RADIANS(ra_avg)) AS y,
--SIN(RADIANS(decl_avg)) AS z,
--u0.flux_ref AS flux_ref,
--t0.flux AS flux_tgs
--FROM u0_zone u0, smallt as t0
--WHERE 
--u0.zone BETWEEN zonemin AND zonemax
--AND u0.ra_avg between t0.ra-0.001 and t0.ra+0.001
--AND u0.decl_avg between t0.decmin and t0.decmax
--AND u0.x*t0.x+u0.y*t0.y+u0.z*t0.z > cos(radians(radius))
--) as ut;
--END;


--2.
CREATE PROCEDURE flag_n_to_m_tempunique()
BEGIN
UPDATE tempuniquecatalog SET INACTIVE = FALSE;    

UPDATE tempuniquecatalog
SET INACTIVE = TRUE
WHERE EXISTS (SELECT uniqueid,
                     targetid
              FROM (SELECT t1.uniqueid,
                           t1.targetid
                      FROM (SELECT targetid,
                                   MIN(distance_arcsec) as min_d
                              FROM tempuniquecatalog
				WHERE  uniqueid IN ( select uniqueid
						       from tempuniquecatalog
						     where uniqueid IN (SELECT uniqueid
									  from tempuniquecatalog
									where targetid IN (select targetid
											     from tempuniquecatalog
											   GROUP BY targetid
											   HAVING COUNT(*) > 1
											   )
									 )
						     GROUP BY uniqueid
						     HAVING COUNT(*) >1
						    )
                                 AND targetid IN (SELECT targetid
                                                    FROM tempuniquecatalog
                                                  GROUP BY targetid
                                                  HAVING COUNT(*) > 1
                                                  )
                              GROUP BY targetid
                   ) t0
                  , (SELECT uniqueid,
                            targetid,
                            distance_arcsec as d
		      FROM tempuniquecatalog
			WHERE  uniqueid IN ( select uniqueid
					       from tempuniquecatalog
					     where uniqueid IN (SELECT uniqueid
								  from tempuniquecatalog
								where targetid IN (select targetid
										     from tempuniquecatalog
										   GROUP BY targetid
										   HAVING COUNT(*) > 1
										   )
								 )
					     GROUP BY uniqueid
					     HAVING COUNT(*) >1
					    )
			 AND targetid IN (SELECT targetid
					    FROM tempuniquecatalog
					  GROUP BY targetid
					  HAVING COUNT(*) > 1
					  )
         	   ) t1
         	  WHERE t0.targetid = t1.targetid
           	    AND t0.min_d < t1.d
       		   ) t2 
     WHERE t2.uniqueid = tempuniquecatalog.uniqueid
       AND t2.targetid = tempuniquecatalog.targetid 
);

UPDATE tempuniquecatalog
  SET inactive = TRUE
WHERE EXISTS ( SELECT uniqueid, targetid
                          FROM  ( select t0.uniqueid, t0.targetid
                                        from ( SELECT uniqueid, targetid
	                                        FROM tempuniquecatalog
	                                        WHERE  uniqueid IN ( select uniqueid
	                                           from tempuniquecatalog
	                                            where uniqueid IN (SELECT uniqueid
	                                                                from tempuniquecatalog
	                                                                where targetid IN (select targetid
	                                                                                     from tempuniquecatalog
	                                                                                     GROUP BY targetid
	                                                                                     HAVING COUNT(*) > 1
	                                                                                    )
	                                                                        )
	                                            GROUP BY uniqueid
	                                            HAVING COUNT(*) >1
	                                          )
	                                       AND targetid IN (SELECT targetid	
	                                       FROM tempuniquecatalog
	                                       GROUP BY targetid
	                                       HAVING COUNT(*) > 1
	                                       )
	                                     AND inactive = false) t0,
	                                     
		                       (select min(uniqueid) as minuid, targetid
                                       from tempuniquecatalog
                                        where uniqueid IN (SELECT uniqueid
                                                                from tempuniquecatalog
                                                                where targetid IN (select targetid
                                                                                           from tempuniquecatalog
                                                                                             GROUP BY targetid
                                                                                             HAVING COUNT(*) > 1
                                                                                             )
			                                            GROUP BY uniqueid
			                                            HAVING COUNT(*) >1
			                                          )
                                       AND targetid IN (SELECT targetid	
				                                       FROM tempuniquecatalog
				                                       GROUP BY targetid
				                                       HAVING COUNT(*) > 1
				                                       )
                                       AND inactive = false
    				        group by targetid		
		                       ) t1 					                       
		                         WHERE t0.targetid = t1.targetid
		                          AND t0.uniqueid > t1.minuid
					) t2
				WHERE t2.uniqueid = tempuniquecatalog.uniqueid 
				AND t2.targetid = tempuniquecatalog.targetid
			)
;
END;

--3.
--CREATE PROCEDURE insert_1_to_n_unique()
--BEGIN
--INSERT INTO uniquecatalog3
--(targetid
--,datapoints
--,zone
--,ra_avg
--,decl_avg
--,flux_ref
--,x
--,y
--,z
--,inactive
--)
--SELECT  targetid
--             ,datapoints
--             ,zone
--             ,ra_avg
--             ,decl_avg
--             ,flux_ref
--             ,x
--             ,y
--             ,z
--             ,FALSE
--from (select uniqueid
--        from tempuniquecatalog
--          where inactive = FALSE
--          group by uniqueid
--          having count(*) >1
--         ) one_to_many
--     , tempuniquecatalog t
--    where t.uniqueid = one_to_many.uniqueid
--    and t.inactive = FALSE ;
-- END;
--
----4.
--CREATE PROCEDURE flag_1_to_many_inactive_unique()
--BEGIN
--UPDATE uniquecatalog3
--  SET inactive = TRUE
-- WHERE id IN ( SELECT uniqueid
--                 FROM tempuniquecatalog
--                WHERE inactive = FALSE
--                GROUP BY uniqueid
--                HAVING COUNT(*)>1
--             );
--END;
--CREATE PROCEDURE delete_1_to_many_unique()
--BEGIN
--DELETE FROM uniquecatalog3
-- WHERE id IN ( SELECT uniqueid
--                 FROM tempuniquecatalog
--                WHERE inactive = FALSE
--                GROUP BY uniqueid
--                HAVING COUNT(*)>1
--             );
--END;
----5.insert into association with new unique entries and the new targets ids.(New to New)
--CREATE PROCEDURE insert_new_1_to_n_assoc()
--BEGIN
--DECLARE TABLE one_to_many (old_uniqueid int, new_targetid int, distance_arcsec double);
--DECLARE TABLE new_to_new (uniqueid int, targetid int, distance_arcsec double);
----DECLARE TABLE old_to_old (uniqueid int, targetid int, distance_arcsec double);
--INSERT INTO one_to_many(old_uniqueid, new_targetid, distance_arcsec) SELECT uniqueid, targetid, distance_arcsec FROM tempuniquecatalog WHERE uniqueid IN ( SELECT uniqueid FROM tempuniquecatalog WHERE inactive = FALSE GROUP BY uniqueid HAVING COUNT(*) > 1) ORDER BY uniqueid, targetid;
--INSERT INTO new_to_new SELECT u.id AS uniqueid, u.targetid, one_to_many.distance_arcsec FROM uniquecatalog3 u, one_to_many WHERE u.targetid = one_to_many.new_targetid ORDER BY uniqueid, targetid;
----INSERT INTO old_to_old SELECT uniqueid, targetid, distance_arcsec FROM associatedsource WHERE uniqueid IN (SELECT old_uniqueid FROM one_to_many) ORDER BY uniqueid, targetid;
--
--INSERT INTO associatedsource
--  (uniqueid
--  ,targetid
--  ,distance_arcsec
--  ,type
--  )
--  SELECT new_to_new.uniqueid
--        ,new_to_new.targetid   --this is targetid from new image.
--        ,new_to_new.distance_arcsec
--        ,2 --base points of 1-n
--    FROM one_to_many
--        , new_to_new
--   WHERE one_to_many.new_targetid = new_to_new.uniqueid; --NB we pull the new unique id from the runningcatalog by matching with temprunningcatalog via targetid.(their temprunningcatalog.unique points at old unique entries)
--END;
--
----6.Insert links into the association table between the new unique entries and the old targets. (New is New links have been added earlier)
--CREATE PROCEDURE insert_1_to_n_replacement_assoc()
--BEGIN
--DECLARE TABLE one_to_many (old_uniqueid int, new_targetid int);
--DECLARE TABLE new_to_new (uniqueid int, targetid int);
--DECLARE TABLE old_to_old (uniqueid int, targetid int, distance_arcsec double);
--INSERT INTO one_to_many(old_uniqueid, new_targetid) SELECT uniqueid, targetid FROM tempuniquecatalog WHERE uniqueid IN ( SELECT uniqueid FROM tempuniquecatalog WHERE inactive = FALSE GROUP BY uniqueid HAVING COUNT(*) > 1) ORDER BY uniqueid, targetid;
--INSERT INTO old_to_old SELECT uniqueid, targetid, distance_arcsec FROM associatedsource WHERE uniqueid IN (SELECT old_uniqueid FROM one_to_many) ORDER BY uniqueid, targetid;
--INSERT INTO new_to_new SELECT id AS uniqueid, targetid FROM uniquecatalog3 WHERE targetid IN (SELECT new_targetid FROM one_to_many) ORDER BY uniqueid, targetid;
--
--INSERT INTO associatedsource
--  (uniqueid
--  ,targetid
--  ,distance_arcsec
--  ,type
--  )
--  SELECT new_to_new.uniqueid 
--        ,old_to_old.targetid              --old targetids corresponds to new unique ids' old existing ones.
--        ,old_to_old.distance_arcsec
--        ,6
--    FROM one_to_many
--        ,new_to_new
--        ,old_to_old
--   WHERE one_to_many.new_targetid = new_to_new.targetid    --NB we pull the new unique id from the runningcatalog by matching with temprunningcatalog via targetid.
--     AND one_to_many.old_uniqueid = old_to_old.uniqueid;
--END;
--
----9.
--CREATE PROCEDURE insert_1_to_1_assoc(imgid int)
--BEGIN
--INSERT INTO associatedsource
--   (uniqueid
--  , targetid
--  , distance_arcsec
--  , type
--   )
--  SELECT tp.uniqueid
--       , tp.targetid
--       , tp.distance_arcsec
--       , 3            -- 3 is 1-1 type
--   FROM tempuniquecatalog tp
--   WHERE tp.inactive = FALSE; --all remaining active pairs in tempuniquecatalog are 1-to-1.
--END;
--

--10.
CREATE procedure update_1_to_1_uniq()
BEGIN
UPDATE uniquecatalog3
           SET datapoints = (SELECT datapoints  
                               FROM tempuniquecatalog
                              WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                              AND tempuniquecatalog.inactive = FALSE
                             )
              ,zone = (SELECT zone
                         FROM tempuniquecatalog
                        WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                      )
              ,ra_avg = (SELECT ra_avg
                          FROM tempuniquecatalog
                         WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                       )
              ,decl_avg = (SELECT decl_avg
                            FROM tempuniquecatalog
                           WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                         )
              ,x = (SELECT x
                      FROM tempuniquecatalog
                     WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                   )
              ,y = (SELECT y
                      FROM tempuniquecatalog
                     WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                   )
              ,z = (SELECT z
                      FROM tempuniquecatalog
                     WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                   )
              ,flux_ref = (SELECT flux_ref
                      FROM tempuniquecatalog
                     WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                   )
              ,inactive = FALSE
         WHERE EXISTS (SELECT uniqueid
                         FROM tempuniquecatalog
                        WHERE tempuniquecatalog.uniqueid = uniquecatalog3.id
                          AND tempuniquecatalog.inactive = FALSE
                      );
END;


--11.
--CREATE PROCEDURE insert_new_uniq(imageno int)
--BEGIN
--INSERT INTO uniquecatalog3
--  (targetid
--  ,datapoints
--  ,zone
--  ,ra_avg
--  ,decl_avg
--  ,flux_ref
--  ,x
--  ,y
--  ,z
--  ,inactive
--  )
--  SELECT t.targetid
--        ,t.datapoints
--        ,t.zone
--        ,t.ra_avg 
--        ,t.decl_avg
--        ,t.flux_ref
--        ,t.x
--        ,t.y
--        ,t.z
--        ,FALSE
--    FROM (SELECT t0.id AS targetid
--                ,1 AS datapoints
--                ,t0.zone
--                ,t0.ra AS ra_avg
--                ,t0."dec" AS decl_avg
--                ,t0.flux AS flux_ref
--                ,t0.x
--                ,t0.y
--                ,t0.z
--            FROM targets3 t0
--             WHERE t0.imageid = imageno
--         ) t
--         LEFT OUTER JOIN tempuniquecatalog tp
--         ON t.targetid = tp.targetid
--   WHERE tp.targetid IS NULL ;
--END;
--
----12.
--CREATE PROCEDURE insert_new_assoc(imageno int)
--BEGIN
--INSERT INTO associatedsource
--  (uniqueid
--  ,targetid
--  ,distance_arcsec
--  ,type
--  )
--  SELECT u0.id AS uniqueid
--        ,u0.targetid
--        ,0
--        ,4
--    FROM (SELECT t1.id AS targetid
--            FROM targets3 t1
--                 LEFT OUTER JOIN tempuniquecatalog tuc1
--                 ON t1.id = tuc1.targetid
--            WHERE t1.imageid = imageno
--              AND tuc1.targetid IS NULL
--          ) new_src 
--        ,uniquecatalog3 u0
--   WHERE u0.targetid = new_src.targetid;
--END;
--
----13.
--CREATE PROCEDURE delete_inactive_unique()
--BEGIN
--DELETE FROM uniquecatalog3 WHERE inactive = TRUE;
--END;

----14.
--CREATE PROCEDURE gwac_uniquecatalog(imageid int, radius double)
--BEGIN 
--       DECLARE imgid_t int;
--       SET imgid_t =1;
--       SET imgid_t = imgid_t +1;
--
--WHILE (imgid_t<=imageid)
--DO                                              
--    CALL insert_tempuniquecatalog(imgid_t,radius);  
--    CALL flag_n_to_m_tempunique();
--    
--    CALL  insert_1_to_n_unique();
--    CALL  insert_new_1_to_n_assoc();
--    CALL  insert_1_to_n_replacement_assoc();
--    CALL  delete_1_to_n_inactive_assoc();
--    CALL  flag_1_to_n_inactive_tempuniq();
--    
--    CALL  insert_1_to_1_assoc(imgid_t);
--    CALL  update_1_to_1_uniq();
--
--    CALL  insert_new_uniq(imgid_t);
--    CALL  insert_new_assoc(imgid_t);
--    SET   imgid_t = imgid_t +1;
--
--END WHILE; 
--END;


--15.
--create function distance_ut(uid int, tid int)
--RETURNS double
--BEGIN
--declare ra1,decl1,ra2,decl2, rra1, rdecl1, rra2 ,rdecl2 double;
--SET ra1=(select ra_avg from uniquecatalog3 where id=uid);
--SET decl1=(select decl_avg from uniquecatalog3 where id=uid);
--SET ra2=(select ra from targets3 where id=tid);
--SET decl2=(select "dec" from targets3 where id=tid);
--SET rra1=radians(ra1);
--SET rra2=radians(ra2);
--SET rdecl1=radians(decl1);
--SET rdecl2=radians(decl2);
----RETURN 180/3.1415926*3600*acos( sin(rdecl1)*sin(rdecl2)+cos(rdecl1)*cos(rdecl2)*cos(rra1-rra2));
----RETURN 180/3.1415926*3600*acos( round(sin(rdecl1)*sin(rdecl2),7) + round(cos(rdecl1)*cos(rdecl2)*cos(rra1-rra2),7) );
--RETURN 180/3.1415926*3600*acos( round(sin(rdecl1)*sin(rdecl2) + cos(rdecl1)*cos(rdecl2)*cos(rra1-rra2),12) );
--END;
--
----16
--create function distanceo_ut(uid int, tid int)
--RETURNS double
--BEGIN
--declare ra1,decl1,ra2,decl2, rra1, rdecl1, rra2 ,rdecl2 double;
--SET ra1=(select ra_avg from uniquecatalog3 where id=uid);
--SET decl1=(select decl_avg from uniquecatalog3 where id=uid);
--SET ra2=(select ra from targets3 where id=tid);
--SET decl2=(select "dec" from targets3 where id=tid);
--SET rra1=radians(ra1);
--SET rra2=radians(ra2);
--SET rdecl1=radians(decl1);
--SET rdecl2=radians(decl2);
--RETURN 180/3.1415926*3600*acos( sin(rdecl1)*sin(rdecl2)+cos(rdecl1)*cos(rdecl2)*cos(rra1-rra2));
--END;
--
--create function distance_utxyz(uid int, tid int)
--RETURNS double
--BEGIN
--declare x1,y1,z1,x2,y2,z2 double;
--SET x1=(select x from uniquecatalog3 where id=uid);
--SET y1=(select y from uniquecatalog3 where id=uid);
--SET z1=(select z from uniquecatalog3 where id=uid);
--SET x2=(select x from targets3 where id=tid);
--SET y2=(select y from targets3 where id=tid);
--SET z2=(select z from targets3 where id=tid);
--RETURN 3600*DEGREES(acos(round(x1*x2+y1*y2+z1*z2,12)));
--END;

create function distance_sin(uid int, tid int)
RETURNS double
BEGIN
declare x1,y1,z1,x2,y2,z2 double;
SET x1=(select x from uniquecatalog3 where id=uid);
SET y1=(select y from uniquecatalog3 where id=uid);
SET z1=(select z from uniquecatalog3 where id=uid);
SET x2=(select x from targets3 where id=tid);
SET y2=(select y from targets3 where id=tid);
SET z2=(select z from targets3 where id=tid);
RETURN 3600*DEGREES(2*ASIN(SQRT( (x1 - x2) * (x1 - x2)+
                          (y1 - y2) * (y1 - y2)+
                          (z1 - z2) * (z1 - z2)
                               )
                         /2)
                   );
END;

CREATE FUNCTION associates(imageno int, radius double)
RETURNS TABLE (uniqueid bigint, targetid bigint, distance_arcsec double,datapoints int,zone smallint,ra_avg double,decl_avg double,x double,y double,z double, flux_ref double, flux_tgs double,inactive boolean)
BEGIN
DECLARE TABLE u0_zone (id bigint, targetid bigint, ra_avg double, decl_avg double, flux_ref double, datapoints int,zone smallint,x double, y double, z double);
DECLARE zoneheig double;
SET zoneheig=1e1/3600;
INSERT INTO u0_zone select id,targetid,ra_avg,decl_avg,flux_ref,datapoints,zone,x,y,z from uniquecatalog3 order by zone;
RETURN TABLE(SELECT uniqueid,targetid,distance_arcsec,datapoints,zone,ra_avg,decl_avg,x,y,z,flux_ref,flux_tgs,false
FROM (
with x as (select targets.id,
                  targets."dec" - radius as decmin,
                  targets."dec" + radius as decmax,
                  targets.ra,
                  targets."dec",
                  targets.x,
                  targets.y,
                  targets.z,
                  targets.flux
                  from targets3 as targets
           where targets.imageid = imageno),

     smallt as (select x.id,
                  x.decmin,
                  x.decmax,
                  cast(floor(x.decmin / zoneheig) as integer) as zonemin,
                  cast(floor(x.decmax / zoneheig) as integer) as zonemax,
                  x.ra,
                  x."dec",
                  x.x,
                  x.y,
                  x.z,
                  x.flux
                from x)

SELECT 
u0.id AS uniqueid,
t0.id AS targetid,
3600*DEGREES(2*ASIN(SQRT( (u0.x - t0.x) * (u0.x - t0.x)+
                          (u0.y - t0.y) * (u0.y - t0.y)+
                          (u0.z - t0.z) * (u0.z - t0.z)
                        )/2)
             ) AS distance_arcsec,
u0.datapoints+1 AS datapoints,
u0.zone AS zone,
((datapoints-1)*u0.ra_avg+t0.ra)/datapoints AS ra_avg,
((datapoints-1)*u0.decl_avg+t0."dec")/datapoints AS decl_avg,
COS(RADIANS(decl_avg)) * COS(RADIANS(ra_avg)) AS x,
COS(RADIANS(decl_avg)) * SIN(RADIANS(ra_avg)) AS y,
SIN(RADIANS(decl_avg)) AS z,
u0.flux_ref AS flux_ref,
t0.flux AS flux_tgs
FROM u0_zone u0, smallt as t0
WHERE
u0.zone BETWEEN zonemin AND zonemax
AND u0.ra_avg between t0.ra-alpha(t0."dec", radius) and t0.ra+alpha(t0."dec", radius)
AND u0.decl_avg between t0.decmin and t0.decmax
AND u0.x*t0.x+u0.y*t0.y+u0.z*t0.z > cos(radians(radius))
) AS ut)
;
end;

--CREATE FUNCTION associates(imageno int, radius double)
--RETURNS TABLE (uniqueid bigint, targetid bigint, distance_arcsec double,datapoints int,zone smallint,ra_avg double,decl_avg double,x double,y double,z double, flux_ref double, flux_tgs double,inactive boolean)
--BEGIN
--DECLARE TABLE u0_zone (id bigint, targetid bigint, ra_avg double, decl_avg double, flux_ref double, datapoints int,zone smallint,x double, y double, z double);
--DECLARE TABLE t0_zone (id bigint, decmin double, decmax double, zonemin smallint, zonemax smallint, ra double, "dec" double, x double, y double, z double, flux double);
--DECLARE zoneheig double;
--SET zoneheig=1e1/3600;
--INSERT INTO u0_zone select id,targetid,ra_avg,decl_avg,flux_ref,datapoints,zone,x,y,z from uniquecatalog3 order by zone;
--INSERT INTO t0_zone
--with x as (select targets.id,
--                  targets."dec" - radius as decmin,
--                  targets."dec" + radius as decmax,
--                  targets.ra,
--                  targets."dec",
--                  targets.x,
--                  targets.y,
--                  targets.z,
--                  targets.flux
--                  from targets3 as targets
--           where targets.imageid = imageno)
--select x.id,
--                  x.decmin,
--                  x.decmax,
--                  cast(floor(x.decmin / zoneheig) as integer) as zonemin,
--                  cast(floor(x.decmax / zoneheig) as integer) as zonemax,
--                  x.ra,
--                  x."dec",
--                  x.x,
--                  x.y,
--                  x.z,
--                  x.flux from x order by zonemin, zonemax;
--RETURN TABLE(SELECT uniqueid,targetid,distance_arcsec,datapoints,zone,ra_avg,decl_avg,x,y,z,flux_ref,flux_tgs,false
--FROM (
--SELECT
--u0.id AS uniqueid,
--t0.id AS targetid,
--3600*DEGREES(2*ASIN(SQRT( (u0.x - t0.x) * (u0.x - t0.x)+
--                          (u0.y - t0.y) * (u0.y - t0.y)+
--                          (u0.z - t0.z) * (u0.z - t0.z)
--                        )/2)
--             ) AS distance_arcsec,
--u0.datapoints+1 AS datapoints,
--u0.zone AS zone,
--((datapoints-1)*u0.ra_avg+t0.ra)/datapoints AS ra_avg,
--((datapoints-1)*u0.decl_avg+t0."dec")/datapoints AS decl_avg,
--COS(RADIANS(decl_avg)) * COS(RADIANS(ra_avg)) AS x,
--COS(RADIANS(decl_avg)) * SIN(RADIANS(ra_avg)) AS y,
--SIN(RADIANS(decl_avg)) AS z,
--u0.flux_ref AS flux_ref,
--t0.flux AS flux_tgs
--FROM u0_zone u0, t0_zone as t0
--WHERE
--u0.zone BETWEEN zonemin AND zonemax
--AND u0.ra_avg between t0.ra-alpha(t0."dec", radius) and t0.ra+alpha(t0."dec", radius)
--AND u0.decl_avg between t0.decmin and t0.decmax
--AND u0.x*t0.x+u0.y*t0.y+u0.z*t0.z > cos(radians(radius))
--) AS ut order by uniqueid, targetid);
--end;
