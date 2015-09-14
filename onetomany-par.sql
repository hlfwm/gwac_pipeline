DROP TABLE one_to_many;
DROP TABLE new_to_new;
CREATE TABLE one_to_many (old_uniqueid bigint, new_targetid bigint, distance_arcsec double);
CREATE TABLE new_to_new (uniqueid bigint, targetid bigint, distance_arcsec double);

INSERT INTO one_to_many(old_uniqueid, new_targetid, distance_arcsec) SELECT t.uniqueid, t.targetid, t.distance_arcsec FROM tempuniquecatalog t, ( SELECT uniqueid FROM tempuniquecatalog WHERE inactive = FALSE GROUP BY uniqueid HAVING COUNT(*) > 1) t0 WHERE t0.uniqueid = t.uniqueid and t.inactive=false ORDER BY uniqueid, targetid;
--3, insert 1 to many unique
INSERT INTO uniquecatalog3(targetid,datapoints,zone,ra_avg,decl_avg,flux_ref,x,y,z,inactive) SELECT targetid, datapoints, zone, ra_avg, decl_avg, flux_ref, x, y, z, FALSE from (select distinct old_uniqueid from one_to_many) o,tempuniquecatalog t  where t.uniqueid = o.old_uniqueid and t.inactive = FALSE ;
--4.flag_1_to_many_inactive_unique
UPDATE uniquecatalog3 SET inactive = TRUE WHERE id IN (SELECT DISTINCT old_uniqueid FROM one_to_many);

INSERT INTO new_to_new SELECT u.id AS uniqueid, u.targetid, one_to_many.distance_arcsec FROM uniquecatalog3 u, one_to_many WHERE u.targetid = one_to_many.new_targetid ORDER BY uniqueid, targetid;
INSERT INTO uidlegacy(old_uid, new_uid) select o.old_uniqueid, n.uniqueid from one_to_many o, new_to_new n where o.new_targetid = n.targetid;
--5
INSERT INTO associatedsource301(uniqueid, targetid, distance_arcsec, type) SELECT uniqueid, targetid, distance_arcsec,2 FROM new_to_new;

--8 flag_1_to_n_inactive_tempuniq
UPDATE tempuniquecatalog SET inactive = TRUE  WHERE uniqueid IN (SELECT old_uniqueid FROM one_to_many);
--9.insert_1_to_1_assoc
INSERT INTO associatedsource301(uniqueid, targetid, distance_arcsec, type) SELECT t.uniqueid, t.targetid, t.distance_arcsec, 3 FROM tempuniquecatalog t WHERE t.inactive = FALSE order by uniqueid, targetid;

