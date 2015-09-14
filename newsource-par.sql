--11. insert_new_uniq(imageno int)
INSERT INTO uniquecatalog3(targetid,datapoints,zone,ra_avg,decl_avg,flux_ref,x,y,z,inactive) SELECT t.id AS targetid, 1 AS datapoints ,t.zone, t.ra AS ra_avg, t."dec" AS decl_avg, t.flux AS flux_ref, t.x, t.y, t.z, FALSE FROM newsrc, targets3 t WHERE newsrc.targetid = t.id;

--12 insert_new_assoc(imageno int)
INSERT INTO associatedsource301(uniqueid, targetid, distance_arcsec, type) SELECT u.id AS uniqueid, newsrc.targetid, 0, 4 FROM newsrc, uniquecatalog3 u WHERE newsrc.targetid = u.targetid;

--13. delete_inactive_unique
DELETE FROM uniquecatalog3 WHERE inactive = TRUE;
