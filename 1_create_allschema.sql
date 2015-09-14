--prepare all the schema used for gwac pipeline
--1. uniquecatalog related schemas.
create table uniquecatalogg(targetid bigint, ra_avg double, decl_avg double, flux_ref double, zone smallint, x double, y double, z double);
drop sequence "unique_seq";
create sequence "unique_seq" as bigint;

create table uniquecatalog3(id bigint PRIMARY KEY 
DEFAULT NEXT VALUE FOR "unique_seq"
, targetid bigint  --the first target id of the uniqueid.
, ra_avg double
, decl_avg double
, flux_ref double
, datapoints int
, zone smallint
, x double
, y double
, z double
, INACTIVE BOOLEAN
);

--3. other schema.
create sequence "sky_seq" as int;
create table skyzone(skyznid int PRIMARY KEY DEFAULT NEXT VALUE FOR "sky_seq", center_ra double, center_dec double, se_radius double);

create sequence "image_seq" as int;
create table image3(imageid int DEFAULT NEXT VALUE FOR "image_seq", skyznid int, jd double, exptime date, filepath char(100), fwhm_pixel double, Rm double, PRIMARY KEY (imageid));

create table associatedsource301(uniqueid bigint,targetid bigint, type tinyint, distance_arcsec double);
--create table associatedsource(uniqueid int,targetid int, type int, distance_arcsec double, FOREIGN KEY (uniqueid) REFERENCES uniquecatalog3(id) ON DELETE CASCADE);
--create table associatedsource(uniqueid int, targetid int, type int, distance_arcsec double, PRIMARY KEY (uniqueid, targetid), FOREIGN KEY (uniqueid) REFERENCES uniquecatalog3(id), FOREIGN KEY (targetid) REFERENCES targets3(id));

create table transient(id serial, uniqueid bigint, alert_targetid int, FOREIGN KEY (uniqueid) REFERENCES uniquecatalog3(id), FOREIGN KEY (alert_targetid) REFERENCES targets3(id));
--serial type is already a primary key, not need to specify primary key.

--create table tempuniquecatalog(uniqueid int, targetid int, distance_arcsec double, datapoints int, zone smallint, ra_avg double, decl_avg double, x double, y double, z double, INACTIVE BOOLEAN, flux_ref double, flux_tgs double, PRIMARY KEY(uniqueid, targetid));
create table tempuniquecatalog(uniqueid bigint, targetid bigint, distance_arcsec double, datapoints int, zone smallint, ra_avg double, decl_avg double, x double, y double, z double, flux_ref double, flux_tgs double, inactive BOOLEAN);

--create table u0_zone (id int, targetid int, ra_avg double, decl_avg double, flux_ref double, datapoints int,zone smallint,x double, y double, z double);

--initialize uniquecatalog3 and associatedsource
insert into uniquecatalogg select id,ra,"dec",flux,zone,x,y,z from targets3 where imageid=1;
insert into uniquecatalog3(targetid, ra_avg, decl_avg, flux_ref,zone,x,y,z) select * from uniquecatalogg;
update uniquecatalog3 set datapoints=1, inactive=FALSE;
insert into associatedsource301 select id, id , 3, 0 from uniquecatalog3;

CREATE TABLE one_to_many (old_uniqueid bigint, new_targetid bigint, distance_arcsec double);
CREATE TABLE new_to_new (uniqueid bigint, targetid bigint, distance_arcsec double);
--CREATE TABLE old_to_old (uniqueid int, targetid int, distance_arcsec double);
--create table smalla(uniqueid int, targetid int, distance_arcsec double, type int, PRIMARY KEY (uniqueid, targetid));
--create table u0_zone (id int, targetid int, ra_avg double, decl_avg double, flux_ref double, datapoints int,zone smallint,x double, y double, z double);
create TABLE newsrc(targetid bigint);
--uidlegacy is to store deleted old uniqueid and their children(replacement) uniqueid in one_to_many scenario
create table uidlegacy(old_uid bigint, new_uid bigint);
