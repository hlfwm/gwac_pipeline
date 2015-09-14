--every night after image aource association pipeline finished, re-sort associatedsource table for ligthcurve lookup.
drop table temp1;
create table temp1 (like associatedsource302);
--insert into temp1 select * from associatedsource3 order by uniqueid, targetid;
insert into temp1 select * from associatedsource302;
--drop table associatedsource3;
--create table associatedsource3 (like temp1);
--nsert into associatedsource3 select * from temp1;
--sort uidlegacy
drop table temp2;
create table temp2 (like uidlegacy);
insert into temp2 select * from uidlegacy order by old_uid,new_uid;
drop table uidlegacy;
create table uidlegacy (like temp2);
insert into uidlegacy select * from temp2;

select count(*) from associatedsource3;
select count(*) from uniquecatalog3;
select max(id) from uniquecatalog3;
\w
select * from storage() where "table"='associatedsource302';
select count(*) from uidlegacy;

--select * from associatedsource3 group by uniqueid, targetid having count(*) >1;
select count(*) from uidlegacy group by old_uid, new_uid having count(*) >1;

drop table temp3;
create table temp3 (like uniquecatalog3);
insert into temp3 select * from uniquecatalog3;
drop table uniquecatalog3 cascade;
create table uniquecatalog3(like temp3);
insert into uniquecatalog3 select * from temp3;
alter table uniquecatalog3 alter column id set default next value for unique_seq;
alter table uniquecatalog3 add primary key(id);
