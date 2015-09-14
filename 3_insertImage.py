import astropy.time
import numpy as np
import sys,os,socket

gcal="2010-04-13 00:00:00"
gdate0=astropy.time.Time(gcal,format="iso", scale="utc")

startimgno=int(sys.argv[1])
stopimgno=int(sys.argv[2])
imgnum=stopimgno-startimgno+1

day1=startimgno/2400
dayn=stopimgno/2400
dt=astropy.time.TimeDelta(day1*86400, format='sec')
gdate00=gdate0+dt

#15 sec * 2400 is one night observation time 10 hour
dt0=astropy.time.TimeDelta(15*2400, format='sec')
tt=[]
for day in range(day1, dayn):
    tt.append(gdate00+dt0*np.linspace(0,1,2400))
    #the second day, jd should be added by 1 jd day!
    gdate00=gdate00+astropy.time.TimeDelta(1, format='jd')
#tt is two dimentinal array,tt[x][y], x is day count, y is 0:2399!

dd=[]
for dayi in range(len(tt)):
    for y in range(0, 2400):
        dd.append(tt[dayi][y].jd)

#np.savetxt("imagejd.txt",dd)

machine_tableno = {
    'stones01.scilens.private' :   0,
    'stones02.scilens.private' :   1,
    'stones03.scilens.private' :   2,
    'stones04.scilens.private' :   3,
    'stones05.scilens.private' :   4,
    'stones06.scilens.private' :   5,
    'stones07.scilens.private' :   6,
    'stones08.scilens.private' :   7,
    'stones09.scilens.private' :   8,
    'stones10.scilens.private' :   9,
    'stones11.scilens.private' :   10,
    'stones12.scilens.private' :   11,
    'stones13.scilens.private' :   12,
    'stones14.scilens.private' :   13,
    'stones15.scilens.private' :   14,
    'stones16.scilens.private' :   15,
}
tblno = machine_tableno[socket.gethostname()]
#if startimgno == 1:
#    os.system("mclient gwacdb -e -s \"create sequence image_seq as int start with 52801\"")
#    os.system("mclient gwacdb -e -s \"create table image%d(imageid int DEFAULT NEXT VALUE FOR image_seq, skyznid int, jd double, exptime date, filepath char(100), fwhm_pixel double, Rm double, PRIMARY KEY (imageid))\"" %tblno)


os.system("mclient gwacdb -e -s \"alter sequence image_seq restart with 19201\"")
ii=range(startimgno, stopimgno+1)

for x in range(imgnum):
    cmd="mclient gwacdb -e -s \"insert into image3(imageid, jd) values(%d,%f)\" " %(ii[x],dd[x])
    os.system(cmd)
