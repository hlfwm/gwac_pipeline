#!/usr/bin/python
import os, sys, socket

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
mypath=os.getcwd()
included_ext=['sql','sh','py']
filenames=[fn for fn in os.listdir(mypath) if any([fn.endswith(ext) for ext in included_ext])]
for fn in filenames: 
    os.system("sed -i 's/uniquecatalog[[:digit:]]\+/uniquecatalog%d/g' %s" %(tblno, fn));
    os.system("sed -i 's/targets[[:digit:]]\+/targets%d/g' %s" %(tblno, fn));
    os.system("sed -i 's/associatedsource[[:digit:]]\+/associatedsource%d/g' %s" %(tblno, fn));
