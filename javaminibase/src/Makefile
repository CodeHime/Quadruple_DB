#This Makefile is used to create assignments.

#the following macros should be updated according to
#the assignment to be generated

JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class\
         heap/*.class quadrupleheap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class

# EDIT 1: Set java path to your java jdk dir
JDKPATH = /usr/lib/jvm/jdk-17.0.2
#JDKPATH = ${JDKPATH}
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

DOCFILES=bufmgr diskmgr global chainexception heap quadrupleheap btree iterator index

##############  update the above for each assignment in making

# EDIT 2: Set ASSIGN to your javaminibase src dir
#ASSIGN=/home/candan/Courses/CSE510spr02/minjava/javaminibase/src
ASSIGN=~/Downloads/minjava/javaminibase/src
LIBDIR=$(ASSIGN)/lib
KEY=$(ASSIGN)/key
SRC=$(ASSIGN)/src

IMAGELINK=$(PACKAGEINDEX)/images
PACKAGEINDEX=$(ASSIGN)/javadoc

JAVADOC=javadoc -public -d $(PACKAGEINDEX)

### Generate jar and javadoc files.  Apply to most assignments.
db: 
	make -C global
	make -C chainexception
	make -C btree
	make -C bufmgr
	make -C diskmgr
	make -C heap
	make -C quadrupleheap
	make -C iterator
	
doc:
	$(JAVADOC) $(DOCFILES)

test: 
	cd tests; make bmtest dbtest; whoami; make hftest bttest indextest jointest sorttest sortmerge
	
cl:
	cd command_line; make command_line

clean:
	\rm -f $(CLASSPATH)/*.class *~ \#* core $(JARFILES) TRACE

