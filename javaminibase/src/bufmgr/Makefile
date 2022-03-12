JDKPATH = /usr/lib/jvm/jdk-17.0.2/
LIBPATH = /home/pavan/DBMSI/CSE510_Project/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

bm:BufMgr.java
	$(JAVAC) BufMgr.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
