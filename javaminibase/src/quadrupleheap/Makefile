CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

hf:Heapfile.java
	$(JAVAC) QuadrupleHeapfile.java

hp:HFPage.java
	$(JAVAC) THFPage.java

tuple:Tuple.java
	$(JAVAC) Quadruple.java

scan:Scan.java
	$(JAVAC) TScan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
