# modify the paths appropriately
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)



# test btree

Compile:
	$(JAVAC) CommandLine.java

command_line: Compile
	$(JAVA) command_line.CommandLine

