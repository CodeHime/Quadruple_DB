// jar file 
// ./gradlew build 
package commandline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Scanner;
import java.io.FileWriter;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.PCounter;
import diskmgr.Stream;
import diskmgr.rdfDB;
import global.Convert;
import global.EID;
import global.PID;
import global.QID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import quadrupleheap.*;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;

import java.io.FileInputStream;

public class CommandLine {

	public static void report(String options[]) {
		
		String dbname = options[0];
		SystemDefs sysdef = new SystemDefs(dbname, 0, 1000, "Clock");
		rdfDB database = rdfDB.getInstance();
		int type = Integer.parseInt(dbname.split("_")[1]);
		database.openrdfDB(dbname, type);

		int labelCnt = database.getEntityCnt();
		int subCnt = database.getSubjectCnt();
		int predCnt = database.getPredicateCnt();
		int objCnt = database.getObjectCnt();
		int quadCnt = database.getQuadrupleCnt();

		System.out.print("Label Count:");
		System.out.println(labelCnt);

		System.out.print("Subject Count:");
		System.out.println(subCnt);

		System.out.print("Predicate Count:");
		System.out.println(predCnt);

		System.out.print("Object Count:");
		System.out.println(objCnt);

		System.out.print("Quadruple Count:");
		System.out.println(quadCnt);

	}

	public static void query(String options[]) {
		System.out.println("query");
		String dbname = options[0];
		rdfDB database = rdfDB.getInstance();
		SystemDefs sysdef = new SystemDefs(dbname, 0, 1000, "Clock");
		int type = Integer.parseInt(options[1]);
		database.openrdfDB(dbname, type);

		// int labelCnt = database.getEntityCnt();
		// int subCnt = database.getSubjectCnt();
		// int predCnt = database.getPredicateCnt();
		// int objCnt = database.getObjectCnt();
		// int quadCnt = database.getQuadrupleCnt();

		// // query testDB_1 1 1 null null null null 1
		// System.out.print("Label Count:");
		// System.out.println(labelCnt);

		// System.out.print("Subject Count:");
		// System.out.println(subCnt);

		// System.out.print("Predicate Count:");
		// System.out.println(predCnt);

		// System.out.print("Object Count:");
		// System.out.println(objCnt);

		// System.out.print("Quadruple Count:");
		// System.out.println(quadCnt);

		try {
			Stream stream = database.openStream(Integer.parseInt(options[2]), options[3], options[4], options[5],
					options[6]);
			// QID qid = stream.getFirstQID();
			// for (Quadruple quad = stream.getNext(); quad != null; quad = stream.getNext()) {
			Quadruple quad = stream.getNext();
			while( quad != null){
				// stream.quadover.mvNext(qid);
				// System.out.println(database.getQuadrupleString(quad));
				database.getQuadrupleString(quad);
				quad = stream.getNext();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	//:Jorunn_Danielsen :knows :Eirik_Newth		0.5232176791516268
	private static String[] processLine(String line) {
		line = line.replaceAll(" ", "");
		line = line.replaceFirst("\t", "");
		line = line.replaceFirst("\t", ":");
		line = line.replaceFirst(":", "");
		return line.split(":");
	}

	public static void batchinsert(String options[])
			throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException {
		// System.out.println("batch");

		try {
			File f = new File(options[0]);
			String dbname = options[2] + "_" + options[1];
			SystemDefs sysdef = new SystemDefs(dbname, 30, 10, "Clock");
			rdfDB database = rdfDB.getInstance();
			int type = Integer.parseInt(options[1]);
			database.openrdfDB(dbname, type);

			Scanner scanner = new Scanner(f);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();

				String parts[] = processLine(line);

				boolean isPartsGood = true;
				for (int i = 0; i < parts.length; i++)
				{
					if( parts[i].length() < 1)
					{
						isPartsGood = false;
					}
				}

				if(parts.length != 4 || !isPartsGood){
					continue;
				}

				byte quad[] = new byte[32];

				// System.out.println(line);
				EID subjectid = database.insertEntity(parts[0]);
				Convert.setIntValue(subjectid.pageNo.pid, 0, quad);
				Convert.setIntValue(subjectid.slotNo, 4, quad);

				PID predicateid = database.insertPredicate(parts[1]);
				Convert.setIntValue(predicateid.pageNo.pid, 8, quad);
				Convert.setIntValue(predicateid.slotNo, 12, quad);


				EID objectid = database.insertEntity(parts[2]);
				Convert.setIntValue(objectid.pageNo.pid, 16, quad);
				Convert.setIntValue(objectid.slotNo, 20, quad);

				Convert.setDoubleValue(Double.parseDouble(parts[3]), 24, quad);
				QID qid = database.insertQuadruple(quad);
				
				// System.out.println(Arrays.toString(quad));
				// System.out.println(qid.pageNo.pid);
				// System.out.println(qid.slotNo);
			}
			// databases.put(dbname,database);

			// int labelCnt = database.getEntityCnt();
			// int subCnt = database.getSubjectCnt();
			// int predCnt = database.getPredicateCnt();
			// int objCnt = database.getObjectCnt();
			// int quadCnt = database.getQuadrupleCnt();

			// System.out.print("Label Count:");
			// System.out.println(labelCnt);

			// System.out.print("Subject Count:");
			// System.out.println(subCnt);

			// System.out.print("Predicate Count:");
			// System.out.println(predCnt);

			// System.out.print("Object Count:");
			// System.out.println(objCnt);

			// System.out.print("Quadruple Count:");
			// System.out.println(quadCnt);


			System.out.println("BatchInsert Successful");
			scanner.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	public static void getInput() throws InvalidPageNumberException, FileIOException, DiskMgrException {

		int end = 0;
		while (end == 0) {

			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

			try {
				FileWriter fw;
				// String input = "batchinsert phase2_test_data.txt 1 testDB"; 
				String input =  in.readLine();
				String parsed[] = input.split(" ");
				PCounter.initialize();

				// if (parsed[0].equals("report") && parsed.length == 1) {
				if (parsed[0].equals("report") && parsed.length == 2) {
					report(Arrays.copyOfRange(parsed, 1, parsed.length));
					File f = new File("logfile.txt");
					Scanner scan = new Scanner(f);
					while (scan.hasNextLine()) {
						System.out.println(scan.nextLine());
					}
					scan.close();
				} else if (parsed[0].equals("query") && parsed.length == 9) {
					Long startTime = new java.util.Date().getTime();
					query(Arrays.copyOfRange(parsed, 1, parsed.length));
					Long endTime = new java.util.Date().getTime();
					// fw = new FileWriter(parsed[1]+"_"+parsed[2]);
					fw = new FileWriter("logfile.txt", true);
					
					fw.append(input + "\t");
					fw.append("Execution Time " + Long.toString(endTime - startTime) + "\t");
					fw.append("Reads " + PCounter.rcounter + "\tWrites: " + PCounter.wcounter + "\n\n");
					fw.close();
					System.out.println("Reads " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
				} else if (parsed[0].equals("batchinsert") && parsed.length == 4) {
					Long startTime = new java.util.Date().getTime();
					batchinsert(Arrays.copyOfRange(parsed, 1, parsed.length));
					Long endTime = new java.util.Date().getTime();
					// fw = new FileWriter(parsed[3]+"_"+parsed[2]);
					fw = new FileWriter("logfile.txt", true);
					fw.append(input + "\t");
					fw.append("Execution Time " + Long.toString(endTime - startTime) + "\t");
					fw.append("Reads " + PCounter.rcounter + "\tWrites: " + PCounter.wcounter + "\n\n");
					fw.close();
					System.out.println("Reads " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
				} else if (parsed[0].equals("exit") || parsed[0].equals("quit") || parsed[0].equals("q")) {
					end = 1;
				} else {
					System.out.println("Unrecgonized command. Leave with 'exit'.");
				}

			} catch (IOException e) {
				System.out.println("error");
			}
		}
	}

	public static void main(String[] argvs) throws InvalidPageNumberException, FileIOException, DiskMgrException {
		// System.out.println("Hello, It's working");
		getInput();
	}
}
