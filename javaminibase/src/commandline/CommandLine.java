// jar file 
// ./gradlew build 
package commandline;

import global.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import basicpattern.BPOrder;
import basicpattern.BPSort;
import basicpattern.BP_Triple_Join;
import basicpattern.BasicPattern;
import basicpattern.BasicPatternIteratorScan;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;

import java.io.FileWriter;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.OutOfSpaceException;
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

		System.out.print("Entity Count:");
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

		SystemDefs sysdef = new SystemDefs(dbname, 0, Integer.parseInt(options[options.length - 1]), "Clock");
		int type = Integer.parseInt(options[1]);
		database.openrdfDB(dbname, type);

		// int labelCnt = database.getEntityCnt();
		// int subCnt = database.getSubjectCnt();
		// int predCnt = database.getPredicateCnt();
		// int objCnt = database.getObjectCnt();
		// int quadCnt = database.getQuadrupleCnt();

		// // query testDB_1 1 1 * * * * 1
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
		Stream stream = null;
		try {
			stream = database.openStream(Integer.parseInt(options[2]), options[3], options[4], options[5],
					options[6]);
			// QID qid = stream.getFirstQID();
			// for (Quadruple quad = stream.getNext(); quad != null; quad =
			// stream.getNext()) {
			// INSIGHT: for #times query: #total writes <
			// BufferSize-SortBufferSize-#write/iteration, else error
			int quadCount = 0;
			Quadruple quad = stream.getNext();
			while (quad != null) {
				try {
					database.getQuadrupleString(quad);
					quad = stream.getNext();
					quadCount++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println("Number of Quadruples: "+Integer.toString(quadCount));
			stream.closestream();
			sysdef.JavabaseBM.flushAllPages();

		} catch (Exception e) {
			e.printStackTrace();
			if (stream != null) {
				stream.closestream();
			}
		}

	}

	// :Jorunn_Danielsen :knows :Eirik_Newth 0.5232176791516268
	private static String[] processLine(String line) {

		char[] characters = line.toCharArray();

		String[] quadComponents = new String[4];
		String component = "";
		int componentIndex = 0;
		for (char c : characters) {
			if ((c >= 48 && c <= 57) || (c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c == 95 || c == 46
					|| c == 45) {
				component += c;
			} else {
				if (!component.equals("")) {
					quadComponents[componentIndex] = component;
					componentIndex += 1;

					// System.out.println(component);
					component = "";
				}
			}
		}

		if (!component.equals("")) {
			quadComponents[componentIndex] = component;
		}

		// line = line.replaceAll(" ", "");
		// line = line.replaceFirst("\t", "");
		// line = line.replaceFirst("\t", ":");
		// line = line.replaceFirst(":", "");
		// return line.split(":");
		return quadComponents;
	}

	public static void batchinsert(String options[])
			throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException {
		// System.out.println("batch");

		try {
			File f = new File(options[0]);
			String dbname = options[2] + "_" + options[1];

			File dbfile = new File(dbname); // Check if database already exist
			SystemDefs sysdef;
			if (dbfile.exists()) {
				// Database already present just open it
				sysdef = new SystemDefs(dbname, 0, 1000, "Clock");
				// System.out.println("*** Opening existing database ***");
				// existingdb = true;
			} else {
				// Create new database
				sysdef = new SystemDefs(dbname, 10000, 1000, "Clock");
				// System.out.println("*** Creating existing database ***");
			}

			// SystemDefs sysdef = new SystemDefs(dbname, 1000, 100, "Clock");
			rdfDB database = rdfDB.getInstance();
			int type = Integer.parseInt(options[1]);
			database.openrdfDB(dbname, type);

			Scanner scanner = new Scanner(f);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();

				String parts[] = processLine(line);

				for (String p : parts) {
					// System.out.print(p);
				}
				// System.out.println("");
				boolean isPartsGood = true;
				for (int i = 0; i < parts.length; i++) {
					if (parts[i] == null) {
						isPartsGood = false;
					}
				}

				if (parts.length != 4 || !isPartsGood) {
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
			try {
				sysdef.JavabaseBM.flushAllPages();
			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	// batchinsert phase3_test_data_small.txt 1 testDB
	// query testDB_1 query1.txt 1000
	public static void query2(String options[]) throws IOException {
		String dbname = options[0];
		rdfDB database = rdfDB.getInstance();

		SystemDefs sysdef = new SystemDefs(dbname, 0, Integer.parseInt(options[options.length - 2]), "Clock");
		int type = Integer.parseInt(options[0].split("_")[1]);
		database.openrdfDB(dbname, type);

		// Parse join text file

		String queryfile = options[1];

		String query = new String(Files.readAllBytes(Paths.get(queryfile)));

		// Remove newlines and whitespaces
		// query = query.replace("\n", "").replace("\r", "");
		query = query.replace("\n", "").replace("\r", "").replace("\t", "");
		query = query.replaceAll("\\s+", "");

		String[] strs = query.split("\\),");
		String[] firstStrs = strs[0].split("],");
		String[] temp1 = firstStrs[0].split("\\[");
		String[] temp2 = temp1[1].split(",");
		stringProcess(temp2);

		String SubjectFilter = temp2[0], PredicateFilter = temp2[1], ObjectFilter = temp2[2],
				ConfidenceFilter = temp2[3];

		// JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
		String[] temp3 = firstStrs[1].split(",");
		int BPJoinNodePosition = Integer.parseInt(temp3[0]);
		int JoinOnSubjectorObject = Integer.parseInt(temp3[1]);
		String RightSubjectFilter = stringProcess(temp3[2]);
		String RightPredicateFilter = stringProcess(temp3[3]);
		String RightObjectFilter = stringProcess(temp3[4]);
		String RightConfidenceFilter = temp3[5];

		List<Integer> lonpAL = new ArrayList<Integer>();

		// If length is 9 then there is one or zero indexs for LONP given
		if (temp3.length != 9) {
			int i = 6;

			// drop the bracket of the first LONP value
			temp3[i] = temp3[i].substring(1);

			while (!temp3[i].contains("}")) {
				lonpAL.add(Integer.parseInt(temp3[i]));
				i++;
			}
			// remove closing }
			lonpAL.add(Integer.parseInt(temp3[i].substring(0, 1)));
		}
		// There is one index given, add the just the int without the brackets
		else if (temp3[6].length() > 2) {
			lonpAL.add((Integer.parseInt(temp3[6].substring(1, temp3[6].indexOf("}")))));
		}

		int[] LeftOutNodePositions = lonpAL.stream().mapToInt(i -> i).toArray();

		int OutputRightSubject = Integer.parseInt(temp3[temp3.length - 2]);
		int OutputRightObject = Integer.parseInt(temp3[temp3.length - 1]);

		String[] secondStrs = strs[1].split("\\)");

		// JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
		String[] secondSet = secondStrs[0].split(",");
		// stringProcess(secondSet);
		// System.out.println(Arrays.toString(secondSet));

		Integer BPJoinNodePosition2 = Integer.parseInt(secondSet[0]);
		Integer JoinOnSubjectorObject2 = Integer.parseInt(secondSet[1]);
		String RightSubjectFilter2 = stringProcess(secondSet[2]);
		String RightPredicateFilter2 = stringProcess(secondSet[3]);
		String RightObjectFilter2 = stringProcess(secondSet[4]);
		String RightConfidenceFilter2 = secondSet[5];

		// List<Integer> lonp2 = processLONP(secondSet[6]);
		List<Integer> lonpAL2 = new ArrayList<Integer>();

		// If length is 9 then there is 1 or 0 indexes for LONP given
		if (secondSet.length != 9) {
			int i = 6;

			// drop the bracket of the first LONP value
			secondSet[i] = secondSet[i].substring(1);

			while (!secondSet[i].contains("}")) {
				lonpAL2.add(Integer.parseInt(secondSet[i]));
				i++;
			}
			// remove closing }
			lonpAL2.add(Integer.parseInt(secondSet[i].substring(0, secondSet[i].indexOf("}"))));
		}
		// There is one index given, add the just the int without the brackets
		else if (secondSet[6].length() > 2) {
			lonpAL2.add((Integer.parseInt(secondSet[6].substring(1, secondSet[6].indexOf("}")))));
		}

		int[] LeftOutNodePositions2 = lonpAL2.stream().mapToInt(i -> i).toArray();

		int OutputRightSubject2 = Integer.parseInt(secondSet[secondSet.length - 2]);
		int OutputRightObject2 = Integer.parseInt(secondSet[secondSet.length - 1]);

		String[] lastStrs = secondStrs[1].split(",");
		int sort_order = Integer.parseInt(lastStrs[0]), SortNodeIDPos = Integer.parseInt(lastStrs[1]),
				n_pages = Integer.parseInt(lastStrs[2]);

		int amt_of_mem = Integer.parseInt(options[2]);

		try {
			String joinTypeFileNames[] = { "basic_nlj", "basic_index_nlj", "bsi_nlj", "smj" };
			int iterations[] = { 1, 2, 3, 4 };
			int iterationsLength = 3;
			if (Integer.parseInt(options[3]) != 5) {
				iterations[0] = Integer.parseInt(options[3]);
				iterationsLength = 1;
				joinTypeFileNames[0] = joinTypeFileNames[Integer.parseInt(options[3]) - 1];
			}

			for (int i = 0; i < iterationsLength; i++) {
				// Get left iterator with sorted values
				Stream tempStream;
				if (iterations[i] != 3) {
					tempStream = new Stream(database, SubjectFilter, PredicateFilter, ObjectFilter,
							ConfidenceFilter);
				} else {
					tempStream = new Stream(database, sort_order, SubjectFilter,
							PredicateFilter, ObjectFilter, ConfidenceFilter);
				}
				tempStream = new Stream(database, SubjectFilter, PredicateFilter, ObjectFilter,
							ConfidenceFilter);
				BasicPatternIteratorScan left_itr = new BasicPatternIteratorScan(tempStream.getResults().getFilename());

				// ---------------------------------------------------------------------------------------------
				// Perform first join
				BP_Triple_Join btj = new BP_Triple_Join(amt_of_mem, 3, left_itr, BPJoinNodePosition,
						JoinOnSubjectorObject,
						RightSubjectFilter, RightPredicateFilter, RightObjectFilter, RightConfidenceFilter,
						LeftOutNodePositions, OutputRightSubject, OutputRightObject);
				System.out.println("BP_Triple_Join:\n---------------");
				System.out.println("Reads: " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n");

				btj.runJoinType(iterations[i]);
				System.out.println("1st " + joinTypeFileNames[i] + ":\n---------------------");
				System.out.println("Reads: " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n");

				// Reset left iterator
				left_itr = new BasicPatternIteratorScan(left_itr.getFileName() + joinTypeFileNames[i],
						btj.getNumLeftNodes());

				// // Test code: DELETE or COMMENT
				// //------------------------------------------------
				// if (left_itr.getHeapFile().getRecCnt() > 1)
				// {
				// 	// Print results
				// 	BasicPattern testBP = left_itr.get_next();
				// 	int bpTestCount = 0;					
				// 	while (testBP != null) {
				// 		bpTestCount++;
				// 		testBP.print();
				// 		testBP = left_itr.get_next();
				// 	}
				// 	System.out.println(bpTestCount);

				// 	Stream rstream;
				// 	if (JoinOnSubjectorObject == 0) {
				// 		rstream = new Stream(database, QuadrupleOrder.SubjectConfidence, RightSubjectFilter, RightPredicateFilter,
				// 				RightObjectFilter, RightConfidenceFilter);
				// 	} else {
				// 		rstream = new Stream(database, QuadrupleOrder.ObjectConfidence, RightSubjectFilter, RightPredicateFilter,
				// 				RightObjectFilter, RightConfidenceFilter);
				// 	}

				// 	Quadruple testQuad = rstream.getNext();
				// 	int quadTestCount = 0;					
				// 	while (testQuad != null) {
				// 		quadTestCount++;
				// 		String subject = database.getEntityHeapFile().getLabel(testQuad.getSubjectQid().returnLID()).getLabel();
				// 		String predicate = database.getPredicateHeapFile().getLabel(testQuad.getPredicateID().returnLID()).getLabel();
				// 		String object = database.getEntityHeapFile().getLabel(testQuad.getObjectQid().returnLID()).getLabel();
				// 		System.out.println("["+Double.toString(testQuad.getConfidence())+" "+subject+" "+predicate+" "+object+"]");
				// 		testQuad = rstream.getNext();
				// 	}
				// 	System.out.println(bpTestCount);
				// }
				// //------------------------------------------------

				// Perform second join
				btj = new BP_Triple_Join(amt_of_mem, btj.getNumLeftNodes(), left_itr, BPJoinNodePosition2,
						JoinOnSubjectorObject2, RightSubjectFilter2, RightPredicateFilter2, RightObjectFilter2,
						RightConfidenceFilter2, LeftOutNodePositions2, OutputRightSubject2, OutputRightObject2);
				btj.runJoinType(iterations[i]);
				System.out.println("2nd " + joinTypeFileNames[i] + ":\n---------------------");
				System.out.println("Reads: " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n");

				// Reset left iterator
				left_itr = new BasicPatternIteratorScan(left_itr.getFileName() + joinTypeFileNames[i],
						btj.getNumLeftNodes());

				int bpCount = 0;
				if (left_itr.getHeapFile().getRecCnt() > 1)
				{
					// Print results
					BPSort sort = new BPSort(left_itr, new BPOrder(sort_order), SortNodeIDPos, n_pages);

					BasicPattern bp = sort.get_next();
					// BasicPattern bp = left_itr.get_next();
					
					while (bp != null) {
						bpCount++;
						bp.print();
						bp = sort.get_next();
						// bp = left_itr.get_next();
					}
				}

				System.out.println("Final Count: " + Integer.toString(bpCount));
				System.out.println("Query Complete!");
			}
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	public static void deleteDB(String db) {
		File dbf = new File(db);
		dbf.delete();
	}

	public static void getInput() throws InvalidPageNumberException, FileIOException, DiskMgrException {

		int end = 0;
		while (end == 0) {

			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

			try {
				FileWriter fw;
				// String input = "batchinsert phase2_test_data.txt 1 testDB";
				String input = in.readLine();
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
				} else if (parsed[0].equals("delete") && parsed.length == 2) {
					deleteDB(parsed[1]);
				} else if (parsed[0].equals("query") && parsed.length == 9) {
					Long startTime = new java.util.Date().getTime();
					query(Arrays.copyOfRange(parsed, 1, parsed.length));
					Long endTime = new java.util.Date().getTime();
					// fw = new FileWriter(parsed[1]+"_"+parsed[2]);
					fw = new FileWriter("logfile.txt", true);

					fw.append(input + "\t");
					fw.append("Execution Time: " + Long.toString(endTime - startTime) + "\t");
					fw.append("Reads: " + PCounter.rcounter + "\tWrites: " + PCounter.wcounter + "\n");
					fw.close();
					System.out.println("Reads: " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
				} else if (parsed[0].equals("batchinsert") && parsed.length == 4) {
					Long startTime = new java.util.Date().getTime();
					batchinsert(Arrays.copyOfRange(parsed, 1, parsed.length));
					Long endTime = new java.util.Date().getTime();
					// fw = new FileWriter(parsed[3]+"_"+parsed[2]);
					fw = new FileWriter("logfile.txt", true);
					fw.append(input + "\t");
					fw.append("Execution Time: " + Long.toString(endTime - startTime) + "\t");
					fw.append("Reads: " + PCounter.rcounter + "\tWrites: " + PCounter.wcounter + "\n");
					fw.close();
					System.out.println("Reads: " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
				} else if (parsed[0].equals("query") && parsed.length == 5) {
					Long startTime = new java.util.Date().getTime();
					query2(Arrays.copyOfRange(parsed, 1, parsed.length));

					Long endTime = new java.util.Date().getTime();
					fw = new FileWriter("logfile.txt", true);
					fw.append(input + "\t");
					fw.append("Execution Time: " + Long.toString(endTime - startTime) + "\t");
					fw.append("Reads: " + PCounter.rcounter + "\tWrites: " + PCounter.wcounter + "\n");
					fw.close();
					System.out.println("Reads: " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
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

	// helper for processing Query file
	private static void stringProcess(String[] strs) {
		for (int i = 0; i < strs.length - 1; i++) {
			if (!strs[i].equals("*")) {
				strs[i] = strs[i].substring(1, strs[i].length() - 1);
			}
		}
	}

	// helper for processing Query file
	private static String stringProcess(String str) {
		if (!str.equals("*")) {
			str = str.substring(1, str.length() - 1);
		}

		return str;
	}

	public static void main(String[] argvs) throws InvalidPageNumberException, FileIOException, DiskMgrException {
		// System.out.println("Hello, It's working");
		getInput();
	}
}
