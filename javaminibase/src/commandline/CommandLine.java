// jar file 
// ./gradlew build 
package commandline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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
import quadrupleheap.Quadruple;

public class CommandLine {

	public static void query(String options[]) {
		System.out.println("query");

		String dbname = options[0] + "_" + options[1];
		rdfDB database = rdfDB.getInstance();
		int type = Integer.parseInt(options[1]);
		database.openrdfDB(dbname, type);

		Stream stream = database.openStream(Integer.parseInt(options[2]), options[3], options[4], options[5],
				options[6]);
		try {
			QID qid = stream.getFirstQID();
			for (Quadruple quad = stream.getNext(qid); quad != null; quad = stream.getNext(qid)) {
				stream.quadover.mvNext(qid);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void batchinsert(String options[])
			throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException {
		System.out.println("batch");

		try {
			File f = new File(options[0]);
			String dbname = options[2] + "_" + options[1];
			rdfDB database = rdfDB.getInstance();
			int type = Integer.parseInt(options[1]);
			database.openrdfDB(dbname, type);

			Scanner scanner = new Scanner(f);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();

				String parts[] = line.replace('\t', ':').replaceAll(" ", "").split(":");

				byte quad[] = new byte[32];
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
				database.insertQuadruple(quad);
			}
			// databases.put(dbname,database);

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
				String input = in.readLine();
				String parsed[] = input.split(" ");
				PCounter.initialize();

				if (parsed[0].equals("report") && parsed.length == 1) {
					File f = new File("../logfile.txt");
					Scanner scan = new Scanner(f);
					while (scan.hasNextLine()) {
						System.out.println(scan.nextLine());
					}
					scan.close();
				} else if (parsed[0].equals("query") && parsed.length == 9) {
					query(Arrays.copyOfRange(parsed, 1, parsed.length));
					// fw = new FileWriter(parsed[1]+"_"+parsed[2]);
					fw = new FileWriter("../logfile.txt");
					fw.write(input + "\n");
					fw.write("Reads " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
					fw.close();
					System.out.println("Reads " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
				} else if (parsed[0].equals("batchinsert") && parsed.length == 4) {
					batchinsert(Arrays.copyOfRange(parsed, 1, parsed.length));
					// fw = new FileWriter(parsed[3]+"_"+parsed[2]);
					fw = new FileWriter("../logfile.txt");
					fw.write(input + "\n");
					fw.write("Reads " + PCounter.rcounter + "\nWrites: " + PCounter.wcounter + "\n\n");
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
		getInput();
	}
}
