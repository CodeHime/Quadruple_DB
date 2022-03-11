package command_line;

import java.io.*;
import java.util.*;
import java.lang.*;


public class CommandLine{
   
   
  //private static HashMap<String,rdfDB> databases = new HashMap<String,rdfDB>();
  public static void report(){
  	System.out.println("Reads: "+PCounter.rcounter+"\nWrites: "+PCounter.wcounter);
  	
  }
  
  public static void query(String options[]){
  	System.out.println("query");
  	
  	//rdfDB database;
	String dbname = options[0]+"_"+options[1];
	if (databases.containsKey(dbname)){
		database = databases.get(dbname);
	}
	else{
		System.out.println("Can not find the database");
	}
	
	Stream stream = database.openStream(options[2],options[3],options[4],options[5],options[6]);
	
	for (QID qid = stream.getNext();qid != null; qid = stream.getNext()){
		
	}
	
	report();
  }
  
  public static void batchinsert(String options[]){
  	System.out.println("batch");
  	
  	
  	try {
		File f = new File(options[0]);
		//rdfDB database;
		String dbname = options[2]+"_"+options[1];
		if (databases.containsKey(dbname)){
			database = databases.get(dbname);
		}
		else{
	  	//database = new rdfDB(Integer.parseInt(options[1]));
	  	}
	      Scanner scanner = new Scanner(f);
	      while (scanner.hasNextLine()) {
		String line = scanner.nextLine();

		String parts[] = line.replace('\t',':').replaceAll(" ","").split(":");
		
		
		byte quad[] = new byte[28];
		EID subjectid = database.insertEntity(parts[0]);
		Convert.setIntValue(subjectid.pageNo.pid,0,quad);
		Convert.setIntValue(subjectid.slotNo,4,quad);
		
		PID predicateid = database.insertPredicate(parts[1]);
		Convert.setIntValue(predicateid.pageNo.pid,8,quad);
		Convert.setIntValue(predicateid.slotNo,12,quad);
		
		EID objectid = database.insertEntity(parts[2]);
		Convert.setIntValue(objectid.pageNo.pid,16,quad);
		Convert.setIntValue(objectid.slotNo,20,quad);
		
		Convert.setFloValue(Float.parseFloat(parts[3], 24, quad); 
		database.insertQuadruple(quad);
		}
		//databases.put(dbname,database);
	      
	      scanner.close();
	    } catch (FileNotFoundException e) {
	      System.out.println("An error occurred.");
	      e.printStackTrace();
	    }
  	
  	report();
  
  	
  }
   
   
  public static void getInput(){
  
  	int end = 0;
  	while(end == 0){
  
  	BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
	
	try {
	   String input = in.readLine();
	   String parsed[] = input.split(" ");
	   PCounter.initialize();
	   
	   if (parsed[0].equals("report")&&parsed.length == 1){
	   	report();
	   }
	   else if (parsed[0].equals("query")&&parsed.length == 9){
	   	query(Arrays.copyOfRange(parsed,1,parsed.length));
	   }
	   else if (parsed[0].equals("batchinsert")&&parsed.length == 4){
	   	batchinsert(Arrays.copyOfRange(parsed,1,parsed.length));
	   }
	   else if (parsed[0].equals("exit")||parsed[0].equals("quit")||parsed[0].equals("q")){
		end = 1;	
	   }
	   else{
	   	System.out.println("Unrecgonized command. Leave with 'exit'.");
	   }
	   
	}
	catch (IOException e) {
		System.out.println("error");
	}
  	}
  }
  
  public static void main(String [] argvs) {
 
   getInput();
  }
  
}
