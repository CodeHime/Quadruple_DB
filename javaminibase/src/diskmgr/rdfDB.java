/* File rdfDB.java */

package diskmgr;

import java.util.ArrayList;
import java.util.Arrays;

import btree.*;
import global.*;
import labelheap.*;
import quadrupleheap.*;

public class rdfDB extends DB {

    private static rdfDB rdfdb;
    private LabelHeapfile entity_heap_file; 
    private LabelHeapfile predicate_heap_file;
    private QuadrupleHeapfile quad_heap_file;
    
    public String rdfDB_name;
    int indexOption;

    public LabelHeapfile getEntityHeapFile() { return entity_heap_file; }
    public LabelHeapfile getPredicateHeapFile() { return predicate_heap_file; }
    public QuadrupleHeapfile getQuadHeapFile() { return quad_heap_file; }

    public String getName() { return rdfDB_name;}
    public int getIndexOption() { return indexOption; }


    private rdfDB() {}

    public static rdfDB getInstance( ) {
      if (rdfdb == null) {
        rdfdb = new rdfDB();
      } 

      return rdfdb;
    }

    public void openrdfDB(String name, int type) {
        rdfDB_name = name + "_" + Integer.toString(type);
        try {
            openDB(name);
            
            createRDFDB(type);
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        
    }

    public void openrdfDB(String name, int pages, int type) {
        rdfDB_name = name + "_" + Integer.toString(type);
        try {
            openDB(name, pages);
            createRDFDB(type);
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    public void createRDFDB(int val) {
        // initialize PCounter
        PCounter.initialize();
        indexOption = val;
        int maxKeySize = 128;
        int deleteFashion = 1; // Full Delete

        // Create Quad heap file
        try {
            quad_heap_file = new QuadrupleHeapfile(rdfDB_name  + "QuadHF");
            predicate_heap_file = new LabelHeapfile(rdfDB_name  + "PredHF");
            entity_heap_file = new LabelHeapfile(rdfDB_name + "EntityHF");

            QuadBTreeFile quadBT = new QuadBTreeFile(rdfDB_name  + "QuadBT", AttrType.attrString, maxKeySize, deleteFashion);
            LabelBTreeFile predBT = new LabelBTreeFile(rdfDB_name  + "PredBT", AttrType.attrString, maxKeySize, deleteFashion);
            LabelBTreeFile entityBT = new LabelBTreeFile(rdfDB_name + "EntityBT", AttrType.attrString, maxKeySize, deleteFashion);
            quadBT.close();
            predBT.close();
            entityBT.close();

        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

    public int getQuadrupleCnt() {
        int cnt = 0;
        try {
            cnt = quad_heap_file.getQuadrupleCnt();
            
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return cnt;
        
    }

    public int getEntityCnt() {
        int cnt = 0;
        try {
            cnt = entity_heap_file.getLabelCnt();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return cnt;
        
    }

    public int getPredicateCnt() {

        int cnt = 0;
        try {
            cnt = predicate_heap_file.getLabelCnt();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return cnt;
    }

    public int getSubjectCnt() {
        ArrayList<String> eids = new ArrayList<String>();

        try{
            QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name + "QuadBT");
            QuadBTFileScan scan = quadBTFile.new_scan(null, null);
            KeyDataEntry entry = scan.get_next();
            QID qid;
            EID eid;
            while(entry != null){
                qid = ((QuadLeafData)entry.data).getData();
                eid = quad_heap_file.getQuadruple(qid).getSubjectQid();
                String eidString = eid.pageNo.pid + ":" + eid.slotNo;
                if(!eids.contains(eidString)){
                    eids.add(eidString);
                }
                entry = scan.get_next();
            }

        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        
        return eids.size();
    }

    public int getObjectCnt() {
        ArrayList<String> eids = new ArrayList<String>();

        try{
            QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name + "QuadBT");
            QuadBTFileScan scan = quadBTFile.new_scan(null, null);
            KeyDataEntry entry = scan.get_next();
            QID qid;
            EID eid;
            
            while(entry != null){
                qid = ((QuadLeafData)entry.data).getData();
                eid = quad_heap_file.getQuadruple(qid).getObjectQid();
                String eidString = eid.pageNo.pid + ":" + eid.slotNo;
                if(!eids.contains(eidString)){
                    eids.add(eidString);
                }
                entry = scan.get_next();
            }

        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        
        return eids.size();

    }

    public EID insertEntity(String EntityLabel) {
        EID eid = null;
        LID lid = null;
        KeyClass key = new StringKey(EntityLabel);

        try {
            LabelBTreeFile entityBTFile = new LabelBTreeFile(rdfDB_name + "EntityBT");
            LabelBTFileScan scan = entityBTFile.new_scan(key, key);
            KeyDataEntry entry = scan.get_next();
            // entry is not already in btree
            if(entry == null){
                lid = entity_heap_file.insertLabel(EntityLabel.getBytes());
                eid = lid.returnEID();
                entityBTFile.insert(key, lid);
            }
            // entry already exists, return existing EID
            else{
                eid = ((LabelLeafData)entry.data).getData().returnEID();
            }
            scan.DestroyBTreeFileScan();
            entityBTFile.close();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return eid;
    }

    public boolean deleteEntity(String EntityLabel) {
        boolean isDeleted = false;
        LID lid = null;
        KeyClass key = new StringKey(EntityLabel);

        try {
            LabelBTreeFile entityBTFile = new LabelBTreeFile(rdfDB_name + "EntityBT");
            LabelBTFileScan scan = entityBTFile.new_scan(key, key);
            KeyDataEntry entry = scan.get_next();
            // entry found in btree
            if(entry != null){
                lid = ((LabelLeafData)entry.data).getData();
                if (lid != null){
                    isDeleted = entity_heap_file.deleteLabel(lid) && entityBTFile.Delete(key, lid);
                }
            }
            scan.DestroyBTreeFileScan();
            entityBTFile.close();
        }
        catch(Exception e){
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
      }

    return isDeleted;

    }

    public PID insertPredicate(String PredicateLabel) {

        PID pid = null;
        LID lid = null;
        KeyClass key = new StringKey(PredicateLabel);
        try {
            LabelBTreeFile predBTFile = new LabelBTreeFile(rdfDB_name + "PredBT");
            LabelBTFileScan scan = predBTFile.new_scan(key, key);
            KeyDataEntry entry = scan.get_next();

            // entry is not already in BTree
            if(entry == null){
                lid =  predicate_heap_file.insertLabel(PredicateLabel.getBytes());
                pid = lid.returnPID();
                predBTFile.insert(key, lid);
            }
            // entry already exists, return existing PID
            else {
                pid = ((LabelLeafData)entry.data).getData().returnPID();
            }
            scan.DestroyBTreeFileScan();
            predBTFile.close();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return pid;
    }

    public boolean deletePredicate(String PredicateLabel) {
        boolean isDeleted = false;
        LID lid = null;
        KeyClass key = new StringKey(PredicateLabel);

        try {
            LabelBTreeFile predBTFile = new LabelBTreeFile(rdfDB_name + "PredBT");
            LabelBTFileScan scan = predBTFile.new_scan(key, key);
            KeyDataEntry entry = scan.get_next();

            // entry is found in btree
            if(entry != null){
                lid = ((LabelLeafData)entry.data).getData();
                if (lid != null){
                    isDeleted = predicate_heap_file.deleteLabel(lid) && predBTFile.Delete(key, lid);
                }
            }
            scan.DestroyBTreeFileScan();
            predBTFile.close();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
          }

        return isDeleted;
    }

    public QID insertQuadruple(byte[] quadruplePtr) {

        QID qid = null;
        try {
          //LOOKUP
          QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name + "QuadBT");
          KeyClass key = getStringKey(quadruplePtr);
          
          QuadBTFileScan scan = quadBTFile.new_scan(key, key);
          KeyDataEntry entry = scan.get_next();

          // The quadruple is not already in the btree, so it can be inserted
          if(entry == null)
          {
            qid = quad_heap_file.insertQuadruple(quadruplePtr);
            quadBTFile.insert(key, qid);
          }
          // btree already contains at least one entry with this key. Must check each of them
          else{
            Boolean changeBool = false;
            while(entry != null) {
                // qid = quad_heap_file.insertQuadruple(quadruplePtr); // DELETE THIS
                qid = ((QuadLeafData)entry.data).getData();
                Quadruple oldQuad = quad_heap_file.getQuadruple(qid);

                // compare subject, predicate, object of the quadruple. These are the first 24 bytes
                byte[] oldBytes = getFirstNBytes(oldQuad.returnQuadrupleByteArray(), 24);
                byte[] newBytes = getFirstNBytes(quadruplePtr, 24);

                if ( Arrays.equals(oldBytes, newBytes)){
                    double new_confidence = Convert.getDoubleValue(24, quadruplePtr);
                    double old_confidence = Convert.getDoubleValue(24, oldQuad.returnQuadrupleByteArray());
    
                    if (new_confidence > old_confidence){
                        Quadruple newQuad = new Quadruple(quadruplePtr, 0);
                        quad_heap_file.updateQuadruple(qid, newQuad);

                        changeBool = true;
                    }
                }
                entry = scan.get_next();
            }
            if (!changeBool){
                qid = quad_heap_file.insertQuadruple(quadruplePtr);
                quadBTFile.insert(key, qid);
            }
          }

          scan.DestroyBTreeFileScan();
          quadBTFile.close();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return qid;
    }

    public boolean deleteQuadruple(byte[] quadruplePtr) {

      boolean isDeleted = false;
      QID qid;
      try {
        QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name + "Quadbt");
        KeyClass key =  getStringKey(quadruplePtr);

        QuadBTFileScan scan = quadBTFile.new_scan(key, key);
        KeyDataEntry entry = scan.get_next();

        // Quadruples with same key are found in btree, iterate to find the exact matching one
        while (entry != null){
          qid = ((QuadLeafData)entry.data).getData();
          // check that the quadruples are the exact same
          byte[] oldBytes = quad_heap_file.getQuadruple(qid).returnQuadrupleByteArray();
          if (Arrays.equals(oldBytes, quadruplePtr)) {
            isDeleted = quad_heap_file.deleteQuadruple(qid) && quadBTFile.Delete(key, qid);
          }
          entry = scan.get_next();
        }
        scan.DestroyBTreeFileScan();
        quadBTFile.close();
      }
      catch(Exception e) {
        System.err.println(e);
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }
      return isDeleted;

    }

    public void getQuadrupleString(Quadruple q){
        // String return_string="";
        try {
            // return_string += entity_heap_file.getLabel(q.getSubjectQid().returnLID()).getLabel();
            // return_string+=":";
            // return_string += predicate_heap_file.getLabel(q.getPredicateID().returnLID()).getLabel();
            // return_string+=":";
            // return_string += entity_heap_file.getLabel(q.getObjectQid().returnLID()).getLabel();
            // return_string+=":";
            // return_string += q.getConfidence();

            String subject = entity_heap_file.getLabel(q.getSubjectQid().returnLID()).getLabel();
            String predicate = predicate_heap_file.getLabel(q.getPredicateID().returnLID()).getLabel();
            String object = entity_heap_file.getLabel(q.getObjectQid().returnLID()).getLabel();


            System.out.printf("%20s %20s %20s %.17f\n",subject,predicate,object,q.getConfidence());

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // return return_string;
    }

    public Stream openStream(int orderType, String subjectFilter, String predicateFilter, String objectFilter, String confidenceFilter){
        Stream stream = null;
        try{
            stream = new Stream(this, orderType, subjectFilter, predicateFilter, objectFilter, confidenceFilter);
        }
        catch(Exception e){
            System.err.println(e);
            e.printStackTrace();
        }
        return stream;
    }

    // helper method to get the first n(exclusive) bytes from an array
    private byte[] getFirstNBytes(byte[] input, int n) {
        byte[] res = new byte[n];
        for(int i = 0; i < n; i++){
            res[i] = input[i];
        }
        return res;
    }

    // gets the key based on the indexing scheme determined when the file was opened
    public KeyClass getStringKey(byte[] quadruplePtr) {
        KeyClass key = null;
        try{
            Quadruple newQuad = new Quadruple(quadruplePtr, 0);
            // newQuad.quadrupleInit(quadruplePtr, 0);
            switch(indexOption){
                // object 
                case(1):{
                    LID lid = newQuad.getObjectQid().returnLID();
                    String object = entity_heap_file.getLabel(lid).getLabel();
                    key = new StringKey(object);
                    break;
                }
                // predicate
                case(2):{
                    LID lid = newQuad.getPredicateID().returnLID();
                    String pred = predicate_heap_file.getLabel(lid).getLabel();
                    key = new StringKey(pred);
                    break;
                }
                // subject
                case(3):{
                    LID lid = newQuad.getSubjectQid().returnLID();
                    String subject = entity_heap_file.getLabel(lid).getLabel();
                    key = new StringKey(subject);
                    break;
                }
                // object + predicate
                case(4):{
                    LID lid = newQuad.getObjectQid().returnLID();
                    String object = entity_heap_file.getLabel(lid).getLabel();
                    lid = newQuad.getPredicateID().returnLID();
                    String pred = predicate_heap_file.getLabel(lid).getLabel();
                    key = new StringKey(object + pred);
                    break;
                }
                // predicate + subject
                case(5):{
                    LID lid = newQuad.getPredicateID().returnLID();
                    String pred = predicate_heap_file.getLabel(lid).getLabel();
                    lid = newQuad.getSubjectQid().returnLID();
                    String subject = entity_heap_file.getLabel(lid).getLabel();
                    key = new StringKey(pred + subject);
                    break;

                }
                default:{
                    System.out.println("Default index");
                    LID lid = newQuad.getPredicateID().returnLID();
                    String pred = entity_heap_file.getLabel(lid).getLabel();
                    lid = newQuad.getSubjectQid().returnLID();
                    String subject = entity_heap_file.getLabel(lid).getLabel();
                    lid = newQuad.getObjectQid().returnLID();
                    String object = entity_heap_file.getLabel(lid).getLabel();
                    key = new StringKey(subject + pred + object);
                    //System.err.println("ERROR: NO INDEXOPTION IN RDFDB");
                    //Runtime.getRuntime().exit(1); 
                }
            }
          }
          catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return key;
    }
}
