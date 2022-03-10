/* File rdfDB.java */

package diskmgr;

import java.io.*;
import java.util.Set;

// import javax.xml.stream.events.EndElement;

import btree.KeyClass;
import btree.KeyDataEntry;
import btree.QuadBT;
import btree.QuadBTFileScan;
import btree.QuadBTreeFile;
import btree.QuadLeafData;
import btree.StringKey;
import bufmgr.*;
import global.*;
import labelheap.*;
import quadrupleheap.*;

public class rdfDB extends DB {

    private LabelHeapfile entity_heap_file; 
    private LabelHeapfile predicate_heap_file;
    private QuadrupleHeapfile quad_heap_file;

    private String rdfDB_name;

    public LabelHeapfile getEntityHeapFile() { return entity_heap_file; }
    public LabelHeapfile getPredicateHeapFile() { return predicate_heap_file; }
    public QuadrupleHeapfile getQuadHeapFile() { return quad_heap_file; }

    public rdfDB() {}

    public void openrdfDB(String name, int type) {
        rdfDB_name = name + "_" + Integer.toString(type);
        try {
            openDB(name);
            rdfDB(type);
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        
    }

    public void openrdfDB(String name, int pages, int type) {
        rdfDB_name = name;
        try {
            openDB(name, pages);
            rdfDB(type);
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    public void rdfDB(int val) {
        // initialize PCounter
        PCounter.initialize();

        // Create Quad heap file
        try {
            quad_heap_file = new QuadrupleHeapfile(rdfDB_name + "/quadHF");
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // Create Entity Label Heap File
        try {
            entity_heap_file = new LabelHeapfile(rdfDB_name + "/entityHF");
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // Create Predicate Label Heap File
        try {
            predicate_heap_file = new LabelHeapfile(rdfDB_name + "/predHF");
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // Create Quad btree file
        try  {
            // TODO: quad btree. val is the index scheme
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // Create Entity btree file. val is the index scheme
        try  {
            // TODO: Entity btree
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // Create Predicate btree file. val is the index scheme
        try  {
            // TODO: Predicate btree
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
        // TODO: need b trees
        int subject_count = 0;

        

        return subject_count;
    }

    public int getObjectCnt() {
        // TODO: need b trees
        int object_count = 0;

        return object_count;
    }

    public EID insertEntity(String EntityLabel) {
        EID eid = null;
        KeyClass key = new StringKey(EntityLabel);
        // TODO: check if label already in btree file

        try {
            LID lid = entity_heap_file.insertLabel(EntityLabel);
        // TODO: add (key, label) to btree and close btree

            eid = lid.returnEID();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return eid;
    }

    public boolean deleteEntity(String EntityLabel) {

    }

    public PID insertPredicate(String PredicateLabel) {

        PID pid = null;
        KeyClass key = new StringKey(PredicateLabel);
        // TODO: check if label already in btree file
        try {
            LID lid =  predicate_heap_file.insertLabel(PredicateLabel);

        // TODO: add (key, label) to btree and close btree

            pid = lid.returnPID();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return pid;
    }

    public boolean deletePredicate(String PredicateLabel) {

    }

    public QID insertQuadruple(byte[] quadruplePtr) {

        QID qid = null;
        // TODO: check if quad already in btree file
        try {
          QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name+"/quadbt");

          KeyClass key;
          // TODO: what will be the key of the scan? Does this matter based on index option?
          QuadBTFileScan scan = quadBTFile.new_scan(key, key);
          KeyDataEntry entry = scan.get_next();

          // The quadruple is not already in the btree, so it can be inserted
          if(entry == null)
          {
            qid = quad_heap_file.insertQuadruple(quadruplePtr);
            quadBTFile.insert(key, qid);
          }
          // btree already contains this quadruple
          else{
            qid = ((QuadLeafData)entry.data).getData();
            Quadruple quad = quad_heap_file.getQuadruple(qid);

            double new_confidence = Convert.getFloValue(24, quadruplePtr);
            double old_confidence = Convert.getFloValue(24, quad.getQuadrupleByteArray());

            if (new_confidence > old_confidence){
              Quadruple newQuad = new Quadruple(quadruplePtr, 0);
              quad_heap_file.updateQuadruple(qid, newQuad);
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
        QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name+"/quadbt");
        KeyClass key;
        // TODO: what will be the key of the scan? Does this matter based on index option?
        QuadBTFileScan scan = quadBTFile.new_scan(key, key);
        KeyDataEntry entry = scan.get_next();

        if (entry != null){
          qid = ((QuadLeafData)entry.data).getData();
          if (qid != null) {
          isDeleted = quad_heap_file.deleteQuadruple(qid) && quadBTFile.Delete(key, qid);
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
      return isDeleted;

    }

    public Stream openStream(int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){
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
}