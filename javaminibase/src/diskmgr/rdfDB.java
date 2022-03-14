/* File rdfDB.java */

package diskmgr;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import btree.*;
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
    public String getName() { return rdfDB_name;}

    public rdfDB() {}

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
        rdfDB_name = name;
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

        // Create Quad heap file
        try {
            quad_heap_file = new QuadrupleHeapfile(rdfDB_name + "QuadHF");
            predicate_heap_file = new LabelHeapfile(rdfDB_name + "PredHF");
            entity_heap_file = new LabelHeapfile(rdfDB_name + "EntityHF");

            QuadBTreeFile quadBT = new QuadBTreeFile(rdfDB_name + "QuadBT", AttrType.attrString, keysize, 1);
            LabelBTreeFile predBT = new LabelBTreeFile(rdfDB_name + "PredBT", AttrType.attrString, keysize, 1);
            LabelBTreeFile entityBT = new LabelBTreeFile(rdfDB_name + "EntityBT", AttrType.attrString, keysize, 1);

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
        ArrayList<EID> eids = new ArrayList<EID>();

        try{
            QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name + "QuadBT");
            QuadBTFileScan scan = quadBTFile.new_scan(null, null);
            KeyDataEntry entry = scan.get_next();
            QID qid;
            EID eid;
            while(entry != null){
                qid = ((QuadLeafData)entry.data).getData();
                eid = quad_heap_file.getQuadruple(qid).getSubjecqid();
                if(!eids.contains(eid)){
                    eids.add(eid);
                }
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
        ArrayList<EID> eids = new ArrayList<EID>();

        try{
            QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name + "QuadBT");
            QuadBTFileScan scan = quadBTFile.new_scan(null, null);
            KeyDataEntry entry = scan.get_next();
            QID qid;
            EID eid;
            while(entry != null){
                qid = ((QuadLeafData)entry.data).getData();
                eid = quad_heap_file.getQuadruple(qid).getObjecqid();
                if(!eids.contains(eid)){
                    eids.add(eid);
                }
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
            // lid = entity_heap_file.insertLabel(EntityLabel);
            KeyDataEntry entry = scan.get_next();
            // entry is not already in btree
            if(entry == null){
                lid = entity_heap_file.insertLabel(EntityLabel);
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
            // lid = entity_heap_file.insertLabel(EntityLabel);
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
                lid =  predicate_heap_file.insertLabel(PredicateLabel);
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
          QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name+"QuadBT");

          KeyClass key;
          // TODO: what will be the key of the scan? Does this matter based on index option?
          // this doesn't work if conf used in key
          
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
            while(entry != null) {

                qid = ((QuadLeafData)entry.data).getData();
                Quadruple oldQuad = quad_heap_file.getQuadruple(qid);

                // compare subject, predicate, object of the quadruple. These are the first 23 bytes
                byte[] oldBytes = getFirstNBytes(oldQuad.getQuadrupleByteArray(), 23);
                byte[] newBytes = getFirstNBytes(quadruplePtr, 23);

                if ( Arrays.equals(oldBytes, newBytes)){
                    double new_confidence = Convert.getFloValue(24, quadruplePtr);
                    double old_confidence = Convert.getFloValue(24, oldQuad.getQuadrupleByteArray());
    
                    if (new_confidence > old_confidence){
                        Quadruple newQuad = new Quadruple(quadruplePtr, 0);
                        quad_heap_file.updateQuadruple(qid, newQuad);
                    }

                }
                
                entry = scan.get_next();
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
        QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name+"Quadbt");
        KeyClass key;
        // TODO: what will be the key of the scan? Does this matter based on index option?
        QuadBTFileScan scan = quadBTFile.new_scan(key, key);
        KeyDataEntry entry = scan.get_next();

        // Quadruple is found in btree
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

    // helper method to get the first n bytes from an array
    private byte[] getFirstNBytes(byte[] input, int n) {
        byte[] res = new byte[n];
        for(int i = 0; i < n; i++){
            res[i] = input[i];
        }
        return res;
    }
}