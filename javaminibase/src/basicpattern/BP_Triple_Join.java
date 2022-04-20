package diskmgr;

/** JAVA */
/**
 * Stream.java-  class Stream
 *
 */

import java.io.*;

import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import iterator.LowMemException;
import iterator.QuadFileScan;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknownKeyTypeException;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;
import quadrupleheap.TScan;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import basicpattern.*;
/**
 */
public class BP_Triple_Join implements GlobalConst {

	/** The rdfDB we are using. */
	private rdfDB _rdfdb;
	int _orderType = 0;
	boolean _needSort;
	String _subjectFilter, _predicateFilter, _objectFilter;
	double _minConfidence = 0.6;
	String _minConfidenceFilter = Double.toString(_minConfidence);
	Scan qfs;

	boolean _subjectNullFilter = false;
	boolean _predicateNullFilter = false;
	boolean _objectNullFilter = false;
	boolean _confidenceNullFilter = false;
	boolean scan_on_btree = false;

	EID _subjectID = new EID();
	PID _predicateID = new PID();
	EID _objectID = new EID();

	//int SORT_Q_NUM_PAGES = SystemDefs.JavabaseBM.getNumBuffers()/2;
	int SORT_Q_NUM_PAGES = 16;
	
	Sort qsort = null;
	Heapfile _results=null;
	QuadrupleHeapfile _rightQuads = null;
	public TScan quadover = null;

	boolean use_index = true;

	/**
	 * The constructor pins the first directory page in the file
	 * and initializes its private data members from the private
	 * data member from hf
	 *
	 * @exception InvalidTupleSizeException Invalid tuple size
	 * @exception IOException               I/O errors
	 *
	 * @param rdfdb A rdfDB object
	 */
	public BP_Triple_Join(int amt_of_mem, int num_left_nodes, BasicPatternIterator left_itr,
    int BPJoinNodePosition, int JoinOnSubjectorObject, java.lang.String
    RightSubjectFilter, java.lang.String RightPredicateFilter, java.lang.String
    RightObjectFilter, double RightConfidenceFilter, int [] LeftOutNodePositions,
    int OutputRightSubject, int OutputRightObject) throws InvalidTupleSizeException,
			IOException {
            // Get filtered resultant right heap file
            // from quadrupleFile to BasicPattern
            // 

            try{
                rdfDB database = rdfDB.getInstance();
                Stream rstream = null;
                Stream lstream = null;
                QuadrupleHeapfile _leftQuads;

                // Get all quads with minimum confidence
                lstream = new Stream(database, "*","*", "*", _minConfidenceFilter);            
                _leftQuads =  lstream.getResults();
                
				
                // Get all filtered Quads for the join
                rstream = new Stream(database, RightSubjectFilter, RightPredicateFilter, RightObjectFilter, Double.toString(RightConfidenceFilter));            
                _rightQuads =  rstream.getResults();

				// JOIN
                _results = new Heapfile(Long.toString((new java.util.Date()).getTime()));
				
				

				// qfs = new Scan(_results);


				lstream.closestream();
                rstream.closestream();
				// qfs.closescan();
            }
            catch(Exception e){
                System.err.println(e);
                e.printStackTrace();
            } 


	}

	public BasicPattern get_next() 
	throws IOException, InvalidTypeException, PageNotReadException, TupleUtilsException, SortException, LowMemException, UnknownKeyTypeException, Exception
	{
		QID qid = new QID();


	}

	public void close() throws IOException, SortException{
		// try{
		// 	if()
		// }
	}
}