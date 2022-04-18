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
import heap.InvalidTupleSizeException;
import iterator.QuadFileScan;
import iterator.Sort;
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
	String _confidenceFilter;
	TScan qfs;

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
	QuadrupleHeapfile _results = null;
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
	public BP_Triple_Join(int amt_of_mem, int num_left_nodes, BPIterator left_itr,
    int BPJoinNodePosition, int JoinOnSubjectorObject, java.lang.String
    RightSubjectFilter, java.lang.String RightPredicateFilter, java.lang.String
    RightObjectFilter, double RightConfidenceFilter, int [] LeftOutNodePositions,
    int OutputRightSubject, int OutputRightObject) throws InvalidTupleSizeException,
			IOException {
                
	}

}