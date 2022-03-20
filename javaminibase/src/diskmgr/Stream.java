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

//Install JDk

/*
Steps:
1. Initialize Stream values
	- get null filters
	- set filter values
	- check if index chosen and filter do not conflict
		- if conflict, run command to create a normal full scan
		- if no conflict, run command to create btree index
2. Create indexes
	- Get EID: From BTree, scan the indexes and get the value that matches filter string and return the LID for that
	- Get PID: From BTree, scan the indexes and get the value that matches filter string and return the LID for that
	- ScanBTReeIndex: 
		- Fetch all matching subject EID 
			- if subject filter is not null, match
			- else take all
		- Fetch all matching predicate PIDs 
		- Fetch all matching object EIDs
		- create a key with them and set lower and upper key with this value (In exact match lower_key=upper_key=key)
		- Get handle to heapfiles having the values
		- Start a new scan and compare the keys until match is found
			- Check confidence for threshold
				- If record satisfies conditions, insert record into results to return
				- else no record found
		
	- sort the values and store them for iteration
3. Iterate over indexes (getNext from qsort or tscan)
4. Close stream
	- delete all index files
	- close scan
	- close sorting method
	

*/

/**
 * A Stream object is created ONLY through the function openStream
 * of a rdfDB. It supports the getNext interface which will
 * simply retrieve the next record in the rdfDB.
 * Heapfile
 * An object of type Stream will always have pinned one directory page
 * of the rdfDB.
 */
/*
 * Changes
 * HeapFile-> QuadrupleHeapfile ->rdfDB
 * RID->QID
 * Scan->Stream
 * Tuple->Quadruple
 */
public class Stream implements GlobalConst {

	/**
	 * Note that one record in our way-cool rdfDB implementation is
	 * specified by six (6) parameters, some of which can be determined
	 * from others:
	 */

	/** The rdfDB we are using. */
	private rdfDB _rdfdb;
	int _orderType = 0;
	boolean _needSort;
	String _subjectFilter, _predicateFilter, _objectFilter;
	String _confidenceFilter;

	boolean _subjectNullFilter = false;
	boolean _predicateNullFilter = false;
	boolean _objectNullFilter = false;
	boolean _confidenceNullFilter = false;
	boolean scan_on_btree = false;

	EID _subjectID = new EID();
	PID _predicateID = new PID();
	EID _objectID = new EID();

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
	public Stream(rdfDB rdfdatabase, int orderType, String subjectFilter, String predicateFilter, String objectFilter,
			String confidenceFilter)
			throws InvalidTupleSizeException,
			IOException {
		// set null filters and filter values
		init(rdfdatabase, orderType, subjectFilter, predicateFilter, objectFilter, confidenceFilter);
		// Scan file using index
		if (!_subjectNullFilter & !_predicateNullFilter & !_objectNullFilter & !_confidenceNullFilter) {
			// No nulls so we can perform a filter on all columns in the full btree
			// scan_on_btree=> only one match will be found
			scan_on_btree = true;
		} else {
			scan_on_btree = false;
			if (indexValidForStream()) {
				use_index = true;
			} else {
				// None of the above options were selected or the index cannot be created for
				// the given filter as one of the column values used in the index is null
				use_index = false;
			}
			ScanBTreeIndex();

			// Sort results
			// TODO: set class name and define instance
			// SORT: don't forget a default case ASK TANNER
			// quadover = new TScan(_results);
			try {
				QuadFileScan qfs = new QuadFileScan(_results);
				// 0 means Ascending
				QuadrupleOrder quadrupleOrder = new QuadrupleOrder(_orderType, 0);
				qsort = new Sort(qfs, quadrupleOrder, SORT_Q_NUM_PAGES);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public QID getFirstQID() {
		return this.quadover.get_firstQID();
	}

	public boolean indexValidForStream() {
		if (_rdfdb.getIndexOption() == 1 && !_objectNullFilter) {
			return true;
		} else if (_rdfdb.getIndexOption() == 2 && !_predicateNullFilter) {
			return true;
		} else if (_rdfdb.getIndexOption() == 3 && !_subjectNullFilter) {
			return true;
		} else if (_rdfdb.getIndexOption() == 4 && !_objectNullFilter && !_predicateNullFilter) {
			return true;
		} else if (_rdfdb.getIndexOption() == 5 && !_predicateNullFilter && !_subjectNullFilter) {
			return true;
		} else {
			return false;
		}
	}

	public EID getEID(String EntityLabel) {
		EID eid = null;
		LabelBTreeFile entityBTFile;

		try {
			entityBTFile = new LabelBTreeFile(
					rdfDB.getInstance().rdfDB_name + Integer.toString(rdfDB.getInstance().indexOption) + "EntityBT");
			KeyClass eid_key = new StringKey(EntityLabel);

			KeyDataEntry entry = null;

			LabelBTFileScan scan = entityBTFile.new_scan(eid_key, eid_key);
			entry = scan.get_next();
			// entry is not already in btree
			if (entry == null) {
				System.out.println("No EID for given filter found.");
			}
			// entry already exists, return existing EID
			else {
				eid = ((LabelLeafData) entry.data).getData().returnEID();
			}
			scan.DestroyBTreeFileScan();
			entityBTFile.close();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return eid;
	}

	public PID getPID(String PredicateLabel) {
		PID pid = null;
		LabelBTreeFile predBTFile;

		try {
			predBTFile = new LabelBTreeFile(
					rdfDB.getInstance().rdfDB_name + Integer.toString(rdfDB.getInstance().indexOption) + "PredBT");
			KeyClass pid_key = new StringKey(PredicateLabel);

			KeyDataEntry entry = null;
			LabelBTFileScan scan = predBTFile.new_scan(pid_key, pid_key);
			entry = scan.get_next();
			// entry is not already in btree
			if (entry == null) {
				System.out.println("No PID for given filter found.");
			}
			// entry already exists, return existing EID
			else {
				pid = ((LabelLeafData) entry.data).getData().returnPID();
			}
			scan.DestroyBTreeFileScan();
			predBTFile.close();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return pid;
	}

	public boolean ScanBTreeIndex() {
		QID qid = null;
		// TODO: check
		try {
			_results = new QuadrupleHeapfile(Long.toString((new java.util.Date()).getTime()));
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (!use_index) {
			_rdfdb.createRDFDB(0);
		}

		try {
			QuadBTreeFile quadBTFile = new QuadBTreeFile(
					rdfDB.getInstance().rdfDB_name + Integer.toString(rdfDB.getInstance().indexOption) + "QuadBT");

			QuadrupleHeapfile quad_heap_file = _rdfdb.getQuadHeapFile();
			// LabelHeapfile predicate_heap_file = _rdfdb.getPredicateHeapFile();
			// LabelHeapfile entity_heap_file = _rdfdb.getEntityHeapFile();

			EID subjectid = getEID(_subjectFilter);
			PID predicateid = getPID(_predicateFilter);
			EID objectid = getEID(_objectFilter);

			byte quadruplePtr[] = new byte[28];
			Convert.setIntValue(subjectid.pageNo.pid, 0, quadruplePtr);
			Convert.setIntValue(subjectid.slotNo, 4, quadruplePtr);
			Convert.setIntValue(predicateid.pageNo.pid, 8, quadruplePtr);
			Convert.setIntValue(predicateid.slotNo, 12, quadruplePtr);
			Convert.setIntValue(objectid.pageNo.pid, 16, quadruplePtr);
			Convert.setIntValue(objectid.slotNo, 20, quadruplePtr);
			Convert.setDoubleValue(Double.parseDouble(_confidenceFilter), 24, quadruplePtr);
			QuadBTFileScan scan;
			if(use_index){
				KeyClass key = _rdfdb.getStringKey(quadruplePtr);
				scan = quadBTFile.new_scan(key, key);
			}
			else{
				scan = quadBTFile.new_scan(null, null);
			}
			KeyDataEntry entry = scan.get_next();

			// The quadruple is not already in the btree, return false
			if (entry == null) {
				System.out.println("No match found");
				return false;
			}
			// btree found a match
			else {
				while (entry != null) {
					// get qid of given entry
					qid = ((QuadLeafData) entry.data).getData();
					// Quadruple oldQuad = quad_heap_file.getQuadruple(qid);
					byte[] oldQuad = quad_heap_file.getQuadruple(qid).getQuadrupleByteArray();

					// compare subject, predicate, object of the quadruple.
					if (!_subjectNullFilter) {
						if (!(Convert.getIntValue(0, quadruplePtr) == Convert.getIntValue(0, oldQuad) &&
								Convert.getIntValue(4, quadruplePtr) == Convert.getIntValue(4, oldQuad))) {
							continue;
						}
					}
					if (!_predicateNullFilter) {
						if (!(Convert.getIntValue(8, quadruplePtr) == Convert.getIntValue(8, oldQuad) &&
								Convert.getIntValue(12, quadruplePtr) == Convert.getIntValue(12, oldQuad))) {
							continue;
						}
					}
					if (!_objectNullFilter) {
						if (!(Convert.getIntValue(16, quadruplePtr) == Convert.getIntValue(16, oldQuad) &&
								Convert.getIntValue(20, quadruplePtr) == Convert.getIntValue(20, oldQuad))) {
							continue;
						}
					}
					if (!_confidenceNullFilter) {
						// DESIGN DECISION (Index): Confidence is updatable SO to decrease #of sorts
						// needed to be done and unreliable Indexes
						if (Convert.getDoubleValue(24, quadruplePtr) <= Convert.getDoubleValue(24, oldQuad)) {
							continue;
						}
					}
					// TODO:
					_results.insertQuadruple(oldQuad);
					entry = scan.get_next();
				}
			}
			scan.DestroyBTreeFileScan();
			quadBTFile.close();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return true;

	}

	/**
	 * Retrieve the next record in a sequential Stream
	 *
	 * @exception InvalidTupleSizeException Invalid tuple size
	 * @exception IOException               I/O errors
	 *
	 * @param qid Record ID of the record
	 * @return the Quadruple of the retrieved record.
	 */
	public Quadruple getNext(QID qid)
			throws InvalidTupleSizeException,
			IOException {
		try {
			// TODO: ONLY IF DEFAULT CASE COVERED
			return quadover.getNext(qid);
			// return qsort.getNext(qid); TODO delete
		} catch (Exception e) {
			System.out.println("Error in getNext of Steam.java");
		}
		return null;
	}

	/**
	 * Do all the constructor work
	 *
	 * @exception InvalidTupleSizeException Invalid tuple size
	 * @exception IOException               I/O errors
	 *
	 * @param rdfdb A rdfDB object
	 */
	private void init(rdfDB rdfdb, int orderType, String subjectFilter, String predicateFilter, String objectFilter,
			String confidenceFilter)
			throws InvalidTupleSizeException,
			IOException {
		// set variables and filters
		_rdfdb = rdfdb;
		_orderType = orderType;

		if (subjectFilter.compareToIgnoreCase("null") == 0) {
			_subjectNullFilter = true;
		}
		if (predicateFilter.compareToIgnoreCase("null") == 0) {
			_predicateNullFilter = true;
		}
		if (objectFilter.compareToIgnoreCase("null") == 0) {
			_objectNullFilter = true;
		}
		if (confidenceFilter.compareToIgnoreCase("null") == 0) {
			_confidenceNullFilter = true;
		}
		_subjectFilter = subjectFilter;
		_predicateFilter = predicateFilter;
		_objectFilter = objectFilter;
		_confidenceFilter = confidenceFilter;
	}

	/** Closes the Stream object */
	public void closestream() {
		try {
			// if (qsort != null) {
			// qsort.close();
			// }

			if (quadover != null) {
				quadover.closescan();
			}

			if (_results != null && _results != _rdfdb.getQuadHeapFile()) {
				_results.deleteFile();
			}
		} catch (Exception e) {
			System.out.println("Error in closing Stream");
		}

	}

}