package basicpattern;

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
import heap.Tuple;
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
 * TODO:
 * - Join: Index 3 strategies @Krima
 * - Join: Create result Heapfile @Krima
 * - Iterator: @Tanner
 * - BasicPattern from tuple @Jack
 * -
 * - Report
 * - Test Cases:
 * - Eval:
 * # page accesses
 * Cost - formula from page accesses [Start with a very large buffer size]
 * M input1 length, N input2 length, Get length of results
 * Or should not deviate from given formula
 * 
 */
public class BP_Triple_Join implements GlobalConst {

	/** The rdfDB we are using. */
	private rdfDB _rdfdb;
	double _minConfidence = 1;
	String _minConfidenceFilter = Double.toString(_minConfidence);
	// BasicPatternSort bp_sort=null;
	Scan bp_scan;

	EID _subjectID = new EID();
	PID _predicateID = new PID();
	EID _objectID = new EID();

	Heapfile _results = null;
	// QuadrupleHeapfile _rightQuads = null;
	public TScan quadover = null;

	BasicPatternIteratorScan left_itr = null;
	Stream rstream = null;

	int amt_of_mem;
	int num_left_nodes;
	int BPJoinNodePosition;
	int JoinOnSubjectorObject;
	int[] LeftOutNodePositions;
	int OutputRightSubject;
	int OutputRightObject;
	Quadruple inner_quad = null;
	BasicPattern outer_bp = null;

	String RightSubjectFilter, RightPredicateFilter, RightObjectFilter;
	String RightConfidenceFilter;
	boolean COMPLETED_FLAG = false;

	BPSort outerSort = null;
	public int getNumLeftNodes() {
		return num_left_nodes;
	}

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
	public BP_Triple_Join(int amt_of_mem, int num_left_nodes, BasicPatternIteratorScan left_itr,
			int BPJoinNodePosition, int JoinOnSubjectorObject, java.lang.String RightSubjectFilter,
			java.lang.String RightPredicateFilter, java.lang.String RightObjectFilter, String RightConfidenceFilter,
			int[] LeftOutNodePositions,
			int OutputRightSubject, int OutputRightObject) throws InvalidTupleSizeException,
			IOException {
		// Get filtered resultant right heap file
		// from quadrupleFile to BasicPattern
		//

		try {
			_rdfdb = rdfDB.getInstance();
			COMPLETED_FLAG = false;
			// Stream lstream = null;
			// QuadrupleHeapfile _leftQuads;

			// Get all quads with minimum confidence
			// lstream = new Stream(database, "*","*", "*", _minConfidenceFilter);
			// _leftQuads = lstream.getResults();
			this.left_itr = left_itr;

			// available pages for operation
			this.amt_of_mem = amt_of_mem;
			// Number of entites in given BP
			this.num_left_nodes = num_left_nodes;
			this.BPJoinNodePosition = BPJoinNodePosition + 2;
			this.JoinOnSubjectorObject = JoinOnSubjectorObject;
			this.LeftOutNodePositions = LeftOutNodePositions;
			this.OutputRightSubject = OutputRightSubject;
			this.OutputRightObject = OutputRightObject;
			inner_quad = null;
			outer_bp = null;

			this.RightSubjectFilter = RightSubjectFilter;
			this.RightPredicateFilter = RightPredicateFilter;
			this.RightObjectFilter = RightObjectFilter;
			this.RightConfidenceFilter = RightConfidenceFilter;

			// _rightQuads = rstream.getResults();
			// TODO: reset heapfile and close scan
			// _results=null;
			// basic_nlj();
			// basic_index_nlj();
			// bp_scan = new Scan(_results);
			// JOIN
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}

	}

	public boolean runJoinType(int jointType)
			throws IOException, InvalidTypeException, PageNotReadException, TupleUtilsException, SortException,
			LowMemException, UnknownKeyTypeException, Exception {
		switch (jointType) {

			case 1:
				_results = null;
				COMPLETED_FLAG = basic_nlj();
				bp_scan = new Scan(_results);
				break;
			case 2:
				_results = null;
				COMPLETED_FLAG = basic_index_nlj();
				bp_scan = new Scan(_results);
				break;
			case 3:
				_results = null;
				COMPLETED_FLAG = basic_sorted_index_nlj();
				bp_scan = new Scan(_results);
				break;
			case 4:
				_results = null;
				COMPLETED_FLAG = sort_merge_join();
				bp_scan = new Scan(_results);
				break;
			default:
				throw new InvalidTypeException(null, "jointType: JOIN_TYPE_ERROR");
		}

		return COMPLETED_FLAG;
	}

	public Tuple fromBP(BasicPattern basicPattern) {
		Tuple tuple = new Tuple();
		int basicPatternLength = basicPattern.getLength();
		short numberOfTupleFields = (short) (basicPattern.noOfFlds() * 2 - 1);

		AttrType[] tupletypes = new AttrType[numberOfTupleFields];
		short[] strSizes = new short[1];
		short tuplesize = 8;
		tupletypes[0] = new AttrType(AttrType.attrD);
		for (int i = 1; i < numberOfTupleFields; i++) {
			tupletypes[i] = new AttrType(AttrType.attrInteger);
			tuplesize += 4;
		}
		try {
			tuple.setHdr(numberOfTupleFields, tupletypes, strSizes);
			tuple.setDFld(1, basicPattern.getDoubleFld(1));
			for (int i = 2; i <= numberOfTupleFields / 2; i++) {
				tuple.setIntFld(i * 2 - 2, basicPattern.getEIDFld(i).pageNo.pid);
				tuple.setIntFld(i * 2 - 1, basicPattern.getEIDFld(i).slotNo);
			} // according to Iterator.java
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tuple;
	}

	public BasicPattern get_next() {
		RID rid = new RID();
		try {
			return new BasicPattern(bp_scan.getNext(rid));
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public boolean basic_nlj()
			throws IOException, InvalidTypeException, PageNotReadException, TupleUtilsException, SortException,
			LowMemException, UnknownKeyTypeException, Exception {

		// _results = new Heapfile(Long.toString((new java.util.Date()).getTime()));
		_results = new Heapfile(left_itr.getFileName() + "basic_nlj");

		// Apend to heapfile
		do {
			// Scan for tuple
			//

			// Check if end of right stream (inner loop)
			if (rstream == null) {
				rstream = new Stream(_rdfdb, RightSubjectFilter, RightPredicateFilter, RightObjectFilter,
						RightConfidenceFilter);
				// Check if end of BP iteration (outer loop)
				if ((outer_bp = left_itr.get_next()) == null) {
					COMPLETED_FLAG = true;
					// All elements in outer loop have been processed, i.e. JOIN is complete
					return COMPLETED_FLAG;
				}
				// if (outer_bp.getDoubleFld(outer_bp.confidence_fld_num) < _minConfidence) {
				// continue;
				// }
			}

			while ((inner_quad = rstream.getNext()) != null) {
				EID join_eid_outer = outer_bp.getEIDFld(BPJoinNodePosition);
				EID join_eid_inner;
				if (JoinOnSubjectorObject == 0) {
					join_eid_inner = inner_quad.getSubjectQid();
				} else {
					join_eid_inner = inner_quad.getObjectQid();
				}

				// String label1 = _rdfdb.getEntityHeapFile().getLabel(join_eid_outer.returnLID()).getLabel();
				// String label2 = _rdfdb.getEntityHeapFile().getLabel(join_eid_inner.returnLID()).getLabel();
				// System.out.println(label1);
				// System.out.println(label2);

				if (join_eid_outer.equals(join_eid_inner)) {
					// Match found so calculate new confidence
					double new_confidence = Math.min(outer_bp.getDoubleFld(outer_bp.confidence_fld_num),
							inner_quad.getConfidence());

					BasicPattern tempBP = new BasicPattern(LeftOutNodePositions, outer_bp);
					tempBP.setDoubleFld(tempBP.confidence_fld_num, new_confidence);

					// Insert values into Basic Pattern
					if (OutputRightSubject == 1) {
						tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getSubjectQid());
						if (OutputRightObject == 1) {
							tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
						}
					} else if (OutputRightObject == 1) {
						tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
					}
					// _results.insertRecord(outer_bp.returnBasicPatternArray());
					_results.insertRecord(tempBP.returnTupleArray());
					// _results.insertRecord(outer_bp.getBasicPatternByteArray());
				}
			}
			try {
				if (rstream != null) {
					rstream.closestream();
					rstream = null;
				}
			} catch (Exception e) {
				System.out.println("Error in closing stream in BT_Triple_Join");
				e.printStackTrace();
			}
		} while (outer_bp != null);

		// BasicPatternIteratorScan am = BasicPatternIteratorScan(bp_file);
		num_left_nodes = 1 + LeftOutNodePositions.length + OutputRightSubject + OutputRightObject;

		return COMPLETED_FLAG;
	}

	public boolean basic_index_nlj()
			throws IOException, InvalidTypeException, PageNotReadException, TupleUtilsException, SortException,
			LowMemException, UnknownKeyTypeException, Exception {
		outer_bp = null;
		_results = new Heapfile(left_itr.getFileName() + "basic_index_nlj");

		do {
			// Check if end of right stream (inner loop)
			// Sorted Index join
			if (rstream == null) {
				// Check if end of BP iteration (outer loop)
				if ((outer_bp = left_itr.get_next()) == null) {
					COMPLETED_FLAG = true;
					// All elements in outer loop have been processed, i.e. JOIN is complete
					return COMPLETED_FLAG;
				}
				// if (outer_bp.getDoubleFld(outer_bp.confidence_fld_num) < _minConfidence) {
				// continue;
				// }

				LID labelID = outer_bp.getEIDFld(BPJoinNodePosition).returnLID();
				String label = _rdfdb.getEntityHeapFile().getLabel(labelID).getLabel();

				if (JoinOnSubjectorObject == 0) {
					try {
						rstream = new Stream(_rdfdb, label,
								RightPredicateFilter, RightObjectFilter, RightConfidenceFilter);
					} catch (Exception e) {
						try {
							rstream.closestream();
							rstream = null;
							// continue;
						} catch (Exception ex) {
							rstream = null;
							// continue;
						}
					}
				} else {
					try {
						rstream = new Stream(_rdfdb, RightSubjectFilter, RightPredicateFilter,
								label, RightConfidenceFilter);
					} catch (Exception e) {
						try {
							rstream.closestream();
							rstream = null;
							// continue;
						} catch (Exception ex) {
							rstream = null;
							// continue;
						}
					}
				}
			}
			// System.out.println(outer_bp.getDoubleFld(1));

			if (rstream == null) {
				System.out.println("rstream null");
				continue;
			}
			while ((inner_quad = rstream.getNext()) != null) {
				// Match found so calculate new confidence
				if (JoinOnSubjectorObject == 0) {
					if (RightSubjectFilter.compareToIgnoreCase("*") != 0) {
						EID subjectid = rstream.getEID(RightSubjectFilter);
						EID join_eid_outer = outer_bp.getEIDFld(BPJoinNodePosition);

						if (subjectid == null || !subjectid.equals(join_eid_outer)) {
							continue;
						}
					}
				} else {
					if (RightObjectFilter.compareToIgnoreCase("*") != 0) {
						EID objectid = rstream.getEID(RightObjectFilter);
						EID join_eid_outer = outer_bp.getEIDFld(BPJoinNodePosition);

						if (objectid == null || !objectid.equals(join_eid_outer)) {
							continue;
						}
					}
				}
				double new_confidence = Math.min(outer_bp.getDoubleFld(outer_bp.confidence_fld_num),
						inner_quad.getConfidence());

				BasicPattern tempBP = new BasicPattern(LeftOutNodePositions, outer_bp);
				tempBP.setDoubleFld(tempBP.confidence_fld_num, new_confidence);

				// Insert values into Basic Pattern
				if (OutputRightSubject == 1) {
					tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getSubjectQid());
					if (OutputRightObject == 1) {
						tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
					}
				} else if (OutputRightObject == 1) {
					tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
				}
				// _results.insertRecord(outer_bp.getBasicPatternByteArray());
				_results.insertRecord(tempBP.returnTupleArray());
			}
			try {
				if (rstream != null) {
					rstream.closestream();
					rstream = null;
					COMPLETED_FLAG = true;
				}
			} catch (Exception e) {
				System.out.println("Error in closing stream in BT_Triple_Join");
				e.printStackTrace();
			}
		} while (outer_bp != null);

		num_left_nodes = 1 + LeftOutNodePositions.length + OutputRightSubject + OutputRightObject;
		return COMPLETED_FLAG;
	}

	public boolean basic_sorted_index_nlj()
			throws IOException, InvalidTypeException, PageNotReadException, TupleUtilsException, SortException,
			LowMemException, UnknownKeyTypeException, Exception {
		outer_bp = null;
		_results = new Heapfile(left_itr.getFileName() + "bsi_nlj");
		while ((outer_bp = left_itr.get_next()) != null){
			BasicPattern older_bp = outer_bp;
			// if (outer_bp.getDoubleFld(outer_bp.confidence_fld_num) < _minConfidence) {
			// continue;
			// }

			LID labelID = outer_bp.getEIDFld(BPJoinNodePosition).returnLID();
			String outer_bp_label = _rdfdb.getEntityHeapFile().getLabel(labelID).getLabel();
			
			if (JoinOnSubjectorObject == 0) {
				if (RightSubjectFilter.compareToIgnoreCase("*") != 0){
					if(outer_bp_label.compareToIgnoreCase(RightSubjectFilter)!=0){
						continue;
					}
				}
			}
			else{
				if (RightObjectFilter.compareToIgnoreCase("*") != 0){
					if(outer_bp_label.compareToIgnoreCase(RightObjectFilter)!=0){
						continue;
					}
				}
			}
			if (older_bp != null && rstream != null && outer_bp.getEIDFld(BPJoinNodePosition).equals(older_bp.getEIDFld(BPJoinNodePosition))) {
				// labelID = older_bp.getEIDFld(BPJoinNodePosition).returnLID();
				// String older_bp_label = _rdfdb.getEntityHeapFile().getLabel(labelID).getLabel();
				rstream.reset_scan();
			} else {
				try {
					if (rstream != null) {
						rstream.closestream();
						rstream = null;
					}
				} catch (Exception e) {
					System.out.println("Error in closing stream in BT_Triple_Join");
					e.printStackTrace();
				}
				int order;
				if (JoinOnSubjectorObject == 0) {
					order = QuadrupleOrder.SubjectConfidence;
				} else {
					order = QuadrupleOrder.ObjectConfidence;
				}
				rstream = new Stream(_rdfdb, order, RightSubjectFilter, RightPredicateFilter, RightObjectFilter, RightConfidenceFilter);
			}

			while ((inner_quad = rstream.getNext()) != null) {
				// Match found so calculate new confidence
				double new_confidence = Math.min(outer_bp.getDoubleFld(outer_bp.confidence_fld_num),
						inner_quad.getConfidence());

				BasicPattern tempBP = new BasicPattern(LeftOutNodePositions, outer_bp);
				tempBP.setDoubleFld(tempBP.confidence_fld_num, new_confidence);

				// Insert values into Basic Pattern
				if (OutputRightSubject == 1) {
					tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getSubjectQid());
					if (OutputRightObject == 1) {
						tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
					}
				} else if (OutputRightObject == 1) {
					tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
				}
				// _results.insertRecord(outer_bp.getBasicPatternByteArray());
				_results.insertRecord(tempBP.returnTupleArray());
			}
		}

		outer_bp = null;
		try {
			if (rstream != null) {
				rstream.closestream();
				rstream = null;
				COMPLETED_FLAG = true;
			}
		} catch (Exception e) {
			System.out.println("Error in closing stream in BT_Triple_Join");
			e.printStackTrace();
		}

		num_left_nodes = 1 + LeftOutNodePositions.length + OutputRightSubject + OutputRightObject;
		return COMPLETED_FLAG;
	}

	public boolean sort_merge_join()
			throws IOException, InvalidTypeException, PageNotReadException, TupleUtilsException, SortException,
			LowMemException, UnknownKeyTypeException, Exception {
		_results = new Heapfile(left_itr.getFileName() + "smj");
		
		if (left_itr.isEmpty()){
			COMPLETED_FLAG = true;
			return COMPLETED_FLAG;
		}
		// Sort left iterator for outer loop and set outer basic pattern
		outerSort = new BPSort(left_itr, new BPOrder(0), BPJoinNodePosition, 32);
		outer_bp = outerSort.get_next();
		// outer_bp = left_itr.get_next();

		// Create a sorted stream for inner loop and set inner and temp quadruple and inner count
		if (rstream == null) {
			if (JoinOnSubjectorObject == 0) {
				rstream = new Stream(_rdfdb, QuadrupleOrder.SubjectConfidence, RightSubjectFilter, RightPredicateFilter,
						RightObjectFilter, RightConfidenceFilter);
			} else {
				rstream = new Stream(_rdfdb, QuadrupleOrder.ObjectConfidence, RightSubjectFilter, RightPredicateFilter,
						RightObjectFilter, RightConfidenceFilter);
			}
		}
		inner_quad = rstream.getNext();
		int innerCount = 0;
		System.out.println(rstream.getResults().getQuadrupleCnt());

		// Start of current partition
		while ((outer_bp != null) && (inner_quad != null)) {
			// Set labels to be compared
			EID join_eid_inner;
			if (JoinOnSubjectorObject == 0) {
				join_eid_inner = inner_quad.getSubjectQid();
			} else {
				join_eid_inner = inner_quad.getObjectQid();
			}
			String label_inner = _rdfdb.getEntityHeapFile().getLabel(join_eid_inner.returnLID()).getLabel();

			LID labelID = outer_bp.getEIDFld(BPJoinNodePosition).returnLID();
			String label_outer = _rdfdb.getEntityHeapFile().getLabel(labelID).getLabel();

			// Continue scan of outer loop
			while ((label_outer.compareTo(label_inner) < 0) && (outer_bp != null)) {
				outer_bp = outerSort.get_next();
				// outer_bp = left_itr.get_next();

				if (outer_bp != null) {
					labelID = outer_bp.getEIDFld(BPJoinNodePosition).returnLID();
					label_outer = _rdfdb.getEntityHeapFile().getLabel(labelID).getLabel();
				}
			}

			// Continue scan of inner loop
			while ((label_outer.compareTo(label_inner) > 0) && (inner_quad != null)) {
				inner_quad = rstream.getNext();
				innerCount++;

				if (inner_quad != null) {
					if (JoinOnSubjectorObject == 0) {
						join_eid_inner = inner_quad.getSubjectQid();
					} else {
						join_eid_inner = inner_quad.getObjectQid();
					}
					label_inner = _rdfdb.getEntityHeapFile().getLabel(join_eid_inner.returnLID()).getLabel();
				}
			}

			// Needed in case label_outer != label_inner
			// -----------------------------------------
			// Set new outer label
			if((outer_bp != null) && (inner_quad != null)){
				labelID = outer_bp.getEIDFld(BPJoinNodePosition).returnLID();
				label_outer = _rdfdb.getEntityHeapFile().getLabel(labelID).getLabel();

				// Set new inner label
				if (JoinOnSubjectorObject == 0) {
					join_eid_inner = inner_quad.getSubjectQid();
				} else {
					join_eid_inner = inner_quad.getObjectQid();
				}
				label_inner = _rdfdb.getEntityHeapFile().getLabel(join_eid_inner.returnLID()).getLabel();
			}
			// -----------------------------------------
			
			int partInnerCount = 0;
			while ((label_outer.compareTo(label_inner) == 0) && (outer_bp != null)) {
				System.out.println("innerCount: "+Integer.toString(innerCount));
				// Reset rstream
				// ----------------------
				if (rstream == null) {
					if (JoinOnSubjectorObject == 0) {
						rstream = new Stream(_rdfdb, QuadrupleOrder.SubjectConfidence, RightSubjectFilter, RightPredicateFilter,
								RightObjectFilter, RightConfidenceFilter);
					} else {
						rstream = new Stream(_rdfdb, QuadrupleOrder.ObjectConfidence, RightSubjectFilter, RightPredicateFilter,
								RightObjectFilter, RightConfidenceFilter);
					}
					inner_quad = rstream.getNext();
				}
				// ----------------------
				// rstream.reset_scan();
				for(int i=0; i<innerCount; i++){
					inner_quad = rstream.getNext();
				}
				System.out.println(inner_quad);
				if(inner_quad != null){
					if (JoinOnSubjectorObject == 0) {
						join_eid_inner = inner_quad.getSubjectQid();
					} else {
						join_eid_inner = inner_quad.getObjectQid();
					}
					label_inner = _rdfdb.getEntityHeapFile().getLabel(join_eid_inner.returnLID()).getLabel();
				}
				partInnerCount = 0;
				while ((label_outer.compareTo(label_inner) == 0) && (inner_quad != null)) {
					System.out.println("partInnerCount: "+Integer.toString(partInnerCount));
					// Add result to heapfile
					// ----------------------
					double new_confidence = Math.min(outer_bp.getDoubleFld(outer_bp.confidence_fld_num),
							inner_quad.getConfidence());

					BasicPattern tempBP = new BasicPattern(LeftOutNodePositions, outer_bp);
					tempBP.setDoubleFld(tempBP.confidence_fld_num, new_confidence);

					// Insert values into Basic Pattern
					if (OutputRightSubject == 1) {
						tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getSubjectQid());
						if (OutputRightObject == 1) {
							tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
						}
					} else if (OutputRightObject == 1) {
						tempBP.setEIDFld(tempBP.noOfFlds() + 1, inner_quad.getObjectQid());
					}
					_results.insertRecord(tempBP.returnTupleArray());
					// ----------------------

					// Increment inner stream
					// ----------------------
					inner_quad = rstream.getNext();
					partInnerCount++;
					if(inner_quad != null){
						if (JoinOnSubjectorObject == 0) {
							join_eid_inner = inner_quad.getSubjectQid();
						} else {
							join_eid_inner = inner_quad.getObjectQid();
						}
						label_inner = _rdfdb.getEntityHeapFile().getLabel(join_eid_inner.returnLID()).getLabel();
					}
					// ----------------------
				}
				// Increment outer stream
				// ----------------------
				outer_bp = outerSort.get_next();
				// outer_bp = left_itr.get_next();

				if(outer_bp != null){
					labelID = outer_bp.getEIDFld(BPJoinNodePosition).returnLID();
					label_outer = _rdfdb.getEntityHeapFile().getLabel(labelID).getLabel();
				}
				// ----------------------

				// Set rstream to null
				// ----------------------
				try {
					if (rstream != null) {
						rstream.closestream();
						rstream = null;
					}
				} catch (Exception e) {
					System.out.println("Error in closing stream in BT_Triple_Join");
					e.printStackTrace();
				}
				// ----------------------
			}
			// Initialize search for next partition
			innerCount += partInnerCount;
		}

		// if(outerSort != null){
		// 	outerSort.close();
		// 	outerSort = null;
		// }

		COMPLETED_FLAG = true;
		num_left_nodes = 1 + LeftOutNodePositions.length + OutputRightSubject + OutputRightObject;
		return COMPLETED_FLAG;
	}

	public void close() throws IOException, SortException {
		try {
			if (rstream != null) {
				// lstream.closestream();
				rstream.closestream();
				// qfs.closescan();
			}
		} catch (Exception e) {
			System.out.println("Error in closing BT_Triple_Join");
			e.printStackTrace();
		}

	}
}