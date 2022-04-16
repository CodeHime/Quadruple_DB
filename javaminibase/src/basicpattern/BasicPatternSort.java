package basicpattern;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import index.*;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import chainexception.*;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class BasicPatternSort extends BasicPatternIterator implements GlobalConst {
  private static final int ARBIT_RUNS = 10;

  private AttrType[] _in;
  private short n_cols;
  private short[] str_lens;
  private BasicPatternIterator _am;
  private int _sort_fld;
  private AttrType _sort_fld_type;
  private BasicPatternOrder order;
  private int _n_pages;
  private byte[][] bufs;
  private boolean first_time;
  private int Nruns;
  private int max_elems_in_heap;
  private int sortFldLen;
  private int tuple_size;

  private BasicPatternPNodeSplayPQ Q;
  private Heapfile[] temp_files;
  private int n_tempfiles;
  private Tuple output_tuple;
  private int[] n_tuples;
  private int n_runs;
  private Tuple op_buf;
  private BasicPatternOBuf o_buf;
  private BasicPatternSpoofIbuf[] i_buf;
  private PageId[] bufs_pids;
  private boolean useBM = true; // flag for whether to use buffer manager

  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min)
   * from each run into a heap. <code>delete_min() </code> will then get
   * the minimum of all runs.
   * 
   * @param tuple_size size (in bytes) of each tuple
   * @param n_R_runs   number of runs
   * @exception IOException     from lower layers
   * @exception LowMemException there is not enough memory to
   *                            sort in two passes (a subclass of SortException).
   * @exception SortException   something went wrong in the lower layer.
   * @exception Exception       other exceptions
   */
  private void setup_for_merge(int tuple_size, int n_R_runs)
      throws IOException,
      LowMemException,
      SortException,
      Exception {
    // don't know what will happen if n_R_runs > _n_pages
    if (n_R_runs > _n_pages)
      throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");

    int i;
    BasicPatternPNode cur_node; // need pq_defs.java

    i_buf = new BasicPatternSpoofIbuf[n_R_runs]; // need io_bufs.java
    for (int j = 0; j < n_R_runs; j++)
      i_buf[j] = new BasicPatternSpoofIbuf();

    // construct the lists, ignore TEST for now
    // this is a patch, I am not sure whether it works well -- bingjie 4/20/98

    for (i = 0; i < n_R_runs; i++) {
      byte[][] apage = new byte[1][];
      apage[0] = bufs[i];

      // need iobufs.java
      i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_tuples[i]);

      cur_node = new BasicPatternPNode();
      cur_node.run_num = i;

      // may need change depending on whether Get() returns the original
      // or make a copy of the tuple, need io_bufs.java ???
      Tuple temp_tuple = new Tuple(tuple_size);

      try {
        temp_tuple.setHdr(n_cols, _in, str_lens);
      } catch (Exception e) {
        throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
      }

      temp_tuple = i_buf[i].Get(temp_tuple); // need io_bufs.java

      if (temp_tuple != null) {
        /*
         * System.out.print("Get tuple from run " + i);
         * temp_tuple.print(_in);
         */
        cur_node.tuple = temp_tuple; // no copy needed
        try {
          Q.enq(cur_node);
        } catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        } catch (TupleUtilsException e) {
          throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
        }

      }
    }
    return;
  }

  /**
   * Generate sorted runs.
   * Using heap sort.
   * 
   * @param max_elems   maximum number of elements in heap
   * @param sortFldType attribute type of the sort field
   * @param sortFldLen  length of the sort field
   * @return number of runs generated
   * @exception IOException    from lower layers
   * @exception SortException  something went wrong in the lower layer.
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems, int sortFldLen)
      throws IOException,
      SortException,
      UnknowAttrType,
      TupleUtilsException,
      JoinsException,
      Exception {
    BasicPattern basicPattern;
    Tuple tuple;
    BasicPatternPNode cur_node;
    BasicPatternPNodeSplayPQ Q1 = new BasicPatternPNodeSplayPQ(_sort_fld, _sort_fld_type, order);
    BasicPatternPNodeSplayPQ Q2 = new BasicPatternPNodeSplayPQ(_sort_fld, _sort_fld_type, order);
    BasicPatternPNodeSplayPQ pcurr_Q = Q1;
    BasicPatternPNodeSplayPQ pother_Q = Q2;
    

    int run_num = 0; // keeps track of the number of runs

    // number of elements in Q
    // int nelems_Q1 = 0;
    // int nelems_Q2 = 0;
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;

    int comp_res;

    // set the lastElem to be the minimum value for the sort field
    

    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
        basicPattern = _am.get_next();
        int basicPatternLength = basicPattern.getLength();
        short numberOfTupleFields = 1;
        while (basicPatternLength > 0) {
          basicPatternLength -= 8;
          numberOfTupleFields += 2;
        }
        AttrType[] tupletypes = new AttrType[numberOfTupleFields];
        short[] strSizes = new short[1];
        short tuplesize = 8;
        tupletypes[0] = new AttrType(AttrType.attrD);
        for (int i = 1; i < numberOfTupleFields; i++) {
          tupletypes[i] = new AttrType(AttrType.attrInteger);
          tuplesize += 4;
        }
        tuple = new Tuple();
        tuple.setHdr(numberOfTupleFields, tupletypes, strSizes);
        tuple.setDFld(0, basicPattern.getDoubleFld(0));
        for (int i = 1; i < numberOfTupleFields / 2; i++) {
          tuple.setIntFld(i, basicPattern.getEIDFld(i).pageNo.pid);
          tuple.setIntFld(i + 1, basicPattern.getEIDFld(i).slotNo);
        } // according to Iterator.java

        if (first_time) {
          Tuple t = new Tuple(); // need Tuple.java
          try {
            t.setHdr(numberOfTupleFields, tupletypes, strSizes);
          } catch (Exception e) {
            throw new SortException(e, "Sort.java: t.setHdr() failed");
          }
          tuple_size = t.size();

          // this may need change, bufs ??? need io_bufs.java
          // bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
          bufs_pids = new PageId[_n_pages];
          bufs = new byte[_n_pages][];

          if (useBM) {
            try {
              get_buffer_pages(_n_pages, bufs_pids, bufs);
            } catch (Exception e) {
              throw new SortException(e, "Sort.java: BUFmgr error");
            }
          } else {
            for (int k = 0; k < _n_pages; k++)
              bufs[k] = new byte[MAX_SPACE];
          }


          // as a heuristic, we set the number of runs to an arbitrary value
          // of ARBIT_RUNS
          temp_files = new Heapfile[ARBIT_RUNS];
          n_tempfiles = ARBIT_RUNS;
          n_tuples = new int[ARBIT_RUNS];
          n_runs = ARBIT_RUNS;

          try {
            temp_files[0] = new Heapfile(null);
          } catch (Exception e) {
            throw new SortException(e, "Sort.java: Heapfile error");
          }

          o_buf = new BasicPatternOBuf();

          o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
          // output_tuple = null;

          max_elems_in_heap = 200;

          Q = new BasicPatternPNodeSplayPQ(_sort_fld, _sort_fld_type, order);

          op_buf = new Tuple(tuple_size); // need Tuple.java
          try {
            op_buf.setHdr(n_cols, _in, str_lens);
          } catch (Exception e) {
            throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        throw new SortException(e, "Sort.java: get_next() failed");
      }

      if (tuple == null) {
        break;
      }
      cur_node = new BasicPatternPNode();
      cur_node.tuple = new Tuple(tuple); // tuple copy needed -- Bingjie 4/29/98

      pcurr_Q.enq(cur_node);
      p_elems_curr_Q++;
    }

    // now the queue is full, starting writing to file while keep trying
    // to add new tuples to the queue. The ones that does not fit are put
    // on the other queue temperarily
    Tuple lastElem = new Tuple(tuple_size); // need tuple.java
    try {
      lastElem.setHdr(n_cols, _in, str_lens);
    } catch (Exception e) {
      throw new SortException(e, "Sort.java: setHdr() failed");
    }

    if (order.basicPatternOrder == TupleOrder.Ascending) {
      try {
        MIN_VAL(lastElem, _sort_fld_type);
      } catch (UnknowAttrType e) {
        throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
      } catch (Exception e) {
        throw new SortException(e, "MIN_VAL failed");
      }
    } else {
      try {
        MAX_VAL(lastElem, _sort_fld_type);
      } catch (UnknowAttrType e) {
        throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
      } catch (Exception e) {
        throw new SortException(e, "MIN_VAL failed");
      }
    }

    while (true) {
      cur_node = pcurr_Q.deq();
      if (cur_node == null)
        break;
      p_elems_curr_Q--;

      comp_res = BasicPatternUtils.CompareTupleWithValue(_sort_fld_type, cur_node.tuple, _sort_fld, lastElem); // need
                                                                                                            // tuple_utils.java

      if ((comp_res < 0 && order.basicPatternOrder == TupleOrder.Ascending)
          || (comp_res > 0 && order.basicPatternOrder == TupleOrder.Descending)) {
        // doesn't fit in current run, put into the other queue
        try {
          pother_Q.enq(cur_node);
        } catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        }
        p_elems_other_Q++;
      } else {
        // set lastElem to have the value of the current tuple,
        // need tuple_utils.java
        BasicPatternUtils.SetValue(lastElem, cur_node.tuple, _sort_fld, _sort_fld_type);
        // write tuple to output file, need io_bufs.java, type cast???
        // System.out.println("Putting tuple into run " + (run_num + 1));
        // cur_node.tuple.print(_in);

        o_buf.Put(cur_node.tuple);
      }

      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
        // close current run and start next run
        n_tuples[run_num] = (int) o_buf.flush(); // need io_bufs.java
        run_num++;

        // check to see whether need to expand the array
        if (run_num == n_tempfiles) {
          Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
          for (int i = 0; i < n_tempfiles; i++) {
            temp1[i] = temp_files[i];
          }
          temp_files = temp1;
          n_tempfiles *= 2;

          int[] temp2 = new int[2 * n_runs];
          for (int j = 0; j < n_runs; j++) {
            temp2[j] = n_tuples[j];
          }
          n_tuples = temp2;
          n_runs *= 2;
        }

        try {
          temp_files[run_num] = new Heapfile(null);
        } catch (Exception e) {
          throw new SortException(e, "Sort.java: create Heapfile failed");
        }

        // need io_bufs.java
        o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

        // set the last Elem to be the minimum value for the sort field
        if (order.basicPatternOrder == TupleOrder.Ascending) {
          try {
            MIN_VAL(lastElem, _sort_fld_type);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          }
        } else {
          try {
            MAX_VAL(lastElem, _sort_fld_type);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          }
        }

        // switch the current heap and the other heap
        BasicPatternPNodeSplayPQ tempQ = pcurr_Q;
        pcurr_Q = pother_Q;
        pother_Q = tempQ;
        int tempelems = p_elems_curr_Q;
        p_elems_curr_Q = p_elems_other_Q;
        p_elems_other_Q = tempelems;
      }

      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
          try {
            basicPattern = _am.get_next();
            int basicPatternLength = basicPattern.getLength();
            short numberOfTupleFields = 1;
            while (basicPatternLength > 0) {
              basicPatternLength -= 8;
              numberOfTupleFields += 2;
            }
            AttrType[] tupletypes = new AttrType[numberOfTupleFields];
            short[] strSizes = new short[1];
            short tuplesize = 8;
            tupletypes[0] = new AttrType(AttrType.attrD);
            for (int i = 1; i < numberOfTupleFields; i++) {
              tupletypes[i] = new AttrType(AttrType.attrInteger);
              tuplesize += 4;
            }
            tuple = new Tuple();
            tuple.setHdr(numberOfTupleFields, tupletypes, strSizes);
            tuple.setDFld(0, basicPattern.getDoubleFld(0));
            for (int i = 1; i < numberOfTupleFields / 2; i++) {
              tuple.setIntFld(i, basicPattern.getEIDFld(i).pageNo.pid);
              tuple.setIntFld(i + 1, basicPattern.getEIDFld(i).slotNo);
            }

            // according to
            // Iterator.java
          } catch (Exception e) {
            throw new SortException(e, "get_next() failed");
          }

          if (tuple == null) {
            break;
          }
          cur_node = new BasicPatternPNode();
          cur_node.tuple = new Tuple(tuple); // tuple copy needed -- Bingjie 4/29/98

          try {
            pcurr_Q.enq(cur_node);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
          }
          p_elems_curr_Q++;
        }
      }

      // Check if we are done
      if (p_elems_curr_Q == 0) {
        // current queue empty despite our attemps to fill in
        // indicating no more tuples from input
        if (p_elems_other_Q == 0) {
          // other queue is also empty, no more tuples to write out, done
          break; // of the while(true) loop
        } else {
          // generate one more run for all tuples in the other queue
          // close current run and start next run
          n_tuples[run_num] = (int) o_buf.flush(); // need io_bufs.java
          run_num++;

          // check to see whether need to expand the array
          if (run_num == n_tempfiles) {
            Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
            for (int i = 0; i < n_tempfiles; i++) {
              temp1[i] = temp_files[i];
            }
            temp_files = temp1;
            n_tempfiles *= 2;

            int[] temp2 = new int[2 * n_runs];
            for (int j = 0; j < n_runs; j++) {
              temp2[j] = n_tuples[j];
            }
            n_tuples = temp2;
            n_runs *= 2;
          }

          try {
            temp_files[run_num] = new Heapfile(null);
          } catch (Exception e) {
            throw new SortException(e, "Sort.java: create Heapfile failed");
          }

          // need io_bufs.java
          o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

          // set the last Elem to be the minimum value for the sort field
          if (order.basicPatternOrder == TupleOrder.Ascending) {
            try {
              MIN_VAL(lastElem, _sort_fld_type);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            }
          } else {
            try {
              MAX_VAL(lastElem, _sort_fld_type);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            }
          }

          // switch the current heap and the other heap
          BasicPatternPNodeSplayPQ tempQ = pcurr_Q;
          pcurr_Q = pother_Q;
          pother_Q = tempQ;
          int tempelems = p_elems_curr_Q;
          p_elems_curr_Q = p_elems_other_Q;
          p_elems_other_Q = tempelems;
        }
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_tuples[run_num] = (int) o_buf.flush();
    run_num++;

    return run_num;
  }

  /**
   * Remove the minimum value among all the runs.
   * 
   * @return the minimum tuple removed
   * @exception IOException   from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  private Tuple delete_min()
      throws IOException,
      SortException,
      Exception {
    BasicPatternPNode cur_node; // needs pq_defs.java
    Tuple new_tuple, old_tuple;

    cur_node = Q.deq();
    old_tuple = cur_node.tuple;
    /*
     * System.out.print("Get ");
     * old_tuple.print(_in);
     */
    // we just removed one tuple from one run, now we need to put another
    // tuple of the same run into the queue
    if (i_buf[cur_node.run_num].empty() != true) {
      // run not exhausted
      new_tuple = new Tuple(tuple_size); // need tuple.java??

      try {
        new_tuple.setHdr(n_cols, _in, str_lens);
      } catch (Exception e) {
        throw new SortException(e, "Sort.java: setHdr() failed");
      }

      new_tuple = i_buf[cur_node.run_num].Get(new_tuple);
      if (new_tuple != null) {
        /*
         * System.out.print(" fill in from run " + cur_node.run_num);
         * new_tuple.print(_in);
         */
        cur_node.tuple = new_tuple; // no copy needed -- I think Bingjie 4/22/98
        try {
          Q.enq(cur_node);
        } catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        } catch (TupleUtilsException e) {
          throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
        }
      } else {
        throw new SortException("********** Wait a minute, I thought input is not empty ***************");
      }

    }

    // changed to return Tuple instead of return char array ????
    return old_tuple;
  }

  /**
   * Set lastElem to be the minimum value of the appropriate type
   * 
   * @param lastElem    the tuple
   * @param sortFldType the sort field type
   * @exception IOException    from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MIN_VAL(Tuple lastElem, AttrType sortFldType)
      throws IOException,
      FieldNumberOutOfBoundException,
      UnknowAttrType {

    // short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    // AttrType[] junk = new AttrType[1];
    // junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MIN_VALUE;
    String s = new String(c);
    // short fld_no = 1;

    switch (sortFldType.attrType) {
      case AttrType.attrInteger:
        // lastElem.setHdr(fld_no, junk, null);
        lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
        break;
      case AttrType.attrReal:
        // lastElem.setHdr(fld-no, junk, null);
        lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
        break;
      case AttrType.attrString:
        // lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(_sort_fld, s);
        break;
      default:
        // don't know how to handle attrSymbol, attrNull
        // System.err.println("error in sort.java");
        throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }

    return;
  }

  /**
   * Set lastElem to be the maximum value of the appropriate type
   * 
   * @param lastElem    the tuple
   * @param sortFldType the sort field type
   * @exception IOException    from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MAX_VAL(Tuple lastElem, AttrType sortFldType)
      throws IOException,
      FieldNumberOutOfBoundException,
      UnknowAttrType {

    // short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    // AttrType[] junk = new AttrType[1];
    // junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MAX_VALUE;
    String s = new String(c);
    // short fld_no = 1;

    switch (sortFldType.attrType) {
      case AttrType.attrInteger:
        // lastElem.setHdr(fld_no, junk, null);
        lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
        break;
      case AttrType.attrReal:
        // lastElem.setHdr(fld_no, junk, null);
        lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
        break;
      case AttrType.attrString:
        // lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(_sort_fld, s);
        break;
      default:
        // don't know how to handle attrSymbol, attrNull
        // System.err.println("error in sort.java");
        throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }

    return;
  }

  /**
   * Class constructor, take information about the tuples, and set up
   * the sorting
   * 
   * @param in             array containing attribute types of the relation
   * @param len_in         number of columns in the relation
   * @param str_sizes      array of sizes of string attributes
   * @param am             an iterator for accessing the tuples
   * @param sort_fld       the field number of the field to sort on
   * @param sort_order     the sorting order (ASCENDING, DESCENDING)
   * @param sort_field_len the length of the sort field
   * @param n_pages        amount of memory (in pages) available for sorting
   * @exception IOException   from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  public BasicPatternSort(
      BasicPatternIterator am,
      int sort_fld,
      BasicPatternOrder sort_order,
      int n_pages) throws IOException, SortException {

    _am = am;
    _sort_fld = sort_fld;
    order = sort_order;
    _n_pages = n_pages;

    if(_sort_fld == 0){
      _sort_fld_type = new AttrType(AttrType.attrD);
    }


    first_time = true;

  }

  /**
   * Returns the next tuple in sorted order.
   * Note: You need to copy out the content of the tuple, otherwise it
   * will be overwritten by the next <code>get_next()</code> call.
   * 
   * @return the next tuple, null if all tuples exhausted
   * @exception IOException     from lower layers
   * @exception SortException   something went wrong in the lower layer.
   * @exception JoinsException  from <code>generate_runs()</code>.
   * @exception UnknowAttrType  attribute type unknown
   * @exception LowMemException memory low exception
   * @exception Exception       other exceptions
   */
  public BasicPattern get_next()
      throws IOException,
      SortException,
      UnknowAttrType,
      LowMemException,
      JoinsException,
      Exception {
    if (first_time) {
      // first get_next call to the sort routine
      first_time = false;

      // generate runs
      Nruns = generate_runs(max_elems_in_heap, sortFldLen);
      // System.out.println("Generated " + Nruns + " runs");

      // setup state to perform merge of runs.
      // Open input buffers for all the input file
      setup_for_merge(tuple_size, Nruns);
    }

    if (Q.empty()) {
      // no more tuples availble
      return null;
    }

    output_tuple = delete_min();
    if (output_tuple != null) {
      op_buf.tupleCopy(output_tuple);

      return new BasicPattern(op_buf.returnTupleByteArray(),0,op_buf.returnTupleByteArray().length);
    } else
      return null;
  }

  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * 
   * @exception IOException   from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  public void close() throws SortException, IOException {
    // clean up
    if (!closeFlag) {

      try {
        _am.close();
      } catch (Exception e) {
        throw new SortException(e, "Sort.java: error in closing iterator.");
      }

      if (useBM) {
        try {
          free_buffer_pages(_n_pages, bufs_pids);
        } catch (Exception e) {
          throw new SortException(e, "Sort.java: BUFmgr error");
        }
        for (int i = 0; i < _n_pages; i++)
          bufs_pids[i].pid = INVALID_PAGE;
      }

      for (int i = 0; i < temp_files.length; i++) {
        if (temp_files[i] != null) {
          try {
            temp_files[i].deleteFile();
          } catch (Exception e) {
            throw new SortException(e, "Sort.java: Heapfile error");
          }
          temp_files[i] = null;
        }
      }
      closeFlag = true;
    }
  }

}
