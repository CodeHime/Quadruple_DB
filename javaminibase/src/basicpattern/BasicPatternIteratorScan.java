package basicpattern;

import java.io.IOException;

import bufmgr.PageNotReadException;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.Flags;
import global.PageId;
import global.QID;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.FileScanException;
import iterator.IteratorBMException;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;
import quadrupleheap.TScan;

/**
 * All the relational operators and access methods are iterators.
 */
public class BasicPatternIteratorScan extends BPIterator implements Flags {

  /**
   * a flag to indicate whether this iterator has been closed.
   * it is set to true the first time the <code>close()</code>
   * function is called.
   * multiple calls to the <code>close()</code> function will
   * not be a problem.
   */
  public boolean closeFlag = false; // added by bingjie 5/4/98
  private Heapfile f = null;
  private QuadrupleHeapfile q = null;
  private Scan scan;
  private TScan tscan;
  private Tuple tuple;
  private Quadruple quadruple;
  private String filename;

  /**
   * abstract method, every subclass must implement it.
   * 
   * @return the result tuple
   * @throws FileScanException
   * @exception IOException               I/O errors
   * @exception JoinsException            some join exception
   * @exception IndexException            exception from super class
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception InvalidTypeException      tuple type not valid
   * @exception PageNotReadException      exception from lower layer
   * @exception TupleUtilsException       exception from using tuple utilities
   * @exception PredEvalException         exception from PredEval class
   * @exception SortException             sort exception
   * @exception LowMemException           memory error
   * @exception UnknowAttrType            attribute type unknown
   * @exception UnknownKeyTypeException   key type unknown
   * @exception Exception                 other exceptions
   */

  // For Quadruples
  public BasicPatternIteratorScan(String file_name) throws FileScanException, InvalidTupleSizeException {

    filename = file_name;
    quadruple = new Quadruple();

    try {
      q = new QuadrupleHeapfile(file_name);
      tscan = q.openScan();
    } catch (HFException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HFBufMgrException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HFDiskMgrException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public BasicPatternIteratorScan(String file_name, int nOutFlds) throws FileScanException {

    filename = file_name;

    tuple = new Tuple();

    AttrType[] attrTypes = new AttrType[nOutFlds];
    short[] sizes = new short[1];
    attrTypes[0] = new AttrType(AttrType.attrD);
    short s = 8;
    for (int i = 1; i < nOutFlds; i++) {
      attrTypes[i] = new AttrType(AttrType.attrInteger);
      s += 4;
    }
    sizes[0] = s;
    try {
      tuple.setHdr((short) (nOutFlds), attrTypes, sizes);
    } catch (Exception e) {
      throw new FileScanException(e, "setHdr() failed");
    }

    try {
      f = new Heapfile(file_name);

    } catch (Exception e) {
      throw new FileScanException(e, "Create new heapfile failed");
    }

    try {
      scan = f.openScan();
    } catch (Exception e) {
      throw new FileScanException(e, "openScan() failed");
    }
  }

  public String getFileName(){
    return filename;
  }
  public BasicPattern get_next()
      throws IOException,
      JoinsException,
      IndexException,
      InvalidTupleSizeException,
      InvalidTypeException,
      PageNotReadException,
      TupleUtilsException,
      PredEvalException,
      SortException,
      LowMemException,
      UnknowAttrType,
      UnknownKeyTypeException,
      Exception {
    // TODO: QID?
    RID rid = new RID();
    QID qid = new QID();

    if (q == null) {
      while (true) {
        if ((tuple = scan.getNext(rid)) == null) {
          return null;
        }
        
        // return new BasicPattern(tuple.returnTupleByteArray(), 0, tuple.returnTupleByteArray().length);
        return new BasicPattern(tuple);

      }
    } else {
      while (true) {
        if ((quadruple = tscan.getNext(qid)) == null) {
          return null;
        }
        //return new BasicPattern(quadruple.returnQuadrupleByteArray(), 0, 32);
        // byte[] quad = new byte[32];
        // Convert.setDoubleValue(quadruple.getConfidence(), 0, quad);
        // Convert.setIntValue(quadruple.getSubjectQid().pageNo.pid, 8, quad);
        // Convert.setIntValue(quadruple.getSubjectQid().slotNo, 12, quad);
        // Convert.setIntValue(quadruple.getPredicateID().pageNo.pid, 16, quad);
        // Convert.setIntValue(quadruple.getPredicateID().slotNo, 20, quad);
        // Convert.setIntValue(quadruple.getObjectQid().pageNo.pid, 24, quad);
        // Convert.setIntValue(quadruple.getObjectQid().slotNo, 28, quad);

        // return new BasicPattern(quad, 0, 32);

        return new BasicPattern(quadruple);
        
        
        
        


      }

    }
  }

  /**
   * @exception IOException    I/O errors
   * @exception JoinsException some join exception
   * @exception IndexException exception from Index class
   * @exception SortException  exception Sort class
   */
  public void close()
      throws IOException,
      JoinsException,
      SortException,
      IndexException {
    if (!closeFlag) {
      scan.closescan();
      closeFlag = true;
    }
  }

  /**
   * tries to get n_pages of buffer space
   * 
   * @param n_pages the number of pages
   * @param PageIds the corresponding PageId for each page
   * @param bufs    the buffer space
   * @exception IteratorBMException exceptions from bufmgr layer
   */
  public void get_buffer_pages(int n_pages, PageId[] PageIds, byte[][] bufs)
      throws IteratorBMException {
    Page pgptr = new Page();
    PageId pgid = null;

    for (int i = 0; i < n_pages; i++) {
      pgptr.setpage(bufs[i]);

      pgid = newPage(pgptr, 1);
      PageIds[i] = new PageId(pgid.pid);

      bufs[i] = pgptr.getpage();

    }
  }

  /**
   * free all the buffer pages we requested earlier.
   * should be called in the destructor
   * 
   * @param n_pages the number of pages
   * @param PageIds the corresponding PageId for each page
   * @exception IteratorBMException exception from bufmgr class
   */
  public void free_buffer_pages(int n_pages, PageId[] PageIds)
      throws IteratorBMException {
    for (int i = 0; i < n_pages; i++) {
      freePage(PageIds[i]);
    }
  }

  private void freePage(PageId pageno)
      throws IteratorBMException {

    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    } catch (Exception e) {
      throw new IteratorBMException(e, "Iterator.java: freePage() failed");
    }

  } // end of freePage

  private PageId newPage(Page page, int num)
      throws IteratorBMException {

    PageId tmpId = new PageId();

    try {
      tmpId = SystemDefs.JavabaseBM.newPage(page, num);
    } catch (Exception e) {
      throw new IteratorBMException(e, "Iterator.java: newPage() failed");
    }

    return tmpId;

  } // end of newPage
}
