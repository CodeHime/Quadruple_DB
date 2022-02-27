package quadrupleheap;

/** JAVA */
/**
 * Scan.java-  class Scan
 *
 */

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


/**	
 * A Scan object is created ONLY through the function openScan
 * of a HeapFile. It supports the getNext interface which will
 * simply retrieve the next quadruple in the heapfile.
 *
 * An object of type scan will always have pinned one directory page
 * of the heapfile.
 */
public class Scan implements GlobalConst{
 
    /**
     * Note that one quadruple in our way-cool HeapFile implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The heapfile we are using. */
    private QuadrupleHeapfile  _hf;

    /** PageId of current directory page (which is itself an THFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private THFPage dirpage = new THFPage();

    /** quadruple ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current quadruple lives.
     */
    private QID datapageQid = new QID();

    /** the actual PageId of the data page with the current quadruple */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private THFPage datapage = new THFPage();

    /** quadruple ID of the current quadruple (from the current data page) */
    private QID userqid = new QID();

    /** Status of next user status */
    private boolean nextUserStatus;
    
     
    /** The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
  public Scan(QuadrupleHeapfile hf) 
    throws InvalidTupleSizeException,
	   IOException
  {
	init(hf);
  }


  
  /** Retrieve the next quadruple in a sequential scan
   *
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   *
   * @param qid Quadruple ID of the quadruple
   * @return the Quadruple of the retrieved quadruple.
   */
  public Quadruple getNext(QID qid) 
    throws InvalidTupleSizeException,
	   IOException
  {
    Quadruple recptrtuple = null;
    
    if (nextUserStatus != true) {
        nextDataPage();
    }
     
    if (datapage == null)
      return null;
    
    qid.pageNo.pid = userqid.pageNo.pid;    
    qid.slotNo = userqid.slotNo;
         
    try {
      recptrtuple = datapage.getQuadruple(qid);
    }
    
    catch (Exception e) {
  //    System.err.println("SCAN: Error in Scan" + e);
      e.printStackTrace();
    }   
    
    userqid = datapage.nextQuadruple(qid);
    if(userqid == null) nextUserStatus = false;
    else nextUserStatus = true;
     
    return recptrtuple;
  }


    /** Position the scan cursor to the quadruple with the given qid.
     * 
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @param qid Quadruple ID of the given quadruple
     * @return 	true if successful, 
     *			false otherwise.
     */
  public boolean position(QID qid) 
    throws InvalidTupleSizeException,
	   IOException
  { 
    QID    nxtrid = new QID();
    boolean bst;

    bst = peekNext(nxtrid);

    if (nxtrid.equals(qid)==true) 
    	return true;

    // This is kind lame, but otherwise it will take all day.
    PageId pgid = new PageId();
    pgid.pid = qid.pageNo.pid;
 
    if (!datapageId.equals(pgid)) {

      // reset everything and start over from the beginning
      reset();
      
      bst =  firstDataPage();

      if (bst != true)
	return bst;
      
      while (!datapageId.equals(pgid)) {
	bst = nextDataPage();
	if (bst != true)
	  return bst;
      }
    }
    
    // Now we are on the correct page.
    
    try{
    	userqid = datapage.firstQuadruple();
	}
    catch (Exception e) {
      e.printStackTrace();
    }
	

    if (userqid == null)
      {
    	bst = false;
        return bst;
      }
    
    bst = peekNext(nxtrid);
    
    while ((bst == true) && (nxtrid != qid))
      bst = mvNext(nxtrid);
    
    return bst;
  }


    /** Do all the constructor work
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
    private void init(QuadrupleHeapfile hf) 
      throws InvalidTupleSizeException,
	     IOException
  {
	_hf = hf;

    	firstDataPage();
  }


    /** Closes the Scan object */
    public void closescan()
    {
    	reset();
    }
   

    /** Reset everything and unpin all pages. */
    private void reset()
    { 

    if (datapage != null) {
    
    try{
      unpinPage(datapageId, false);
    }
    catch (Exception e){
      // 	System.err.println("SCAN: Error in Scan" + e);
      e.printStackTrace();
    }  
    }
    datapageId.pid = 0;
    datapage = null;

    if (dirpage != null) {
    
      try{
	unpinPage(dirpageId, false);
      }
      catch (Exception e){
	//     System.err.println("SCAN: Error in Scan: " + e);
	e.printStackTrace();
      }
    }
    dirpage = null;
 
    nextUserStatus = true;

  }
 
 
  /** Move to the first data page in the file. 
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   * @return true if successful
   *         false otherwise
   */
  private boolean firstDataPage() 
    throws InvalidTupleSizeException,
	   IOException
  {
    DataPageInfo dpinfo;
    Quadruple        rectuple = null;
    Boolean      bst;

    /** copy data about first directory page */
 
    dirpageId.pid = _hf._firstDirPageId.pid;  
    nextUserStatus = true;

    /** get first directory page and pin it */
    	try {
	   dirpage  = new THFPage();
       	   pinPage(dirpageId, (Page) dirpage, false);	   
       }

    	catch (Exception e) {
    //    System.err.println("SCAN Error, try pinpage: " + e);
	e.printStackTrace();
	}
    
    /** now try to get a pointer to the first datapage */
	 datapageQid = dirpage.firstQuadruple();
	 
    	if (datapageQid != null) {
    /** there is a datapage quadruple on the first directory page: */
	
	try {
          rectuple = dirpage.getQuadruple(datapageQid);
	}  
				
	catch (Exception e) {
	//	System.err.println("SCAN: Chain Error in Scan: " + e);
		e.printStackTrace();
	}		
      			    
    	dpinfo = new DataPageInfo(rectuple);
        datapageId.pid = dpinfo.pageId.pid;

    } else {

    /** the first directory page is the only one which can possibly remain
     * empty: therefore try to get the next directory page and
     * check it. The next one has to contain a datapage quadruple, unless
     * the heapfile is empty:
     */
      PageId nextDirPageId = new PageId();
      
      nextDirPageId = dirpage.getNextPage();
      
      if (nextDirPageId.pid != INVALID_PAGE) {
	
	try {
            unpinPage(dirpageId, false);
            dirpage = null;
	    }
	
	catch (Exception e) {
	//	System.err.println("SCAN: Error in 1stdatapage 1 " + e);
		e.printStackTrace();
	}
        	
	try {
	
           dirpage = new THFPage();
	    pinPage(nextDirPageId, (Page )dirpage, false);
	
	    }
	
	catch (Exception e) {
	//  System.err.println("SCAN: Error in 1stdatapage 2 " + e);
	  e.printStackTrace();
	}
	
	/** now try again to read a data quadruple: */
	
	try {
	  datapageQid = dirpage.firstQuadruple();
	}
        
	catch (Exception e) {
	//  System.err.println("SCAN: Error in 1stdatapg 3 " + e);
	  e.printStackTrace();
	  datapageId.pid = INVALID_PAGE;
	}
       
	if(datapageQid != null) {
          
	  try {
	  
	    rectuple = dirpage.getQuadruple(datapageQid);
	  }
	  
	  catch (Exception e) {
	//    System.err.println("SCAN: Error getQuadruple 4: " + e);
	    e.printStackTrace();
	  }
	  
	  if (rectuple.getLength() != DataPageInfo.size)
	    return false;
	  
	  dpinfo = new DataPageInfo(rectuple);
	  datapageId.pid = dpinfo.pageId.pid;
	  
         } else {
	   // heapfile empty
           datapageId.pid = INVALID_PAGE;
         }
       }//end if01
       else {// heapfile empty
	datapageId.pid = INVALID_PAGE;
	}
}	
	
	datapage = null;

	try{
         nextDataPage();
	  }
	  
	catch (Exception e) {
	//  System.err.println("SCAN Error: 1st_next 0: " + e);
	  e.printStackTrace();
	}
	
      return true;
      
      /** ASSERTIONS:
       * - first directory page pinned
       * - this->dirpageId has Id of first directory page
       * - this->dirpage valid
       * - if heapfile empty:
       *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
       * - if heapfile nonempty:
       *    - this->datapage == NULL, this->datapageId, this->datapageQid valid
       *    - first datapage is not yet pinned
       */
    
  }
    

  /** Move to the next data page in the file and 
   * retrieve the next data page. 
   *
   * @return 		true if successful
   *			false if unsuccessful
   */
  private boolean nextDataPage() 
    throws InvalidTupleSizeException,
	   IOException
  {
    DataPageInfo dpinfo;
    
    boolean nextDataPageStatus;
    PageId nextDirPageId = new PageId();
    Quadruple rectuple = null;

  // ASSERTIONS:
  // - this->dirpageId has Id of current directory page
  // - this->dirpage is valid and pinned
  // (1) if heapfile empty:
  //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
  // (2) if overall first quadruple in heapfile:
  //    - this->datapage==NULL, but this->datapageId valid
  //    - this->datapageQid valid
  //    - current data page unpinned !!!
  // (3) if somewhere in heapfile
  //    - this->datapageId, this->datapage, this->datapageQid valid
  //    - current data page pinned
  // (4)- if the scan had already been done,
  //        dirpage = NULL;  datapageId = INVALID_PAGE
    
    if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
        return false;

    if (datapage == null) {
      if (datapageId.pid == INVALID_PAGE) {
	// heapfile is empty to begin with
	
	try{
	  unpinPage(dirpageId, false);
	  dirpage = null;
	}
	catch (Exception e){
	//  System.err.println("Scan: Chain Error: " + e);
	  e.printStackTrace();
	}
	
      } else {
	
	// pin first data page
	try {
	  datapage  = new THFPage();
	  pinPage(datapageId, (Page) datapage, false);
	}
	catch (Exception e){
	  e.printStackTrace();
	}
	
	try {
	  userqid = datapage.firstQuadruple();
	}
	catch (Exception e) {
	  e.printStackTrace();
	}
	
	return true;
        }
    }
  
  // ASSERTIONS:
  // - this->datapage, this->datapageId, this->datapageQid valid
  // - current datapage pinned

    // unpin the current datapage
    try{
      unpinPage(datapageId, false /* no dirty */);
        datapage = null;
    }
    catch (Exception e){
      
    }
          
    // read next datapagerecord from current directory page
    // dirpage is set to NULL at the end of scan. Hence
    
    if (dirpage == null) {
      return false;
    }
    
    datapageQid = dirpage.nextQuadruple(datapageQid);
    
    if (datapageQid == null) {
      nextDataPageStatus = false;
      // we have read all datapage records on the current directory page
      
      // get next directory page
      nextDirPageId = dirpage.getNextPage();
  
      // unpin the current directory page
      try {
	unpinPage(dirpageId, false /* not dirty */);
	dirpage = null;
	
	datapageId.pid = INVALID_PAGE;
      }
      
      catch (Exception e) {
	
      }
		    
      if (nextDirPageId.pid == INVALID_PAGE)
	return false;
      else {
	// ASSERTION:
	// - nextDirPageId has correct id of the page which is to get
	
	dirpageId = nextDirPageId;
	
 	try { 
	  dirpage  = new THFPage();
	  pinPage(dirpageId, (Page)dirpage, false);
	}
	
	catch (Exception e){
	  
	}
	
	if (dirpage == null)
	  return false;
	
    	try {
	  datapageQid = dirpage.firstQuadruple();
	  nextDataPageStatus = true;
	}
	catch (Exception e){
	  nextDataPageStatus = false;
	  return false;
	} 
      }
    }
    
    // ASSERTION:
    // - this->dirpageId, this->dirpage valid
    // - this->dirpage pinned
    // - the new datapage to be read is on dirpage
    // - this->datapageQid has the Qid of the next datapage to be read
    // - this->datapage, this->datapageId invalid
  
    // data page is not yet loaded: read its quadruple from the directory page
   	try {
	  rectuple = dirpage.getQuadruple(datapageQid);
	}
	
	catch (Exception e) {
	  System.err.println("HeapFile: Error in Scan" + e);
	}
	
	if (rectuple.getLength() != DataPageInfo.size)
	  return false;
                        
	dpinfo = new DataPageInfo(rectuple);
	datapageId.pid = dpinfo.pageId.pid;
	
 	try {
	  datapage = new THFPage();
	  pinPage(dpinfo.pageId, (Page) datapage, false);
	}
	
	catch (Exception e) {
	  System.err.println("HeapFile: Error in Scan" + e);
	}
	
     
     // - directory page is pinned
     // - datapage is pinned
     // - this->dirpageId, this->dirpage correct
     // - this->datapageId, this->datapage, this->datapageQid correct

     userqid = datapage.firstQuadruple();
     
     if(userqid == null)
     {
       nextUserStatus = false;
       return false;
     }
  
     return true;
  }


  private boolean peekNext(QID qid) {
    
    qid.pageNo.pid = userqid.pageNo.pid;
    qid.slotNo = userqid.slotNo;
    return true;
    
  }


  /** Move to the next quadruple in a sequential scan.
   * Also returns the QID of the (new) current quadruple.
   */
  private boolean mvNext(QID qid) 
    throws InvalidTupleSizeException,
	   IOException
  {
    QID nextqid;
    boolean status;

    if (datapage == null)
        return false;

    	nextqid = datapage.nextQuadruple(qid);
	
	if( nextqid != null ){
	  userqid.pageNo.pid = nextqid.pageNo.pid;
	  userqid.slotNo = nextqid.slotNo;
	  return true;
	} else {
	  
	  status = nextDataPage();

	  if (status==true){
	    qid.pageNo.pid = userqid.pageNo.pid;
	    qid.slotNo = userqid.slotNo;
	  }
	
	}
	return true;
  }

    /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
    }

  } // end of unpinPage


}