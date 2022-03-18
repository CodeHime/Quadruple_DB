package heap;

/** JAVA */
/**
 * Stream.java-  class Stream
 *
 */

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;

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
3. Iterate over indexes (getNext)
	- No index => return result from full scan 
	OR
	- If next sorted tuple exists, get Labels for subject, object and predicates, compare the labels and return triple if filters match
		- If use_index=false, return first triple
		- else if nextQID exists, return the next match 
4. Close stream
	- delete all index files
	- close scan
	- close sorting method
	

*/

/**	
 * A Stream object is created ONLY through the function openStream
 * of a rdfDB. It supports the getNext interface which will
 * simply retrieve the next record in the rdfDB.
 *Heapfile
 * An object of type Stream will always have pinned one directory page
 * of the rdfDB.
 */
 /* Changes
  * HeapFile-> QuadrupleHeapfile ->rdfDB
  * RID->QID
  * HFPage->THFPage
  * Scan->Stream
  * Tuple->Quadruple
  * dirPageId.PageID -> dirPageId.pageno
  */
public class Stream implements GlobalConst{
 
    /**
     * Note that one record in our way-cool rdfDB implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The rdfDB we are using. */
    private rdfDB  _rdfdb;
    int _orderType;
    String _subjectFilter, _predicateFilter, _objectFilter;
    double _confidenceFilter;
    
    boolean _subjectNullFilter=false;
    boolean _predicateNullFilter=false;
    boolean _objectNullFilter=false;
    boolean _confidenceNullFilter=false;
    
    EID _subjectID=new EID();
    PID _predicateID=new PID();
    EID _objectID=new EID();
    
    int SORT_Q_NUM_PAGES=16;
    QuadrupleSort qsort =null;
    QuadrupleHeapFile _results = null;
    
    //
    boolean use_index=true;

    /** PageId of current directory page (which is itself an THFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private THFPage dirpage = new THFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private QID datapageQid = new QID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private THFPage datapage = new THFPage();

    /** record ID of the current record (from the current data page) */
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
     * @param rdfdb A rdfDB object
     */
    public Stream(rdfDB rdfdatabase, int orderType, String subjectFilter,String predicateFilter, String objectFilter, double confidenceFilter) 
    throws InvalidTupleSizeException,
	   IOException
  {
	// set null filters and filter values
	init(rdfdatabase, orderType, subjectFilter, predicateFilter, objectFilter, confidenceFilter);
	//Scan file using index
	if(!_subjectNullFilter & !_predicateNullFilter & !_objectNullFilter &!_confidenceNullFilter)
	{
	    // No nulls so we can perform a filter on all columns in the full btree 
	    scan_on_btree = true;
	    ScanBTreeIndex()
	}
	else
	{
	    if(indexValidForStream())
	    {
		use_index=true;
		//TODO:
		ScanBTreeIndex();
	    }
	    else{
		// None of the above options were selected or the index cannot be created for the given filter as one of the column values used in the index is null
		use_index = false;
		//TODO:
		ScanHF(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
	    }
	    
	    // Sort results
	    //TODO: set class name and define instance
	    qsort = new QuadrupleSort (_results);
	    QuadrupleOrder quadrupleOrder = new QuadrupleOrder(_orderType);
	    try{
	      qsort = new QuadrupleSort(qresults, quadrupleOrder, SORT_Q_NUM_PAGES)
	    }
	    catch (Exception e)
	    {
	      e.printStacktrace();
	    }
	}
  }


    public boolean indexValidForStream(){
	if (_rdfdb.getIndexOption()==1 && !_objectNullFilter){
	    return true;
	}
	else if (_rdfdb.getIndexOption()==2 && !_predicateNullFilter){
	    return true;
	}
	else if (_rdfdb.getIndexOption()==3 && !_subjectNullFilter){
	    return true;
	}
	else if (_rdfdb.getIndexOption()==4 && !_objectNullFilter && !_predicateNullFilter){
	    return true;
	}
	else if (_rdfdb.getIndexOption()==5 && !_predicateNullFilter && !_subjectNullFilter){
	    return true;
	}
	else {
	    return false;
	}
    }
    
    public static LID getEID(String EntityLabel){
	LID eid=null;
	LabelBTreeFile entityBTFile = new LabelBTreeFile(rdfDB_name + Integer.toString(indexOption) + "EntityBT");
	
	KeyClass eid_key=new StringKey(EntityLabel);
	
	KeyDataEntry entry = null;
	
	try{
	    LabelBTFileScan scan = entityBTFile.new_scan(eid_key, eid_key);
	    KeyDataEntry entry = scan.get_next();
	    // entry is not already in btree
            if(entry == null){
                System.out.println("No EID for given filter found.");
            }
            // entry already exists, return existing EID
            else{
                eid = ((LabelLeafData)entry.data).getData().returnEID();
            }
            scan.DestroyBTreeFileScan();
            entityBTFile.close();
	}
	catch(Exception e){
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
	}
	return eid;
    }
    
    public static LID getPID(String PredicateLabel){
	LID pid=null;
	LabelBTreeFile predBTFile = new LabelBTreeFile(rdfDB_name + Integer.toString(indexOption) + "PredBT");
	
	KeyClass pid_key=new StringKey(PredicateLabel);
	
	KeyDataEntry entry = null;
	
	try{
	    LabelBTFileScan scan = predBTFile.new_scan(pid_key, pid_key);
	    KeyDataEntry entry = scan.get_next();
	    // entry is not already in btree
            if(entry == null){
                System.out.println("No PID for given filter found.");
            }
            // entry already exists, return existing EID
            else{
                pid = ((LabelLeafData)entry.data).getData().returnPID();
            }
            scan.DestroyBTreeFileScan();
            predBTFile.close();
	}
	catch(Exception e){
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
	}
	return pid;
    }
    
    public boolean ScanBTreeIndex(){
	if(indexValidForStream())
	{
	    QID qid = null;
	    try {
	      QuadBTreeFile quadBTFile = new QuadBTreeFile(rdfDB_name + Integer.toString(indexOption) + "QuadBT");
	      
	      //TODO: get how to get QuadPtr or how to make key
	      LID subjectid= getEID(_subjectFilter);
	      LID predicateid= getPID(_predicateFilter);
	      LID objectid= getEID(_objectFilter);
	      
	      byte quadruplePtr[] = new byte[28];
	      Convert.setIntValue(subjectid.pageNo.pid,0,quadruplePtr);
	      Convert.setIntValue(subjectid.slotNo,4,quadruplePtr);
	      Convert.setIntValue(predicateid.pageNo.pid,8,quadruplePtr);
	      Convert.setIntValue(predicateid.slotNo,12,quadruplePtr);
	      Convert.setIntValue(objectid.pageNo.pid,16,quadruplePtr);
	      Convert.setIntValue(objectid.slotNo,20,quadruplePtr);
	      Convert.setDoubleValue(Convert.getDoubleValue(_confidenceFilter, 24, quadruplePtr)); 

	      KeyClass key = getStringKey(quadruplePtr);
	      
	      QuadBTFileScan scan = quadBTFile.new_scan(key, key);
	      KeyDataEntry entry = scan.get_next();

	      // The quadruple is not already in the btree, return false
	      if(entry == null)
	      {
		  System.out.println("No match found");
		  return false;
	      }
	      // btree found a match
	      else{
		while(entry != null) {
		    boolean entryMatch = true;
		    // get qid of given entry
		    qid = ((QuadLeafData)entry.data).getData();
		    Quadruple oldQuad = quad_heap_file.getQuadruple(qid);
		    byte[] oldQuad = quad_heap_file.getQuadruple(qid).getQuadrupleByteArray();

		    // compare subject, predicate, object of the quadruple. These are the first 24 bytes
		    if(!_subjectNullFilter)
		    {
			if(Arrays.equals(Convert.getIntValue(0, quadruplePtr), Convert.getIntValue(0, oldQuad.getQuadrupleByteArray())))
			{
			    
			}
		    }
		    
		    byte[] oldBytes = getFirstNBytes(oldQuad.getQuadrupleByteArray(), 24);
		    byte[] filterBytes = getFirstNBytes(quadruplePtr, 24);

		    if ( Arrays.equals(oldBytes, newBytes)){
			// TODO: convert into double instead from Jack's branch
			// DESIGN DECISION: Confidence is updatable SO to decrease #of sorts needed to be done and unreliable Indexes
			double new_confidence = Convert.getDoubleValue(24, quadruplePtr);
			double old_confidence = Convert.getDoubleValue(24, oldQuad.getQuadrupleByteArray());
	
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
	else
	{
	    
	}
    
	
	
    }
  /** Retrieve the next record in a sequential Stream
   *
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   *
   * @param qid Record ID of the record
   * @return the Quadruple of the retrieved record.
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
      recptrtuple = datapage.getRecord(qid);
    }
    
    catch (Exception e) {
  //    System.err.println("STREAM: Error in Stream" + e);
      e.printStackTrace();
    }   
    
    userqid = datapage.nextRecord(qid);
    if(userqid == null) nextUserStatus = false;
    else nextUserStatus = true;
     
    return recptrtuple;
  }


    /** Position the Stream cursor to the record with the given qid.
     * 
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @param qid Record ID of the given record
     * @return 	true if successful, 
     *			false otherwise.
     */
  public boolean position(QID qid) 
    throws InvalidTupleSizeException,
	   IOException
  { 
    QID    nxtqid = new QID();
    boolean bst;

    bst = peekNext(nxtqid);

    if (nxtqid.equals(qid)==true) 
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
    	userqid = datapage.firstRecord();
	}
    catch (Exception e) {
      e.printStackTrace();
    }
  

    if (userqid == null)
      {
    	bst = false;
        return bst;
      }
    
    bst = peekNext(nxtqid);
    
    while ((bst == true) && (nxtqid != qid))
      bst = mvNext(nxtqid);
    
    return bst;
  }


    /** Do all the constructor work
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param rdfdb A rdfDB object
     */
    private void init(rdfDB rdfdb, int orderType, String subjectFilter,String predicateFilter, String objectFilter, double confidenceFilter) 
      throws InvalidTupleSizeException,
	     IOException
  {
	// set variables and filters
	_rdfdb = rdfdb;
	_orderType=orderType;
	
	if(subjectFilter.compareToIgnoreCase("null")==0)
	{
	  _subjectNullFilter=true;
	}
	if(predicateFilter.compareToIgnoreCase("null")==0)
	{
	  _predicateNullFilter=true;
	}
	if(objectFilter.compareToIgnoreCase("null")==0)
	{
	  _objectNullFilter=true;
	}
	if(confidenceFilter.compareToIgnoreCase("null")==0)
	{
	  _confidenceNullFilter=true;
	}
	_subjectFilter=subjectFilter;
	_predicateFilter=predicateFilter;
	_objectFilter=objectFilter;
	_confidenceFilter=confidenceFilter;

    	firstDataPage();
  }


    /** Closes the Stream object */
    public void closestream()
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
      // 	System.err.println("STREAM: Error in Stream" + e);
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
	//     System.err.println("STREAM: Error in Stream: " + e);
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
 
    dirpageId.pid = _rdfdb.pageno.pid;  
    nextUserStatus = true;

    /** get first directory page and pin it */
    	try {
	   dirpage  = new THFPage();
       	   pinPage(dirpageId, (Page) dirpage, false);	   
       }

    	catch (Exception e) {
    //    System.err.println("Stream Error, try pinpage: " + e);
	e.printStackTrace();
	}
    
    /** now try to get a pointer to the first datapage */
	 datapageQid = dirpage.firstRecord();
	 
    	if (datapageQid != null) {
    /** there is a datapage record on the first directory page: */
	
	try {
          rectuple = dirpage.getRecord(datapageQid);
	}  
				
	catch (Exception e) {
	//	System.err.println("Stream: Chain Error in Stream: " + e);
		e.printStackTrace();
	}		
      			    
    	dpinfo = new DataPageInfo(rectuple);
        datapageId.pid = dpinfo.pageId.pid;

    } else {

    /** the first directory page is the only one which can possibly remain
     * empty: therefore try to get the next directory page and
     * check it. The next one has to contain a datapage record, unless
     * the rdfDB is empty:
     */
      PageId nextDirPageId = new PageId();
      
      nextDirPageId = dirpage.getNextPage();
      
      if (nextDirPageId.pid != INVALID_PAGE) {
	
	try {
            unpinPage(dirpageId, false);
            dirpage = null;
	    }
	
	catch (Exception e) {
	//	System.err.println("Stream: Error in 1stdatapage 1 " + e);
		e.printStackTrace();
	}
        	
	try {
	
           dirpage = new THFPage();
	    pinPage(nextDirPageId, (Page )dirpage, false);
	
	    }
	
	catch (Exception e) {
	//  System.err.println("Stream: Error in 1stdatapage 2 " + e);
	  e.printStackTrace();
	}
	
	/** now try again to read a data record: */
	
	try {
	  datapageQid = dirpage.firstRecord();
	}
        
	catch (Exception e) {
	//  System.err.println("Stream: Error in 1stdatapg 3 " + e);
	  e.printStackTrace();
	  datapageId.pid = INVALID_PAGE;
	}
       
	if(datapageQid != null) {
          
	  try {
	  
	    rectuple = dirpage.getRecord(datapageQid);
	  }
	  
	  catch (Exception e) {
	//    System.err.println("Stream: Error getRecord 4: " + e);
	    e.printStackTrace();
	  }
	  
	  if (rectuple.getLength() != DataPageInfo.size)
	    return false;
	  
	  dpinfo = new DataPageInfo(rectuple);
	  datapageId.pid = dpinfo.pageId.pid;
	  
         } else {
	   // rdfDB empty
           datapageId.pid = INVALID_PAGE;
         }
       }//end if01
       else {// rdfDB empty
	datapageId.pid = INVALID_PAGE;
	}
}	
	
	datapage = null;

	try{
         nextDataPage();
	  }
	  
	catch (Exception e) {
	//  System.err.println("Stream Error: 1st_next 0: " + e);
	  e.printStackTrace();
	}
	
      return true;
      
      /** ASSERTIONS:
       * - first directory page pinned
       * - this->dirpageId has Id of first directory page
       * - this->dirpage valid
       * - if rdfDB empty:
       *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
       * - if rdfDB nonempty:
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
  // (1) if rdfDB empty:
  //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
  // (2) if overall first record in rdfDB:
  //    - this->datapage==NULL, but this->datapageId valid
  //    - this->datapageQid valid
  //    - current data page unpinned !!!
  // (3) if somewhere in rdfDB
  //    - this->datapageId, this->datapage, this->datapageQid valid
  //    - current data page pinned
  // (4)- if the Stream had already been done,
  //        dirpage = NULL;  datapageId = INVALID_PAGE
    
    if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
        return false;

    if (datapage == null) {
      if (datapageId.pid == INVALID_PAGE) {
	// rdfDB is empty to begin with
	
	try{
	  unpinPage(dirpageId, false);
	  dirpage = null;
	}
	catch (Exception e){
	//  System.err.println("Stream: Chain Error: " + e);
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
	  userqid = datapage.firstRecord();
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
    // dirpage is set to NULL at the end of Stream. Hence
    
    if (dirpage == null) {
      return false;
    }
    
    datapageQid = dirpage.nextRecord(datapageQid);
    
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
	  datapageQid = dirpage.firstRecord();
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
  
    // data page is not yet loaded: read its record from the directory page
   	try {
	  rectuple = dirpage.getRecord(datapageQid);
	}
	
	catch (Exception e) {
	  System.err.println("rdfDB: Error in Stream" + e);
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
	  System.err.println("rdfDB: Error in Stream" + e);
	}
	
     
     // - directory page is pinned
     // - datapage is pinned
     // - this->dirpageId, this->dirpage correct
     // - this->datapageId, this->datapage, this->datapageQid correct

     userqid = datapage.firstRecord();
     
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


  /** Move to the next record in a sequential Stream.
   * Also returns the QID of the (new) current record.
   */
  private boolean mvNext(QID qid) 
    throws InvalidTupleSizeException,
	   IOException
  {
    QID nextqid;
    boolean status;

    if (datapage == null)
        return false;

    	nextqid = datapage.nextRecord(qid);
	
	if( nextqid != null ){
	  nextqid.pageNo.pid = nextqid.pageNo.pid;
	  nextqid.slotNo = nextqid.slotNo;
	  return true;
	} else {
	  
	  status = nextDataPage();

	  if (status==true){
	    qid.pageNo.pid = nextqid.pageNo.pid;
	    qid.slotNo = nextqid.slotNo;
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
      throw new HFBufMgrException(e,"Stream.java: pinPage() failed");
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
      throw new HFBufMgrException(e,"Stream.java: unpinPage() failed");
    }

  } // end of unpinPage


}
