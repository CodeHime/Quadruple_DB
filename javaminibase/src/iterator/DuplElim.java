package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import quadrupleheap.Quadruple;

import java.lang.*;
import java.io.*;

/**
 *Eleminate the duplicate tuples from the input relation
 */
public class DuplElim extends IteratorQ
{
  private AttrType[] _in;     // memory for array allocated by constructor
  private short       in_len;
  private short[]    str_lens;
  
  private IteratorQ _am;
  private boolean      done;
  
  private AttrType  sortFldType;
  private int       sortFldLen;
  private Quadruple    Jtuple;
  
  private Quadruple TempTuple1, TempTuple2;
  
  /**
   *Constructor to set up some information.
   *@param in[]  Array containing field types of R.
   *@param len_in # of columns in R.
   *@param s_sizes[] store the length of string appeared in tuple
   *@param am input relation iterator, access method for left input to join,
   *@param amt_of_mem the page numbers required IN PAGES
   *@exception IOException some I/O fault
   *@exception DuplElimException the exception from DuplElim.java
   */
  public DuplElim(
		  IteratorQ am,          
		  int       amt_of_mem,  
		  boolean     inp_sorted
		  )throws IOException ,DuplElimException
    {
      
      Jtuple =  new Quadruple();
      
	
     
      
      _am = am;
      QuadrupleOrder order = new QuadrupleOrder(QuadrupleOrder.Ascending);
      if (!inp_sorted)
	{
	  try {
	    _am = new Sort(am, 1, order, amt_of_mem);
	  }catch(SortException e){
	    e.printStackTrace();
	    throw new DuplElimException(e, "SortException is caught by DuplElim.java");
	  }
	}

      // Allocate memory for the temporary tuples
      TempTuple1 =  new Quadruple();
      TempTuple2 = new Quadruple();
}

  /**
   * The tuple is returned.
   *@return call this function to get the tuple
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class    
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception TupleUtilsException exception from using tuple utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions
   */
  public Quadruple get_next() 
    throws IOException,
	   JoinsException ,
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
	   Exception
    {
      Quadruple t;
      
      if (done)
        return null;
      Jtuple.quadrupleCopy(TempTuple1);
     
      do {
	if ((t = _am.get_next()) == null) {
	  done = true;                    // next call returns DONE;
	  return null;
	} 
	TempTuple2.quadrupleCopy(t);
      } while (QuadrupleUtils.Equal(TempTuple1, TempTuple2));
      
      // Now copy the the TempTuple2 (new o/p tuple) into TempTuple1.
      TempTuple1.quadrupleCopy(TempTuple2);
      Jtuple.quadrupleCopy(TempTuple2);
      return Jtuple ;
    }
 
  /**
   * implement the abstract method close() from super class Iterator
   *to finish cleaning up
   *@exception JoinsException join error from lower layers
   */
  public void close() throws JoinsException
    {
      if (!closeFlag) {
	
	try {
	  _am.close();
	}catch (Exception e) {
	  throw new JoinsException(e, "DuplElim.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }  
}
