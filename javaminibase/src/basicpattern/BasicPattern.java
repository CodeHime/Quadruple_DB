/* File BasicPattern.java */

package basicpattern;

import java.io.*;

import diskmgr.rdfDB;
import heap.*;
import labelheap.*;
import labelheap.FieldNumberOutOfBoundException;
import labelheap.InvalidTypeException;
import quadrupleheap.Quadruple;
import global.*;


public class BasicPattern implements GlobalConst{


 /** 
  * Maximum size of any tuple
  */
  public static final int max_size = MINIBASE_PAGESIZE;

 /** 
   * a byte array to hold data
   */
  private byte [] data;

  /**
   * start position of this tuple in data[]
   */
  private int bp_offset;

  /**
   * length of this tuple
   */
  private int bp_length;

  /** 
   * private field
   * Number of fields in this tuple
   */
  private short fldCnt;

  /** 
   * private field
   * Array of offsets of the fields
   */
 
  private short [] fldOffset; 
  int confidence_fld_num = 1;

   /**
    * Class constructor
    * Creat a new tuple with length = max_size,tuple offset = 0.
    */

  public  BasicPattern()
  {
       // Creat a new tuple
       data = new byte[max_size];
       bp_offset = 0;
       bp_length = max_size;
  }
   
   /** Constructor
    * @param aBP a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

   public BasicPattern(byte [] aBP, int offset, int length)
   {
      data = aBP;
      bp_offset = offset;
      bp_length = length;
    //  fldCnt = getShortValue(offset, data);
   }
   
   /** Constructor(used as bp copy)
    * @param fromBP   The source bp
    * 
    */
   public BasicPattern(BasicPattern fromBP)
   {
       data = fromBP.returnBasicPatternArray();
       bp_length = fromBP.getLength();
       bp_offset = fromBP.getOffset();
       fldCnt = fromBP.noOfFlds(); 
       fldOffset = fromBP.copyFldOffset(); 
   }

  /** Constructor(used as Quadruple convert)
  * @param quad   the source Quadruple
  * 
  */
  public BasicPattern(Quadruple quad)
  {
    data = new byte[max_size];
    bp_offset = 0;
    bp_length = max_size;

    try
    {
      // set header to have 3 fields
      setHdr((short)3);
      
      //set first field to be the confidence of the quadruple
      setDoubleFld(1, quad.getConfidence());
      
      // set the EID fields for subject and object
      setEIDFld(2, quad.getSubjectQid());
      setEIDFld(3, quad.getObjectQid());

    }
    catch(Exception e){
      System.out.println(e);
      e.printStackTrace();
    }
  }

  /**  
  * Class constructor
  * Creat a new tuple with length = size,tuple offset = 0.
  */
 
  public  BasicPattern(int size)
  {
       // Creat a new tuple
       data = new byte[size];
       bp_offset = 0;
       bp_length = size;     
  }

   /** Copy a tuple to the current tuple position
    *  you must make sure the tuple lengths must be equal
    * @param fromBP the tuple being copied
    */
   public void basicPatternCopy(BasicPattern fromBP)
   {
      byte [] temparray = fromBP.getBasicPatternByteArray();
      System.arraycopy(temparray, 0, data, bp_offset, bp_length);   
      fldCnt = fromBP.noOfFlds(); 
      fldOffset = fromBP.copyFldOffset(); 
   }

   /** This is used when you don't want to use the constructor
    * @param aBP  a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

   public void basicPatternInit(byte [] aBP, int offset, int length)
   {
      data = aBP;
      bp_offset = offset;
      bp_length = length;
   }

 /**
  * Set a tuple with the given tuple length and offset
  * @param	pattern	a byte array contains the tuple
  * @param	offset  the offset of the tuple ( =0 by default)
  * @param	length	the length of the tuple
  */
 public void basicPatternSet(byte [] pattern, int offset, int length)  
  {
      System.arraycopy(pattern, offset, data, 0, length);
      bp_offset = 0;
      bp_length = length;
  }
  
 /** get the length of a tuple, call this method if you did not 
  *  call setHdr () before
  * @return 	length of this tuple in bytes
  */   
  public int getLength()
   {
      return bp_length;
   }

/** get the length of a tuple, call this method if you did 
  *  call setHdr () before
  * @return     size of this tuple in bytes
  */
  public short size()
   {
      return ((short) (fldOffset[fldCnt] - bp_offset));
   }
 
   /** get the offset of a tuple
    *  @return offset of the tuple in byte array
    */   
   public int getOffset()
   {
      return bp_offset;
   }   
   
   /** Copy the tuple byte array out
    *  @return  byte[], a byte array contains the tuple
    *		the length of byte[] = length of the tuple
    */
    
   public byte [] getBasicPatternByteArray() 
   {
       byte [] tuplecopy = new byte [bp_length];
       System.arraycopy(data, bp_offset, tuplecopy, 0, bp_length);
       return tuplecopy;
   }
   
   /** return the data byte array 
    *  @return  data byte array 		
    */
    
   public byte [] returnBasicPatternArray()
   {
       return data;
   }
   
   /**
    * Convert this field into integer 
    * 
    * @param	fldNo	the field number
    * @return		the converted integer if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
    */

  public int getIntFld(int fldNo) 
  	throws IOException, FieldNumberOutOfBoundException
  {           
    int val;
    if ( (fldNo > 0) && (fldNo <= fldCnt))
     {
      val = Convert.getIntValue(fldOffset[fldNo -1], data);
      return val;
     }
    else 
     throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
  }
    
  /**
    * Convert this field into a double
    *
    * @param    fldNo   the field number
    * @return           the converted double number  if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
    */

    public double getDoubleFld(int fldNo) 
    	throws IOException, FieldNumberOutOfBoundException
     {
	    double val;
      if ( (fldNo > 0) && (fldNo <= fldCnt))
       {
        val = Convert.getDoubleValue(fldNo*8, data);
        return val;
       }
      else 
       throw new FieldNumberOutOfBoundException (null, "BasicPattern:BP_FLDNO_OUT_OF_BOUND");
     }

   /**
    * Convert this field in to float
    *
    * @param    fldNo   the field number
    * @return           the converted float number  if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
    */

    public float getFloFld(int fldNo) 
    	throws IOException, FieldNumberOutOfBoundException
     {
	float val;
      if ( (fldNo > 0) && (fldNo <= fldCnt))
       {
        val = Convert.getFloValue(fldOffset[fldNo -1], data);
        return val;
       }
      else 
       throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
     }


   /**
    * Convert this field into String
    *
    * @param    fldNo   the field number
    * @return           the converted string if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
    */

   public String getStrFld(int fldNo) 
   	throws IOException, FieldNumberOutOfBoundException 
   { 
         String val;
    if ( (fldNo > 0) && (fldNo <= fldCnt))      
     {
        val = Convert.getStrValue(fldOffset[fldNo -1], data, 
		fldOffset[fldNo] - fldOffset[fldNo -1]); //strlen+2
        return val;
     }
    else 
     throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
  }
 
   /**
    * Convert this field into a character
    *
    * @param    fldNo   the field number
    * @return           the character if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
    */

   public char getCharFld(int fldNo) 
   	throws IOException, FieldNumberOutOfBoundException 
    {   
       char val;
      if ( (fldNo > 0) && (fldNo <= fldCnt))      
       {
        val = Convert.getCharValue(fldOffset[fldNo -1], data);
        return val;
       }
      else 
       throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
 
    }

     /**
    * Convert this field into a EID
    *
    * @param    fldNo   the field number
    * @return           the EID if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException BP field number out of bound
    */

    public EID getEIDFld(int fldNo)
    throws IOException, FieldNumberOutOfBoundException 
    {
      EID val;
      if ( (fldNo > 0) && (fldNo <= fldCnt))      
      {
        int pageNo = Convert.getIntValue(fldNo*8, data);
        int slotNo = Convert.getIntValue(fldNo*8 + 4, data);

       val = new EID(new LID(new PageId(pageNo), slotNo));
       return val;
      }
     else 
      throw new FieldNumberOutOfBoundException (null, "BasicPattern:BP_FLDNO_OUT_OF_BOUND");

   }


  /**
   * Set this field to integer value
   *
   * @param	fldNo	the field number
   * @param	val	the integer value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
   */

  public BasicPattern setIntFld(int fldNo, int val) 
  	throws IOException, FieldNumberOutOfBoundException
  { 
    if ( (fldNo > 0) && (fldNo <= fldCnt))
     {
	Convert.setIntValue (val, fldOffset[fldNo -1], data);
	return this;
     }
    else 
     throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND"); 
  }

  /**
   * Set this field to double value
   *
   * @param     fldNo   the field number
   * @param     val     the double value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
   */

  public BasicPattern setDoubleFld(int fldNo, double val) 
  	throws IOException, FieldNumberOutOfBoundException
  { 
   if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
     Convert.setDoubleValue(val, fldOffset[fldNo -1], data);
     return this;
    }
    else  
     throw new FieldNumberOutOfBoundException (null, "BasicPattern:BP_FLDNO_OUT_OF_BOUND"); 
     
  }

  /**
   * Set this field to float value
   *
   * @param     fldNo   the field number
   * @param     val     the float value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
   */

  public BasicPattern setFloFld(int fldNo, float val) 
  	throws IOException, FieldNumberOutOfBoundException
  { 
   if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
     Convert.setFloValue (val, fldOffset[fldNo -1], data);
     return this;
    }
    else  
     throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND"); 
     
  }

  /**
   * Set this field to String value
   *
   * @param     fldNo   the field number
   * @param     val     the string value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
   */

   public BasicPattern setStrFld(int fldNo, String val) 
		throws IOException, FieldNumberOutOfBoundException  
   {
     if ( (fldNo > 0) && (fldNo <= fldCnt))        
      {
         Convert.setStrValue (val, fldOffset[fldNo -1], data);
         return this;
      }
     else 
       throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

  /**
   * Set this field to String value
   *
   * @param     fldNo   the field number
   * @param     val     the string value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
   */

   public BasicPattern setEIDFld(int fldNo, EID val) 
   throws IOException, FieldNumberOutOfBoundException  
  {
    if ( fldNo == fldCnt + 1)
    {
       // We need to expand the bp size and update all instance variables.
       try{
          // update the header to have one more field. This will update fldCnt and FldOffset.

          //TODO: Jack: Would this cause any memory errors internaly withour raising errors?
          setHdr((short)(this.noOfFlds() + 1));
 
        }
        catch(Exception e){
          System.out.print("ERROR: Cannot expand Basic Pattern size\n" + e);
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }
    }
    if ( (fldNo > 0) && (fldNo <= fldCnt))        
    {
      Convert.setIntValue (val.pageNo.pid, fldOffset[fldNo -1], data);
      Convert.setIntValue (val.slotNo, fldOffset[fldNo -1] + 4, data);
      return this;
    }

    else 
      throw new FieldNumberOutOfBoundException (null, "BasicPattern:BP_FLDNO_OUT_OF_BOUND");
   }


   /**
    * setHdr will set the header of this BasicPattern.   
    *
    * @param	numFlds	  number of fields
    *				
    * @exception IOException I/O errors
    * @exception InvalidTypeException Invalid tupe type
    * @exception InvalidTupleSizeException Tuple size too big
    *
    */

public void setHdr (short numFlds)
 throws IOException, InvalidTypeException, InvalidTupleSizeException		
{
  if((numFlds +2)*2 > max_size)
    throw new InvalidTupleSizeException (null, "BasicPattern: BP_TOOBIG_ERROR");
  
  fldCnt = numFlds;
  Convert.setShortValue(numFlds, bp_offset, data);
  fldOffset = new short[numFlds+1];
  int pos = bp_offset+2;  // start position for fldOffset[]
  fldOffset[0] = (short) ((numFlds +2) * 2 + bp_offset);   
   
  Convert.setShortValue(fldOffset[0], pos, data);
  pos +=2;
  short incr = 8;
  int i;


  for (i=1; i<=numFlds; i++)
  {
    fldOffset[i]  = (short) (fldOffset[i-1] + incr);
    Convert.setShortValue(fldOffset[i], pos, data);
    pos +=2;
  }
  
  bp_length = fldOffset[numFlds] - bp_offset;

  if(bp_length > max_size)
    throw new InvalidTupleSizeException (null, "BasicPattern: BP_TOOBIG_ERROR");
   
}
     
  /**
   * Returns number of fields in this tuple
   *
   * @return the number of fields in this tuple
   *
   */

  public short noOfFlds() 
   {
     return fldCnt;
   }

  /**
   * Makes a copy of the fldOffset array
   *
   * @return a copy of the fldOffset arrray
   *
   */

  public short[] copyFldOffset() 
   {
     short[] newFldOffset = new short[fldCnt + 1];
     for (int i=0; i<=fldCnt; i++) {
       newFldOffset[i] = fldOffset[i];
     }
     
     return newFldOffset;
   }

 /**
  * Print out the bp
  * @param type  the types in the bp
  * @Exception IOException I/O exception
  */
 public void print(AttrType type[])
    throws IOException 
 {
  int i;
  LabelHeapfile entity_heap_file = rdfDB.getInstance().getEntityHeapFile();
  
  try
  {
    System.out.print("[");
    
    System.out.print(getDoubleFld(1) + " ");
    i = 2;

    while (i <= fldCnt)
    {
      Label nodeID = entity_heap_file.getLabel(this.getEIDFld(i).returnLID());
      String nodeString = nodeID.getLabel();
      System.out.print(nodeString + " ");
      i++;
    }

    System.out.println("]");

  } catch(Exception e) {
    e.printStackTrace();
  } 

 }
}

