/* File Quadruple.java */

package quadrupleheap;

import java.io.*;
import java.nio.ByteBuffer;

import global.AttrType;
import global.Convert;
import global.EID;
import global.PID;
import global.PageId;
import labelheap.FieldNumberOutOfBoundException;
import global.LID;
import global.GlobalConst;



public class Quadruple implements GlobalConst {

 /** 
  * Maximum size of any quadruple
  */
  public static final int max_size = MINIBASE_PAGESIZE;

  /**
   */
  private EID subject;

  private PID predicate;

  private EID object;

  private double confidence;

 /** 
   * a byte array to hold data
   */
  private byte [] data;

  /**
   * start position of this tuple in data[]
   */
  private int quadrupleOffset;

  /**
   * length of this quadruple
   */
  private int quadrupleLength;

  /** 
   * private field
   * Number of fields in this tuple
   * Quadruple will have fixed fields
   */
  private short fldCnt = 4;

  /** 
   * private field
   * Array of offsets of the fields
   */
 
  private short [] fldOffset;

    /**
    * Class constructor
    * Creat a new Quadruple with length = max_size, quadruple offset = 0.
    */

  public Quadruple()
  {
       // Creat a new tuple
       data = new byte[max_size];
       quadrupleOffset = 0;
       quadrupleLength = 28; // (4 + 4) + (4 + 4) + (4 + 4) + 4
  }
   
   /** Constructor
    * @param aQuadruple a byte array which contains the quadruple
    * @param offset the offset of the quadruple in the byte array
    */

   public Quadruple(byte [] aQuadruple, int offset)
   {
      data = aQuadruple;
      quadrupleOffset = offset;
   }

   /** Constructor(used as quadruple copy)
    * @param fromQuadruple  a byte array which contains the quadruple
    * 
    */
   public Quadruple(Quadruple fromQuadruple)
   {
       data = fromQuadruple.getQuadrupleByteArray();
       quadrupleOffset = 0;

       this.subject = new EID();
       this.subject.copyEid(fromQuadruple.getSubjectQid());

       this.predicate = new PID();
       this.predicate.copyPid(fromQuadruple.getPredicateID());

       this.object = new EID();
       this.subject.copyEid(fromQuadruple.getObjectQid());

       this.confidence = fromQuadruple.getConfidence();

       fldCnt = fromQuadruple.noOfFlds(); 
       fldOffset = fromQuadruple.copyFldOffset(); 
   }
   
  /**
    return subject
   */
   EID getSubjectQid() {
     return this.subject;
   }

  /**
    return predicate
   */
   PID getPredicateID() {
     return this.predicate;
   }
  
  /**
    return object
   */
   EID getObjectQid() {
     return this.object;
   }

   double getConfidence() {
     return this.confidence;
   }

  Quadruple setSubjectQid(EID subjectQid) {
    this.subject = subjectQid;
    return this;
  }

  Quadruple setPredicateID(PID predicateID) {
    this.predicate = predicateID;
    return this;
  }

  Quadruple setObjectQid(EID objectQid) {
    this.object = objectQid;
    return this;
  }

  Quadruple setConfidence(double confidence) {
    this.confidence = confidence;
    return this;
  }

  /** return the data byte array 
    *  @return  data byte array 		
    */
    
  public byte [] returnQuadrupleByteArray()
  {
      return data;
  }

   /** Copy a quadruple to the current quadruple position
    *  you must make sure the quadruple lengths must be equal
    * @param fromQuadruple the quadruple being copied
    */
   public void quadrupleCopy(Quadruple fromQuadruple)
   {
       byte [] tempArray = fromQuadruple.getQuadrupleByteArray();
       System.arraycopy(tempArray, 0, data, quadrupleOffset, this.getLength());
       quadrupleOffset = 0;

       this.subject.copyEid(fromQuadruple.getSubjectQid());
       this.predicate.copyPid(fromQuadruple.getPredicateID());
       this.subject.copyEid(fromQuadruple.getObjectQid());
       this.confidence = fromQuadruple.getConfidence();

       fldCnt = fromQuadruple.noOfFlds();
       fldOffset = fromQuadruple.copyFldOffset();
   }

   /** This is used when you don't want to use the constructor
    * @param aQuadruple  a byte array which contains the quadruple
    * @param offset the offset of the quadruple in the byte array
    */

   public void quadrupleInit(byte [] aQuadruple, int offset)
   {
       data = aQuadruple;
       quadrupleOffset = offset;
       ByteBuffer tempBuffer = ByteBuffer.wrap(aQuadruple, offset, this.getLength());

       this.subject = new EID(new LID(new PageId(tempBuffer.getInt()), tempBuffer.getInt()));
       this.predicate = new PID(new LID(new PageId(tempBuffer.getInt()), tempBuffer.getInt()));
       this.object = new EID(new LID(new PageId(tempBuffer.getInt()), tempBuffer.getInt()));
       this.confidence = tempBuffer.getFloat();
   }

 /**
  * Set a tuple with the given quadruple length and offset
  * @param	record	a byte array contains the tuple
  * @param	offset  the offset of the quadruple ( =0 by default)
  */
 public void quadrupleSet(byte [] fromQuadruple, int offset)
  {
      System.arraycopy(fromQuadruple, offset, data, 0, this.getLength());
      quadrupleOffset = 0;
  }
  
/** get the length of a quadruple, call this method if you did 
  *  call setHdr () before
  * @return     size of this quadruple in bytes
  */
  public short size()
   {
        return (4 + 4) + (4 + 4) + (4 + 4) + 4;
   }

    public int getLength()
    {
        return quadrupleLength;
    }

   /** get the offset of a quadruple
    *  @return offset of the quadruple in byte array
    */   
   public int getOffset()
   {
      return quadrupleOffset;
   }   
   
   /** Copy the quadruple byte array out
    *  @return  byte[], a byte array contains the tuple
    *		the length of byte[] = length of the tuple
    */
    
   public byte [] getQuadrupleByteArray() 
   {
       byte [] quadrupleCopy = new byte [quadrupleLength];
       System.arraycopy(data, quadrupleOffset, quadrupleCopy, 0, quadrupleLength);
       return quadrupleCopy;
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
     throw new FieldNumberOutOfBoundException (null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");
  }

 /**
  * Print out the tuple
  * @param type  the types in the tuple
  * @Exception IOException I/O exception
  */
 public void print(AttrType[] type)
 {
    System.out.println(this.subject.toString());
    System.out.println(this.predicate.toString());
    System.out.println(this.object.toString());
    System.out.println(this.confidence);
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
}
