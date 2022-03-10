/* File Quadruple.java */

package quadrupleheap;

import java.io.*;
import java.nio.ByteBuffer;
import java.lang.*;
import global.EID;
import global.PID;


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
  private int quadruple_offset;

  /**
   * length of this quadruple
   */
  private int quadruple_length;

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
       quadruple_offset = 0;
       quadruple_length = 28; // (4 + 4) + (4 + 4) + (4 + 4) + 4
  }
   
   /** Constructor
    * @param aQuadruple a byte array which contains the quadruple
    * @param offset the offset of the quadruple in the byte array
    */

   public Quadruple(byte [] aQuadruple, int offset)
   {
      data = aQuadruple;
      quadruple_offset = offset;
   }

   /** Constructor(used as quadruple copy)
    * @param fromQuadruple  a byte array which contains the quadruple
    * 
    */
   public Quadruple(Quadruple fromQuadruple)
   {
       data = fromQuadruple.getQuadrupleByteArray();
       quadruple_offset = 0;

       this.subject = new EID();
       this.subject.copyEid(fromQuadruple.getSubjectqid());

       this.predicate = new PID();
       this.subject.copyPid(fromQuadruple.getPredicateID());

       this.object = new EID();
       this.subject.copyEid(fromQuadruple.getObjectqid());

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
    this.subject = subjectqid;
    return this;
  }

  Quadruple setPredicateID(PID predicateID) {
    this.predicate = predicateID;
    return this;
  }

  Quadruple setObjectQid(EID objectQid) {
    this.object = objectqid;
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
       System.arraycopy(tempArray, 0, data, quadruple_offset);
       quadruple_offset = 0;

       this.subject.copyEid(fromQuadruple.getSubjectqid());
       this.subject.copyPid(fromQuadruple.getPredicateID());
       this.subject.copyEid(fromQuadruple.getObjectqid());
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
       quadruple_offset = offset;
       ByteBuffer tempBuffer = ByteBuffer.wrap(aQuadruple, offset, this.getLength());

       this.subject = new EID(new LID(tempBuffer.getInt(), tempBuffer.getInt());
       this.predicate = new PID(tempBuffer.getInt(), tempBuffer.getInt());
       this.object = new EID(tempBuffer.getInt(), tempBuffer.getInt());
       this.confidence = tempBuffer.getFloat();
   }

 /**
  * Set a tuple with the given quadruple length and offset
  * @param	record	a byte array contains the tuple
  * @param	offset  the offset of the quadruple ( =0 by default)
  */
 public void quadrupleSet(byte [] fromQuadruple, int offset)
  {
      System.arraycopy(fromQuadruple, offset, data, 0);
      quadruple_offset = 0;
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
        return quadruple_length;
    }

   /** get the offset of a quadruple
    *  @return offset of the quadruple in byte array
    */   
   public int getOffset()
   {
      return quadruple_offset;
   }   
   
   /** Copy the quadruple byte array out
    *  @return  byte[], a byte array contains the tuple
    *		the length of byte[] = length of the tuple
    */
    
   public byte [] getQuadrupleByteArray() 
   {
       byte [] quadrupleCopy = new byte [quadruple_length];
       System.arraycopy(data, quadruple_offset, quadrupleCopy, 0, quadruple_length);
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
 public void print(AttrType type[]) throws IOException 
 {
  System.out.println(this.subject.toString());
  System.out.println(this.predicate.toString());
  System.out.println(this.object.toString());
  System.out.println(this.confidence.toString());
 }

  /**
   * private method
   * Padding must be used when storing different types.
   * 
   * @param	offset
   * @param type   the type of tuple
   * @return short typle
   */

  private short pad(short offset, AttrType type)
   {
      return 0;
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
