/* File Quadruple.java */

package quadrupleheap;

import java.lang.instrument.Instrumentation;
import java.io.*;
import java.lang.*;
import global.*;


public class Quadruple implements GlobalConst {


 /** 
  * Maximum size of any tuple
  */
  public static final int max_size = MINIBASE_PAGESIZE;

  /**
   */
  private EID subject;

  private PID predicate;

  private EID object;

  private AttrType value = AttrType.attrReal;

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
   * private field
   * Number of fields in this tuple
   */
  private short fldCnt;

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
       quadruple_length = max_size;
  }
   
   /** Constructor
    * @param aquadruple a byte array which contains the quadruple
    * @param offset the offset of the quadruple in the byte array
    */

   public Quadruple(byte [] aquadruple, int offset)
   {
      data = atuple;
      quadruple_offset = offset;
   }
   
   /** Constructor(used as quadruple copy)
    * @param fromQuadruple  a byte array which contains the quadruple
    * 
    */
   public Quadruple(Quadruple fromQuadruple)
   {
       data = fromQuadruple.getTupleByteArray();
       quadruple_offset = 0;
       fldCnt = fromQuadruple.noOfFlds(); 
       fldOffset = fromQuadruple.copyFldOffset(); 
   }
   
  /**
    return subject
   */
   EID getSubjecqid() {
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
   PID getObjecqid() {
     return this.object;
   }

   double getConfidence() {
     return this.confidence;
   }

  Quadruple setSubjecqid(EID subjecqid) {
    this.subject = subjecqid;
    return this;
  }

  Quadruple setPredicateID(EID predicateID) {
    this.predicate = predicateID;
    return this;
  }

  Quadruple setobjecqid(EID objecqid) {
    this.object = objecqid;
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
   public void quadrupleCopy(Tuple fromQuadruple)
   {
       byte [] temparray = fromQuadruple.getTupleByteArray();
       System.arraycopy(temparray, 0, data, quadruple_offset);   
//       fldCnt = fromTuple.noOfFlds(); 
//       fldOffset = fromTuple.copyFldOffset(); 
   }

   /** This is used when you don't want to use the constructor
    * @param aquadruple  a byte array which contains the quadruple
    * @param offset the offset of the quadruple in the byte array
    */

   public void quadrupleInit(byte [] aquadruple, int offset)
   {
      data = aquadruple;
      quadruple_offset = offset;      
   }

 /**
  * Set a tuple with the given quadruple length and offset
  * @param	record	a byte array contains the tuple
  * @param	offset  the offset of the quadruple ( =0 by default)
  */
 public void quadrupleSet(byte [] fromquadruple, int offset)  
  {
      System.arraycopy(fromquadruple, offset, data, 0);
      quadruple_offset = 0;
  }
  
/** get the length of a quadruple, call this method if you did 
  *  call setHdr () before
  * @return     size of this quadruple in bytes
  */
  public short size()
   {
      return 100; // just random number now
      // TODO: Implement object get size from individual object types and perform object.getSize() 
      // instrumentation.getObjectSize(this.subject) + 
      // instrumentation.getObjectSize(this.predicate) + 
      // instrumentation.getObjectSize(this.object) + 
      // instrumentation.getObjectSize(this.confidence);
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
       byte [] quadruplecopy = new byte [quadruple_length];
       System.arraycopy(data, quadruple_offset, quadruplecopy, 0, quadruple_length);
       return quadruplecopy;
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
}
