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
       data = new byte[QUADRUPLE_SIZE];
       quadrupleOffset = 0;
       quadrupleLength = QUADRUPLE_SIZE; // (4 + 4) + (4 + 4) + (4 + 4) + 8
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
       data = fromQuadruple.returnQuadrupleByteArray();
       quadrupleOffset = 0;

       this.subject = new EID();
       this.subject.copyEid(fromQuadruple.getSubjectQid());

       this.predicate = new PID();
       this.predicate.copyPid(fromQuadruple.getPredicateID());

       this.object = new EID();
       this.subject.copyEid(fromQuadruple.getObjectQid());

       this.confidence = fromQuadruple.getConfidence();

      //  fldCnt = fromQuadruple.noOfFlds(); 
      //  fldOffset = fromQuadruple.copyFldOffset(); 
   }
   
  /**
    return subject
   */
   public EID getSubjectQid() {
    
    int pageNo = 0;
    int slotNo = 0;
    try {
      pageNo = Convert.getIntValue(0, data);
      slotNo = Convert.getIntValue(4, data);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    EID eid  = new EID(new LID(new PageId(pageNo), slotNo));
    return eid;
   }

  /**
    return predicate
   */
   public PID getPredicateID() {
    int pageNo = 0;
    int slotNo = 0;
    try {
      pageNo = Convert.getIntValue(8, data);
      slotNo = Convert.getIntValue(12, data);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    PID eid  = new PID(new LID(new PageId(pageNo), slotNo));
    return eid;
   }
  
  /**
    return object
   */
   public EID getObjectQid() {
    int pageNo = 0;
    int slotNo = 0;
    try {
      pageNo = Convert.getIntValue(16, data);
      slotNo = Convert.getIntValue(20, data);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    EID eid  = new EID(new LID(new PageId(pageNo), slotNo));
    return eid;
   }

   public double getConfidence() {
    double conf =0;
    try {
      conf = Convert.getDoubleValue(24, data);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return conf;
   }

  public Quadruple setSubjectQid(EID subjectQid) {
    
    try {
      Convert.setIntValue(subjectQid.pageNo.pid, 0, data);
      Convert.setIntValue(subjectQid.slotNo, 4, data);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return this;
  }

  public Quadruple setPredicateID(PID predicateID) {
    try {
      Convert.setIntValue(predicateID.pageNo.pid, 8, data);
      Convert.setIntValue(predicateID.slotNo, 12, data);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return this;
  }

  public Quadruple setObjectQid(EID objectQid) {
    try {
      Convert.setIntValue(objectQid.pageNo.pid, 16, data);
      Convert.setIntValue(objectQid.slotNo, 20, data);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return this;
  }

  public Quadruple setConfidence(double confidence) {
    try {
      Convert.setDoubleValue(confidence, 24, data);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
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
      byte [] tempArray = fromQuadruple.returnQuadrupleByteArray();
      System.arraycopy(tempArray, 0, data, quadrupleOffset, this.getLength());
      // quadrupleOffset = 0;

      // this.subject.copyEid(fromQuadruple.getSubjectQid());
      // this.predicate.copyPid(fromQuadruple.getPredicateID());
      // this.subject.copyEid(fromQuadruple.getObjectQid());
      // this.confidence = fromQuadruple.getConfidence();

      // fldCnt = fromQuadruple.noOfFlds();
      // fldOffset = fromQuadruple.copyFldOffset();
  }

  /** This is used when you don't want to use the constructor
  * @param aQuadruple  a byte array which contains the quadruple
  * @param offset the offset of the quadruple in the byte array
  */

  public void quadrupleInit(byte [] aQuadruple, int offset)
  {
      data = aQuadruple;
      quadrupleOffset = offset;
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
        return (4 + 4) + (4 + 4) + (4 + 4) + (4 + 4);
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
