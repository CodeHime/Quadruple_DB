/* File Tuple.java */

package labelheap;

import java.io.*;
import java.lang.*;
import global.*;


public class Label implements GlobalConst{


  /** 
  * Maximum size of any label
  */
  public static final int max_size = MINIBASE_PAGESIZE;

  /** 
  * a byte array to hold the label
  */
  private byte[] label;

  /**
   * start position of this label in label[]
   */
  private int label_offset;

  /**
  * length of this label
  */
  private int label_length;

  /**
  * Class constructor
  * Creat a new label with length = max_size,label offset = 0.
  */
  public  Label()
  {
       // Creat a new label
       label = new byte[max_size];
       label_offset = 0;
       label_length = max_size;
  }
   
  /** Constructor
  * @param alabel a string array which contains the label
  * @param length the length of the tuple
  */
  public Label(byte[] alabel, int offset, int length)
  {
    label = alabel;
    label_offset = offset;
    label_length = length;
  }
   
  /** Constructor(used as tuple copy)
  * @param fromLabel   a String array which contains the label
  * 
  */
  public Label(Label fromLabel)
  {
    String fromLabelVal = fromLabel.getLabel();
    label = fromLabelVal.getBytes();
    label_offset = 0;
    label_length = fromLabel.getLength();
  }

  /**  
  * Class constructor
  * Creat a new label with length = size,tuple offset = 0.
  */
  public  Label(int size)
  {
    // Creat a new label
    label = new byte[size];
    label_offset = 0;
    label_length = size;     
  }

  /** Copy a tuple to the current tuple position
    *  you must make sure the tuple lengths must be equal
    * @param fromTuple the tuple being copied
    */
  public void labelCopy(Label fromLabel)
  {
    byte[] fromLabelVal = fromLabel.getLabelByteArray();
    int fromLabel_offset = fromLabel.getOffset();
    System.arraycopy(fromLabelVal, fromLabel_offset, label, label_offset, label_length);
  }

  /** This is used when you don't want to use the constructor
  * @param alabel  a String array which contains the label
  * @param offset the offset of the label in the byte array
  * @param length the length of the label
  */
  public void labelInit(byte[] alabel, int offset, int length)
  {
    label = alabel;
    label_offset = offset;
    label_length = length;
  }
  
  /** get the label, call this method if you did not call 
  * setHdr () before
  * @return 	label
  */   
  public String getLabel()
  {
    //byte[] labelcopy = new byte[label_length];
    //System.arraycopy(label, label_offset, labelcopy, 0, label_length);
    //String labelStr = new String(labelcopy);
    return new String(label);//labelStr;
  }
  
  /** get the length of a label, call this method if you did not 
  *  call setHdr () before
  * @return 	length of this tuple in bytes
  */   
  public int getLength()
  {
    return label_length;
  }

  /**
  * Set this field to String value
  *
  * @param     val     the string value
  * @exception   IOException I/O errors
  */

  public Label setLabel(String val) 
	  throws IOException
  {
    byte[] valByte = val.getBytes();
    label_length = valByte.length;
    System.arraycopy(valByte, 0, label, label_offset, label_length);
    return this;
  }

  /**
  * Print out the label
  * @param type  the types in the tuple
  * @Exception IOException I/O exception
  */
  public void print()
  {
    System.out.println(new String(label));
  }

  /** Get the label as a byte array
  * @return	label as a byte array
  */
  public byte[] getLabelByteArray() 
  {
    byte[] labelCopy = new byte[label_length];
    System.arraycopy(label, label_offset, labelCopy, 0, label_length);
    return labelCopy;
  }

  /** Return the label as a byte array
   * @return label
   */
  public byte [] returnLabelByteArray()
  {
      return label;
  }

  /** Get the offset of the label
  * @return 	offset of the label
  */
  public int getOffset()
   {
      return label_offset;
   } 
}

