package iterator;


import labelheap.*;
import global.*;
import java.io.*;
import java.lang.*;

/**
 *some useful method when processing Label 
 */
public class LabelUtils
{
  
  /**
   * This function compares a label with another label in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the label is greater,
   *   -1        if the label is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    l1        one label.
   *@param    l2        another label.
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@return   0        if the two are equal,
   *          1        if the label is greater,
   *         -1        if the label is smaller,                              
   */
  public static int CompareLabelWithLabel(int labelType,
                                          Label  l1,
                                          Label  l2)
    throws IOException,UnknowAttrType
  {
    int l1_i,  l2_i;
    String l1_s, l2_s;
    
    switch (labelType)
    {
      case 0:                         // Compare label length.
        l1_i = l1.getLength();
        l2_i = l2.getLength();

        if (l1_i == l2_i) return  0;
        if (l1_i <  l2_i) return -1;
        if (l1_i >  l2_i) return  1;
      case 1:                         // Compare label offset.
        l1_i = l1.getOffset();
        l2_i = l2.getOffset();

        if (l1_i == l2_i) return  0;
        if (l1_i <  l2_i) return -1;
        if (l1_i >  l2_i) return  1;
      case 2:                         // Compare label strings.
        l1_s = l1.getLabel();
        l2_s = l2.getLabel();
        
        // Now handle the special case that is posed by the max_values for strings...
        if(l1_s.compareTo(l2_s)>0)return 1;
        if (l1_s.compareTo(l2_s)<0)return -1;
        return 0;
      default: 
        throw new UnknowAttrType(null, "Label type not 0, 1, or 2");
    }
  }
  
  /**
   *This function Compares two Label in all fields 
   * @param l1 the first label
   * @param l2 the second label
   * @param type[] the field types
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   */
  public static boolean Equal(Label l1,
                              Label l2,
                              int types[],
                              int len)
    throws IOException,UnknowAttrType
  {
    int i;
    
    for (i = 1; i <= len; i++)
      if (CompareLabelWithLabel(types[i-1], l1, l2) != 0)
        return false;
    
    return true;
  }
  
  /**
   *get the string specified by the field number
   *@param label the label 
   *@param fidno the field number
   *@return the content of the field number
   *@exception IOException some I/O fault
   *@exception LabelUtilsException exception from this class
   */
  public static String Value(Label label,
                             int labelType)
    throws IOException,UnknowAttrType
  {
    int l_i;
    String l_s, str;

    switch (labelType)
    {
      case 0:                         // Get label length.
        l_i = label.getLength();
        str = Integer.toString(l_i);
        return str;
      case 1:                         // Get label offset.
        l_i = label.getOffset();
        str = Integer.toString(l_i);
        return str;
      case 2:                         // Get label strings.
        l_s = label.getLabel();
        return l_s;
      default: 
        throw new UnknowAttrType(null, "Label type not 0, 1, or 2");
    }
  }
  
  /**
   *set up a label in specified field from a label
   *@param value the label to be set 
   *@param label the given label
   *@exception IOException some I/O fault
   */  
  public static void SetValue(Label value,
                              Label  label)
    throws IOException,InvalidUpdateException
  {
    if (value.getLength() == label.getLength())
      value.setLabel(label.getLabel());
    else throw new InvalidUpdateException(null, "invalid label update. Lengths do not match");

  }
}




