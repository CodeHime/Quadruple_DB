package basicpattern;

import heap.*;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.RelSpec;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import labelheap.HFBufMgrException;
import labelheap.HFDiskMgrException;
import labelheap.HFException;
import labelheap.LabelHeapfile;
import global.*;
import java.io.*;
import java.lang.*;

import diskmgr.rdfDB;

/**
 * some useful method when processing Tuple
 */
public class BasicPatternUtils {

  /**
   * This function compares tuple1 with another tuple2 whose
   * field number is same as the tuple1
   *
   * @param fldType   the type of the field being compared.
   * @param t1        one tuple
   * @param value     another tuple.
   * @param t1_fld_no the field numbers in the tuples to be compared.
   * @return 0 if the two are equal,
   *         1 if the tuple is greater,
   *         -1 if the tuple is smaller,
   * @throws TupleUtilsException
   * @throws UnknowAttrType
   * @throws IOException
   * @throws Exception
   * @throws HFBufMgrException
   * @throws HFDiskMgrException
   * @throws HFException
   * @throws InvalidTupleSizeException
   * @throws InvalidSlotNumberException
   */
  public static int CompareTupleWithValue(AttrType fldType,
      Tuple t1, int t1_fld_no,
      Tuple t2) throws TupleUtilsException, UnknowAttrType, IOException{
    
    switch (fldType.attrType) {
      case AttrType.attrInteger: // Compare two integers.
        try {

          if (t1.getIntFld(t1_fld_no)==Integer.MAX_VALUE){
            return 1;
          }
          else if (t1.getIntFld(t1_fld_no)==Integer.MIN_VALUE)
            return -1;
          else if (t2.getIntFld(t1_fld_no)==Integer.MAX_VALUE){
              return -1;
            }
          else if (t2.getIntFld(t1_fld_no)==Integer.MIN_VALUE)
              return 1;

          LabelHeapfile labelHeapfile = rdfDB.getInstance().getEntityHeapFile();
          
          String s1 = labelHeapfile.getLabel(new LID(new PageId(t1.getIntFld(t1_fld_no)),t1.getIntFld(t1_fld_no+1))).getLabel();
          String s2 = labelHeapfile.getLabel(new LID(new PageId(t2.getIntFld(t1_fld_no)),t2.getIntFld(t1_fld_no+1))).getLabel();

          return s1.compareTo(s2);
        } catch (Exception e) {
          throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
      case AttrType.attrD: // Compare two integers.
        try {
          double t1_i = t1.getDFld(t1_fld_no);
          double t2_i = t2.getDFld(t1_fld_no);
          if (t1_i == t2_i)
            return 0;
          if (t1_i < t2_i)
            return -1;
          if (t1_i > t2_i)
            return 1;
        } catch (FieldNumberOutOfBoundException e) {
          throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }

      default:

        throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

    }
  }


  /**
   * set up a tuple in specified field from a tuple
   * 
   * @param value   the tuple to be set
   * @param tuple   the given tuple
   * @param fld_no  the field number
   * @param fldType the tuple attr type
   * @exception UnknowAttrType      don't know the attribute type
   * @exception IOException         some I/O fault
   * @exception TupleUtilsException exception from this class
   */
  public static void SetValue(Tuple value, Tuple tuple, int fld_no, AttrType fldType)
      throws IOException,
      UnknowAttrType,
      TupleUtilsException {

    switch (fldType.attrType) {
      case AttrType.attrInteger:
        try {
          value.setIntFld(fld_no, tuple.getIntFld(fld_no));
          value.setIntFld(fld_no + 1, tuple.getIntFld(fld_no + 1));
        } catch (FieldNumberOutOfBoundException e) {
          throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      case AttrType.attrD:
        try {
          value.setDFld(fld_no, tuple.getDFld(fld_no));
        } catch (FieldNumberOutOfBoundException e) {
          throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      case AttrType.attrReal:
        try {
          value.setFloFld(fld_no, tuple.getFloFld(fld_no));
        } catch (FieldNumberOutOfBoundException e) {
          throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      case AttrType.attrString:
        try {
          value.setStrFld(fld_no, tuple.getStrFld(fld_no));
        } catch (FieldNumberOutOfBoundException e) {
          throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      default:
        throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

    }

    return;
  }

  /**
   * set up the Jtuple's attrtype, string size,field number for using join
   * 
   * @param Jtuple       reference to an actual tuple - no memory has been
   *                     malloced
   * @param res_attrs    attributes type of result tuple
   * @param in1          array of the attributes of the tuple (ok)
   * @param len_in1      num of attributes of in1
   * @param in2          array of the attributes of the tuple (ok)
   * @param len_in2      num of attributes of in2
   * @param t1_str_sizes shows the length of the string fields in S
   * @param t2_str_sizes shows the length of the string fields in R
   * @param proj_list    shows what input fields go where in the output tuple
   * @param nOutFlds     number of outer relation fileds
   * @exception IOException         some I/O fault
   * @exception TupleUtilsException exception from this class
   */
  public static short[] setup_op_tuple(Tuple Jtuple, AttrType[] res_attrs,
      AttrType in1[], int len_in1, AttrType in2[],
      int len_in2, short t1_str_sizes[],
      short t2_str_sizes[],
      FldSpec proj_list[], int nOutFlds)
      throws IOException,
      TupleUtilsException {
    short[] sizesT1 = new short[len_in1];
    short[] sizesT2 = new short[len_in2];
    int i, count = 0;

    for (i = 0; i < len_in1; i++)
      if (in1[i].attrType == AttrType.attrString)
        sizesT1[i] = t1_str_sizes[count++];

    for (count = 0, i = 0; i < len_in2; i++)
      if (in2[i].attrType == AttrType.attrString)
        sizesT2[i] = t2_str_sizes[count++];

    int n_strs = 0;
    for (i = 0; i < nOutFlds; i++) {
      if (proj_list[i].relation.key == RelSpec.outer)
        res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);
      else if (proj_list[i].relation.key == RelSpec.innerRel)
        res_attrs[i] = new AttrType(in2[proj_list[i].offset - 1].attrType);
    }

    // Now construct the res_str_sizes array.
    for (i = 0; i < nOutFlds; i++) {
      if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
        n_strs++;
      else if (proj_list[i].relation.key == RelSpec.innerRel
          && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
        n_strs++;
    }

    short[] res_str_sizes = new short[n_strs];
    count = 0;
    for (i = 0; i < nOutFlds; i++) {
      if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
        res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
      else if (proj_list[i].relation.key == RelSpec.innerRel
          && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
        res_str_sizes[count++] = sizesT2[proj_list[i].offset - 1];
    }
    try {
      Jtuple.setHdr((short) nOutFlds, res_attrs, res_str_sizes);
    } catch (Exception e) {
      throw new TupleUtilsException(e, "setHdr() failed");
    }
    return res_str_sizes;
  }

  /**
   * set up the Jtuple's attrtype, string size,field number for using project
   * 
   * @param Jtuple       reference to an actual tuple - no memory has been
   *                     malloced
   * @param res_attrs    attributes type of result tuple
   * @param in1          array of the attributes of the tuple (ok)
   * @param len_in1      num of attributes of in1
   * @param t1_str_sizes shows the length of the string fields in S
   * @param proj_list    shows what input fields go where in the output tuple
   * @param nOutFlds     number of outer relation fileds
   * @exception IOException         some I/O fault
   * @exception TupleUtilsException exception from this class
   * @exception InvalidRelation     invalid relation
   */

  public static short[] setup_op_tuple(Tuple Jtuple, AttrType res_attrs[],
      AttrType in1[], int len_in1,
      short t1_str_sizes[],
      FldSpec proj_list[], int nOutFlds)
      throws IOException,
      TupleUtilsException,
      InvalidRelation {
    short[] sizesT1 = new short[len_in1];
    int i, count = 0;

    for (i = 0; i < len_in1; i++)
      if (in1[i].attrType == AttrType.attrString)
        sizesT1[i] = t1_str_sizes[count++];

    int n_strs = 0;
    for (i = 0; i < nOutFlds; i++) {
      if (proj_list[i].relation.key == RelSpec.outer)
        res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);

      else
        throw new InvalidRelation("Invalid relation -innerRel");
    }

    // Now construct the res_str_sizes array.
    for (i = 0; i < nOutFlds; i++) {
      if (proj_list[i].relation.key == RelSpec.outer
          && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
        n_strs++;
    }

    short[] res_str_sizes = new short[n_strs];
    count = 0;
    for (i = 0; i < nOutFlds; i++) {
      if (proj_list[i].relation.key == RelSpec.outer
          && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
        res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
    }

    try {
      Jtuple.setHdr((short) nOutFlds, res_attrs, res_str_sizes);
    } catch (Exception e) {
      throw new TupleUtilsException(e, "setHdr() failed");
    }
    return res_str_sizes;
  }
}
