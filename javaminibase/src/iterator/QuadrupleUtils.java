package iterator;

import heap.*;
import quadrupleheap.Quadruple;
import global.*;
import java.io.*;

/**
 *some useful method when processing Tuple 
 */
public class QuadrupleUtils
{
  
  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple.
   *@param    t2        another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.
   *@param    t2_fld_no the field numbers in the tuples to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,                              
   */
  public static int compareQuadrupleWithQuadruple(Quadruple q1, Quadruple q2)
    {   
        return 0;
    }
  
  public static boolean Equal(Quadruple q1, Quadruple q2)
    {
        if (q1.getSubjecqid().equals(q2.getSubjecqid()) &&
            q1.getPredicateID().equals(q2.getPredicateID()) &&
            q1.getObjecqid().equals(q2.getObjecqid()) &&
            q1.getConfidence() == q2.getConfidence()
        ) {
            return true;
        }

        return false;
    }
}




