package iterator;

import heap.*;
import labelheap.HFBufMgrException;
import labelheap.HFDiskMgrException;
import labelheap.HFException;
import labelheap.InvalidLabelSizeException;
import labelheap.InvalidSlotNumberException;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;
import global.*;
import java.io.*;
import diskmgr.rdfDB;

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
 * @throws Exception
 * @throws HFBufMgrException
 * @throws HFDiskMgrException
 * @throws HFException
 * @throws InvalidLabelSizeException
 * @throws InvalidSlotNumberException
   */
  public static int compareQuadrupleWithQuadruple(Quadruple q1, Quadruple q2, int quadrupleFldNo) throws InvalidSlotNumberException, InvalidLabelSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
    {   
        LabelHeapfile labelHeapfile;
        switch (quadrupleFldNo) {
            case 1:
                // compare subject value
                labelHeapfile = rdfDB.getInstance().getEntityHeapFile();
                LID subjectLID1 = q1.getSubjectQid().returnLID();
                String subject1 = labelHeapfile.getLabel(subjectLID1).getLabel();

                LID subjectLID2 = q2.getSubjectQid().returnLID();
                String subject2 = labelHeapfile.getLabel(subjectLID2).getLabel();

                return subject1.compareTo(subject2);
            case 2:
                // compare predicate value
                labelHeapfile = rdfDB.getInstance().getPredicateHeapFile();
                LID predicateLID1 = q1.getPredicateID().returnLID();
                String predicate1 = labelHeapfile.getLabel(predicateLID1).getLabel();

                LID predicateLID2 = q2.getPredicateID().returnLID();
                String predicate2 = labelHeapfile.getLabel(predicateLID2).getLabel();

                return predicate1.compareTo(predicate2);
            case 3:
                // compare object value
                labelHeapfile = rdfDB.getInstance().getEntityHeapFile();
                LID objectLID1 = q1.getObjectQid().returnLID();
                String object1 = labelHeapfile.getLabel(objectLID1).getLabel();

                LID objectLID2 = q1.getObjectQid().returnLID();
                String object2 = labelHeapfile.getLabel(objectLID2).getLabel();

                return object1.compareTo(object2);
            case 4:
                // compare confidence value
                return Double.compare(q1.getConfidence(), q2.getConfidence());
            default:
            // invalid field no throw exception
                throw new Exception("QuadrupleUtils invalid quadrupleFldNo = " +  quadrupleFldNo);
        }
    }
  
  public static boolean Equal(Quadruple q1, Quadruple q2) throws InvalidSlotNumberException, InvalidLabelSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
    {
        LabelHeapfile labelHeapfile;
        labelHeapfile = rdfDB.getInstance().getEntityHeapFile();
        // compare subject value
        LID subjectLID1 = q1.getSubjectQid().returnLID();
        String subject1 = labelHeapfile.getLabel(subjectLID1).getLabel();

        LID subjectLID2 = q2.getSubjectQid().returnLID();
        String subject2 = labelHeapfile.getLabel(subjectLID2).getLabel();

        if (!subject1.equals(subject2)) {
            return false;
        }
            
        // compare predicate value
        labelHeapfile = rdfDB.getInstance().getPredicateHeapFile();
        LID predicateLID1 = q1.getPredicateID().returnLID();
        String predicate1 = labelHeapfile.getLabel(predicateLID1).getLabel();

        LID predicateLID2 = q2.getPredicateID().returnLID();
        String predicate2 = labelHeapfile.getLabel(predicateLID2).getLabel();

        if (!predicate1.equals(predicate2)) {
            return false;
        }

        // compare object value
        labelHeapfile = rdfDB.getInstance().getEntityHeapFile();
        LID objectLID1 = q1.getObjectQid().returnLID();
        String object1 = labelHeapfile.getLabel(objectLID1).getLabel();

        LID objectLID2 = q1.getObjectQid().returnLID();
        String object2 = labelHeapfile.getLabel(objectLID2).getLabel();

        if (!object1.equals(object2)) {
            return false;
        }

        if (q1.getConfidence() != q2.getConfidence()) {
            return false;
        }

        return true;
    }
}




