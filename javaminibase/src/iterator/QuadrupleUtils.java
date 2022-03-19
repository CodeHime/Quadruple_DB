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
 * some useful method when processing Tuple
 */
public class QuadrupleUtils {

    /**
     * This function compares a tuple with another tuple in respective field, and
     * returns:
     *
     * 0 if the two are equal,
     * 1 if the tuple is greater,
     * -1 if the tuple is smaller,
     *
     * @param fldType   the type of the field being compared.
     * @param t1        one tuple.
     * @param t2        another tuple.
     * @param t1_fld_no the field numbers in the tuples to be compared.
     * @param t2_fld_no the field numbers in the tuples to be compared.
     * @exception UnknowAttrType      don't know the attribute type
     * @exception IOException         some I/O fault
     * @exception TupleUtilsException exception from this class
     * @return 0 if the two are equal,
     *         1 if the tuple is greater,
     *         -1 if the tuple is smaller,
     * @throws Exception
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws HFException
     * @throws InvalidLabelSizeException
     * @throws InvalidSlotNumberException
     */
    public static int compareQuadrupleWithQuadruple(Quadruple q1, Quadruple q2, int quadrupleFldNo[], int index)
            throws InvalidSlotNumberException, InvalidLabelSizeException, HFException, HFDiskMgrException,
            HFBufMgrException, Exception {
        int comapred = -1;
        LabelHeapfile labelHeapfile;
        switch (quadrupleFldNo[index]) {
            case 1:
                // compare subject value
                if (q1.getSubjectQid().slotNo == -1 || q2.getSubjectQid().slotNo == -2) {
                    return 1;
                } else if (q1.getSubjectQid().slotNo == -2 || q2.getSubjectQid().slotNo == -1) {
                    return -1;
                }
                labelHeapfile = rdfDB.getInstance().getEntityHeapFile();
                LID subjectLID1 = q1.getSubjectQid().returnLID();
                String subject1 = labelHeapfile.getLabel(subjectLID1).getLabel();

                LID subjectLID2 = q2.getSubjectQid().returnLID();
                String subject2 = labelHeapfile.getLabel(subjectLID2).getLabel();

                comapred = subject1.compareTo(subject2);
                break;
            case 2:
                // compare predicate value
                if (q1.getPredicateID().slotNo == -1 || q2.getPredicateID().slotNo == -2) {
                    return 1;
                } else if (q1.getPredicateID().slotNo == -2 || q2.getPredicateID().slotNo == -1) {
                    return -1;
                }
                labelHeapfile = rdfDB.getInstance().getPredicateHeapFile();
                LID predicateLID1 = q1.getPredicateID().returnLID();
                String predicate1 = labelHeapfile.getLabel(predicateLID1).getLabel();

                LID predicateLID2 = q2.getPredicateID().returnLID();
                String predicate2 = labelHeapfile.getLabel(predicateLID2).getLabel();

                comapred = predicate1.compareTo(predicate2);
                break;
            case 3:
                // compare object value
                if (q1.getObjectQid().slotNo == -1 || q2.getObjectQid().slotNo == -2) {
                    return 1;
                } else if (q1.getObjectQid().slotNo == -2 || q2.getObjectQid().slotNo == -1) {
                    return -1;
                }

                labelHeapfile = rdfDB.getInstance().getEntityHeapFile();

                LID objectLID1 = q1.getObjectQid().returnLID();
                String object1 = labelHeapfile.getLabel(objectLID1).getLabel();

                LID objectLID2 = q1.getObjectQid().returnLID();
                String object2 = labelHeapfile.getLabel(objectLID2).getLabel();

                comapred = object1.compareTo(object2);
                break;
            case 4:
                // compare confidence value
                comapred = Double.compare(q1.getConfidence(), q2.getConfidence());
                break;
            default:
                // invalid field no throw exception
                throw new Exception("QuadrupleUtils invalid quadrupleFldNo = " + quadrupleFldNo);
                

            
        }
        if (comapred == 0 && index < 3 && quadrupleFldNo[index + 1] != -1) {
            return compareQuadrupleWithQuadruple(q1, q2, quadrupleFldNo, index + 1);
        } else {
            return comapred;
        }
    }

    public static boolean Equal(Quadruple q1, Quadruple q2) throws InvalidSlotNumberException,
            InvalidLabelSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
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

    public static void SetValue(Quadruple q1, Quadruple q2) {

        q1.setSubjectQid(q2.getSubjectQid());

        q1.setPredicateID(q2.getPredicateID());

        q1.setObjectQid(q2.getObjectQid());

        q1.setConfidence(q2.getConfidence());

    }

    public static void setValue(Quadruple q1, boolean max) {

        if (max) {
            EID e = new EID();
            e.pageNo = new PageId(0);
            e.slotNo = -1;

            q1.setSubjectQid(e);
            PID p = new PID();
            p.pageNo = new PageId(0);
            p.slotNo = -1;
            q1.setPredicateID(p);
            q1.setObjectQid(e);

            q1.setConfidence(1.0);

        } else {

            EID e = new EID();
            e.pageNo = new PageId(0);
            e.slotNo = -2;

            q1.setSubjectQid(e);

            PID p = new PID();
            p.pageNo = new PageId(0);
            p.slotNo = -2;
            q1.setPredicateID(p);

            q1.setObjectQid(e);

            q1.setConfidence(0.0);

        }
    }
}
