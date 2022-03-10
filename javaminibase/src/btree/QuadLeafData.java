package btree;
import global.*;

/**  IndexData: It extends the DataClass.
 *   It defines the data "rid" for leaf node in B++ tree.
 */
public class QuadLeafData extends DataClass {
  private QID myQid;

  public String toString() {
     String s;
     s="[ "+ (new Integer(myQid.pageNo.pid)).toString() +" "
              + (new Integer(myQid.slotNo)).toString() + " ]";
     return s;
  }

  /** Class constructor
   *  @param    qid  the data rid
   */
  QuadLeafData(QID qid) {myQid= new QID(qid.pageNo, qid.slotNo);};  

  /** get a copy of the rid
  *  @return the reference of the copy 
  */
  public QID getData() {return new QID(myQid.pageNo, myQid.slotNo);};

  /** set the rid
   */ 
  public void setData(QID qid) { myQid= new QID(qid.pageNo, qid.slotNo);};
}   
