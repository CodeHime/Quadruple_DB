package global;

/** 
 * Enumeration class for QuadrupleOrder
 * 
 */

public class QuadrupleOrder {

  public static final int Random     = 0;
  public static final int SubjectPredicateObjectConfidence = 1;
  public static final int PredicateSubjectObjectConfidence = 2;
  public static final int SubjectConfidence = 3;
  public static final int PredicateConfidence = 4;
  public static final int ObjectConfidence = 5;
  public static final int Confidence = 6;

  public static final int Ascending  = 0;
  public static final int Descending = 1;

  
  public int _quadruplesortorder;
  public int _quadrupleOrder;

  /** 
   * QuadrupleOrder Constructor
   * <br>
   * A tuple ordering can be defined as 
   * <ul>
   * <li>   QuadrupleOrder quadupleOrder = new QuadrupleOrder(QuadrupleOrder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (quadupleOrder.quadupleOrder == QuadrupleOrder.Random) ....
   * </ul>
   *
   * @param _quadruplesortorder The possible ordering of the tuples 
   */

  public QuadrupleOrder (int _quadruplesortorder, int _quadrupleOrder) {

    this._quadruplesortorder = _quadruplesortorder;
    this._quadrupleOrder = _quadrupleOrder;
  }

  public String toString() {
    switch (this._quadruplesortorder) {
    case SubjectPredicateObjectConfidence:
      return "SubjectPredicateObjectConfidence";
    case PredicateSubjectObjectConfidence:
      return "PredicateSubjectObjectConfidence";
    case SubjectConfidence:
      return "SubjectConfidence";
    case PredicateConfidence:
      return "PredicateConfidence";
    case ObjectConfidence:
      return "ObjectConfidence";
    case Confidence:
      return "Confidence";
    case Random:
      return "Random";
    }

    return ("Unexpected TupleOrder " + this._quadruplesortorder);
  }

}
