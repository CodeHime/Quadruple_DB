package global;

/** 
<<<<<<< HEAD
 * Enumeration class for TupleOrder
=======
 * Enumeration class for QuadrupleOrder
>>>>>>> Branching out Stream and modifying as per logic
 * 
 */

public class QuadrupleOrder {

<<<<<<< HEAD
  public static final int Ascending  = 0;
  public static final int Descending = 1;
  public static final int Random     = 2;

  public int quadrupleOrder;

  /** 
   * TupleOrder Constructor
   * <br>
   * A tuple ordering can be defined as 
   * <ul>
   * <li>   TupleOrder quadrupleOrder = new TupleOrder(TupleOrder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (quadrupleOrder.quadrupleOrder == TupleOrder.Random) ....
   * </ul>
   *
   * @param quadrupleOrder The possible ordering of the tuples 
   */

  public QuadrupleOrder (int quadrupleOrder) {
    quadrupleOrder = quadrupleOrder;
=======
  public static final int Random     = 0;
  public static final int SubjectPredicateObjectConfidence = 1;
  public static final int PredicateSubjectObjectConfidence = 2;
  public static final int SubjectConfidence = 3;
  public static final int PredicateConfidence = 4;
  public static final int ObjectConfidence = 5;
  public static final int Confidence = 6;
  
  public int _quadupleOrder;

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
   * @param _quadupleOrder The possible ordering of the tuples 
   */

  public TupleOrder (int _quadupleOrder) {
    quadupleOrder = _quadupleOrder;
>>>>>>> Branching out Stream and modifying as per logic
  }

  public String toString() {
    
<<<<<<< HEAD
    switch (quadrupleOrder) {
    case Ascending:
      return "Ascending";
    case Descending:
      return "Descending";
    case Random:
      return "Random";
    }
    return ("Unexpected TupleOrder " + quadrupleOrder);
=======
    switch (tupleOrder) {
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
    return ("Unexpected TupleOrder " + tupleOrder);
>>>>>>> Branching out Stream and modifying as per logic
  }

}
