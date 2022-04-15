package basicpattern;

/** 
 * Enumeration class for TupleOrder
 * 
 */

public class BasicPatternOrder {

  public static final int Ascending  = 0;
  public static final int Descending = 1;
  public static final int Random     = 2;

  public int basicPatternOrder;

  /** 
   * TupleOrder Constructor
   * <br>
   * A tuple ordering can be defined as 
   * <ul>
   * <li>   TupleOrder tupleOrder = new TupleOrder(TupleOrder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (tupleOrder.tupleOrder == TupleOrder.Random) ....
   * </ul>
   *
   * @param _tupleOrder The possible ordering of the tuples 
   */

  public BasicPatternOrder (int _tupleOrder) {
    basicPatternOrder = _tupleOrder;
  }

  public String toString() {
    
    switch (basicPatternOrder) {
    case Ascending:
      return "Ascending";
    case Descending:
      return "Descending";
    case Random:
      return "Random";
    }
    return ("Unexpected TupleOrder " + basicPatternOrder);
  }

}
