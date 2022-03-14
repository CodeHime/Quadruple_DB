package global;

/** 
 * Enumeration class for TupleOrder
 * 
 */

public class QuadrupleOrder {

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
  }

  public String toString() {
    
    switch (quadrupleOrder) {
    case Ascending:
      return "Ascending";
    case Descending:
      return "Descending";
    case Random:
      return "Random";
    }
    return ("Unexpected TupleOrder " + quadrupleOrder);
  }

}
