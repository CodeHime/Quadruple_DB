
package basicpattern;

/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class BasicPatternPNodeSplayNode
{
  /** a reference to the element in the node */
  public BasicPatternPNode             item;

  /** the left child pointer */
  public BasicPatternPNodeSplayNode    lt;

  /** the right child pointer */
  public BasicPatternPNodeSplayNode    rt;

  /** the parent pointer */
  public BasicPatternPNodeSplayNode    par;

  /**
   * class constructor, sets all pointers to <code>null</code>.
   * @param h the element in this node
   */
  public BasicPatternPNodeSplayNode(BasicPatternPNode h) 
  {
    item = h;
    lt = null;
    rt = null;
    par = null;
  }

  /**
   * class constructor, sets all pointers.
   * @param h the element in this node
   * @param l left child pointer
   * @param r right child pointer
   */  
  public BasicPatternPNodeSplayNode(BasicPatternPNode h, BasicPatternPNodeSplayNode l, BasicPatternPNodeSplayNode r) 
  {
    item = h;
    lt = l;
    rt = r;
    par = null;
  }

  /** a static dummy node for use in some methods */
  public static BasicPatternPNodeSplayNode dummy = new BasicPatternPNodeSplayNode(null);
  
}

