
package basicpattern;
import global.*;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.*;

/**
 * Implements a sorted binary tree (extends class pnodePQ).
 * Implements the <code>enq</code> and the <code>deq</code> functions.
 */
public class BasicPatternPNodeSplayPQ extends BasicPatternPNodePQ
{

  /** the root of the tree */
  protected BasicPatternPNodeSplayNode   root;
  /*
  pnodeSplayNode*   leftmost();
  pnodeSplayNode*   rightmost();
  pnodeSplayNode*   pred(pnodeSplayNode* t);
  pnodeSplayNode*   succ(pnodeSplayNode* t);
  void            _kill(pnodeSplayNode* t);
  pnodeSplayNode*   _copy(pnodeSplayNode* t);
  */

  /**
   * class constructor, sets default values.
   */
  public BasicPatternPNodeSplayPQ() 
  {
    root = null;
    count = 0;
    fld_no = 0;
    fld_type = new AttrType(AttrType.attrInteger);

    sort_order = new BPOrder(1);
  }

  /**
   * class constructor.
   * @param fldNo   the field number for sorting
   * @param fldType the type of the field for sorting
   * @param order   the order of sorting (Ascending or Descending)
   */  
  public BasicPatternPNodeSplayPQ(int fldNo, AttrType at, BPOrder order)
  {
    root = null;
    count = 0;
    fld_type = at;
    fld_no   = fldNo;
    sort_order = order;
  }

  /**
   * Inserts an element into the binary tree.
   * @param item the element to be inserted
   * @exception IOException from lower layers
   * @exception UnknowAttrType <code>attrSymbol</code> or 
   *                           <code>attrNull</code> encountered
   * @exception TupleUtilsException error in tuple compare routines
   */
  public void enq(BasicPatternPNode item) throws IOException, UnknowAttrType, TupleUtilsException 
  {
    count ++;
    BasicPatternPNodeSplayNode newnode = new BasicPatternPNodeSplayNode(item);
    BasicPatternPNodeSplayNode t = root;

    if (t == null) {
      root = newnode;
      return;
    }
    
    int comp = pnodeCMP(item, t.item);
    
    BasicPatternPNodeSplayNode l = BasicPatternPNodeSplayNode.dummy;
    BasicPatternPNodeSplayNode r = BasicPatternPNodeSplayNode.dummy;
     
    boolean done = false;

    while (!done) {
      if ((sort_order.basicPatternOrder == QuadrupleOrder.Ascending && comp >= 0) || (sort_order.basicPatternOrder == QuadrupleOrder.Descending && comp <= 0)) {
        BasicPatternPNodeSplayNode tr = t.rt;
	if (tr == null) {
	  tr = newnode;
	  comp = 0;
	  done = true;
	}
	else comp = pnodeCMP(item, tr.item);
	
	if ((sort_order.basicPatternOrder == QuadrupleOrder.Ascending && comp <= 0) ||(sort_order.basicPatternOrder == QuadrupleOrder.Descending && comp >= 0))  {
	  l.rt = t; t.par = l;
	  l = t;
	  t = tr;
	}
	else {
        BasicPatternPNodeSplayNode trr = tr.rt;
	  if (trr == null) {
	    trr = newnode;
	    comp = 0;
	    done = true;
	  }
	  else comp = pnodeCMP(item, trr.item);
	  
	  if ((t.rt = tr.lt) != null) t.rt.par = t;
	  tr.lt = t; t.par = tr;
	  l.rt = tr; tr.par = l;
	  l = tr;
	  t = trr;
	}
      } // end of if(comp >= 0)
      else {
        BasicPatternPNodeSplayNode tl = t.lt;
	if (tl == null) {
	  tl = newnode;
	  comp = 0;
	  done = true;
	}
	else comp = pnodeCMP(item, tl.item);
	
	if ((sort_order.basicPatternOrder == QuadrupleOrder.Ascending && comp >= 0) || (sort_order.basicPatternOrder == QuadrupleOrder.Descending && comp <= 0)) {
	  r.lt = t; t.par = r;
	  r = t;
	  t = tl;
	}
	else {
        BasicPatternPNodeSplayNode tll = tl.lt;
	  if (tll == null) {
	    tll = newnode;
	    comp = 0;
	    done = true;
	  }
	  else comp = pnodeCMP(item, tll.item);
	  
	  if ((t.lt = tl.rt) != null) t.lt.par = t;
	  tl.rt = t; t.par = tl;
	  r.lt = tl; tl.par = r;
	  r = tl;
	  t = tll;
	}
      } // end of else
    } // end of while(!done)
    
    if ((r.lt = t.rt) != null) r.lt.par = r;
    if ((l.rt = t.lt) != null) l.rt.par = l;
    if ((t.lt = BasicPatternPNodeSplayNode.dummy.rt) != null) t.lt.par = t;
    if ((t.rt = BasicPatternPNodeSplayNode.dummy.lt) != null) t.rt.par = t;
    t.par = null;
    root = t;
	    
    return; 
  }
  
  /**
   * Removes the minimum (Ascending) or maximum (Descending) element.
   * @return the element removed
   */
  public BasicPatternPNode deq() 
  {
    if (root == null) return null;
    
    count --;
    BasicPatternPNodeSplayNode t = root;
    BasicPatternPNodeSplayNode l = root.lt;
    if (l == null) {
      if ((root = t.rt) != null) root.par = null;
      return t.item;
    }
    else {
      while (true) {
        BasicPatternPNodeSplayNode ll = l.lt;
	if (ll == null) {
	  if ((t.lt = l.rt) != null) t.lt.par = t;
	  return l.item;
	}
	else {
        BasicPatternPNodeSplayNode lll = ll.lt;
	  if (lll == null) {
	    if((l.lt = ll.rt) != null) l.lt.par = l;
	    return ll.item;
	  }
	  else {
	    t.lt = ll; ll.par = t;
	    if ((l.lt = ll.rt) != null) l.lt.par = l;
	    ll.rt = l; l.par = ll;
	    t = ll;
	    l = lll;
	  }
	}
      } // end of while(true)
    } 
  }
  
  /*  
                  pnodeSplayPQ(pnodeSplayPQ& a);
  virtual       ~pnodeSplayPQ();

  Pix           enq(pnode  item);
  pnode           deq(); 

  pnode&          front();
  void          del_front();

  int           contains(pnode  item);

  void          clear(); 

  Pix           first(); 
  Pix           last(); 
  void          next(Pix& i);
  void          prev(Pix& i);
  pnode&          operator () (Pix i);
  void          del(Pix i);
  Pix           seek(pnode  item);

  int           OK();                    // rep invariant
  */
}
