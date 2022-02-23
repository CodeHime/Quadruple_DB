/* File hferr.java  */

package labelheap;
import chainexception.*;

public class HFDiskMgrException extends ChainException{


  public HFDiskMgrException()
  {
     super();
  
  }

  public HFDiskMgrException(Exception ex, String name)
  {
    super(ex, name);
  }



}
