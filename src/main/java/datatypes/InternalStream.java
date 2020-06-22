package datatypes;
import datatypes.internals.*;

import java.io.Serializable;


/**  */

public abstract class InternalStream implements Serializable {

    public abstract String getStreamID();

    public String unionKey() { return "0"; }

}
