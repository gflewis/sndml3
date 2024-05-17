package sndml.loader;

import sndml.servicenow.Session;
import sndml.util.PropertySet;

/**
 * {@link Session} that is used to read data records from the instance, 
 * and is NOT used to communicate with a scoped app.
 *
 */
public class ReaderSession extends Session {

	public ReaderSession(PropertySet propset) {
		super(propset);
	}
	
}
