package sndml.loader;

import sndml.servicenow.Session;
import sndml.util.PropertySet;
import sndml.util.ResourceException;

/**
 * {@link Session} that is used to read data records from the instance, 
 * and is NOT used to communicate with a scoped app.
 *
 */
public class ReaderSession extends Session {

	public ReaderSession(PropertySet propset) throws ResourceException {
		super(propset);
		verifySession(propset);
	}
	
}
