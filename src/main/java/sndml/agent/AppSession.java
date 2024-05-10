package sndml.agent;

import java.net.URI;

import sndml.servicenow.Instance;
import sndml.servicenow.Session;
import sndml.util.PropertySet;

/**
 * {@link Session} that is used to communicate with a scoped app in the instance,
 * and is NOT used to read data records.
 */
public class AppSession extends Session {
	
	final PropertySet propset;
	final Instance instance;
	final String agentName;
	final String appScope;

	public AppSession(PropertySet propset) {
		super(propset);
		this.propset = propset;
		this.instance = new Instance(propset);
		this.agentName = propset.getNotEmpty("agent");
		this.appScope = propset.getNotEmpty("scope");
	}

	/**
	 * Return the URI of an API. This will be dependent on the application scope
	 * which is available from the property daemon.scope.
	 */
	public URI getAPI(String apiName) {
		return getAPI(apiName, null);
	}
	
	public URI getAPI(String apiName, String parameter) {
		String apiPath = "api/" + appScope + "/" + apiName;
		if (parameter != null) apiPath += "/" + parameter;
		return instance.getURI(apiPath);		
	}
	
	
}
