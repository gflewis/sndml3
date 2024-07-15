package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigParseException;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.Instance;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.RecordKey;
import sndml.servicenow.SchemaReader;
import sndml.servicenow.Session;
import sndml.util.Log;
import sndml.util.PropertySet;

/**
 * {@link AppSession} that is used to communicate with a scoped app in the instance,
 * and is NOT used to read data records.
 */
public class AppSession extends Session {
	
	final PropertySet propset;
	final Instance instance;
	final String agentName;
	final String appScope;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	

	public AppSession(PropertySet propset) {
		super(propset);
		this.propset = propset;
		this.instance = new Instance(propset);
		this.agentName = propset.getNotEmpty("agent");
		this.appScope = propset.getNotEmpty("scope");
	}

	public String getAgentName() {
		return agentName;
	}
	
	public Instance getAppInstance() {
		return new Instance(propset);
	}
	
	@Override
	public SchemaReader getSchemaReader() {
		if (this.schemaReader == null) {
			this.schemaReader = new AppSchemaReader(this);			
		}
		return this.schemaReader;
	}

	/**
	 * Create a new Session with the same properties as this one. 
	 * The URL and credentials will be the same, but the Session ID will be different.
	 */
	@Override
	public AppSession duplicate() throws IOException {
		return new AppSession(this.propset);
	}
	
	/**
	 * Return the URI of an API. This will be dependent on the application scope
	 * which is available from the property app.scope.
	 */
	public URI getAPI(String apiName) {
		return getAPI(apiName, null);
	}
	
	public URI getAPI(String apiName, String parameter) {
		String apiPath = "api/" + appScope + "/" + apiName;
		if (parameter != null) apiPath += "/" + parameter;
		return instance.getURI(apiPath);
	}
	
	ObjectNode httpGet(URI uri) throws IOException, ConfigParseException {
		Log.setJobContext(agentName);
		JsonRequest request = new JsonRequest(this, uri, HttpMethod.GET, null);
		logger.info(uri.toString());
		ObjectNode response = request.execute();
		logger.debug(Log.RESPONSE, response.toPrettyString());
		ObjectNode objResult = (ObjectNode) response.get("result");
		return objResult;
	}

	ObjectNode getRun(RecordKey jobKey) throws IOException, ConfigParseException {
		URI uriGetRun = this.getAPI("getrun", jobKey.toString());
		return httpGet(uriGetRun);
	}	
	
}
