package sndml.agent;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;

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
import sndml.util.ResourceException;

/**
 * {@link AppSession} that is used to communicate with a scoped app in the instance,
 * and is NOT used to read data records.
 */
public class AppSession extends Session {
	
	final PropertySet propset;
	final Instance instance;
	final String agentName;
	final String appScope;
	private RecordKey agentKey;
	final Logger logger = Log.getLogger(this.getClass());
	

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

	public RecordKey getAgentKey() throws ResourceException {
		if (agentKey == null) {
			Log.setGlobalContext();
			URI uri = this.uriGetAgent();		
			ObjectNode json;
			try {
				json = httpGet(uri);
			} catch (IOException e) {
				throw new ResourceException(e);
			}		
			String sys_id = json.get("agent").asText();
			assert sys_id != null;
			assert RecordKey.isGUID(sys_id);
			agentKey = new RecordKey(sys_id);
			assert agentKey != null;
		}
		return agentKey;
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
	@SuppressWarnings("unused")
	private URI getAPI(String apiName) {
		return getAPI(apiName, null);
	}
	
	private URI getAPI(String apiName, String parameter) {
		String apiPath = "api/" + appScope + "/" + apiName;
		if (parameter != null) apiPath += "/" + parameter;
		return instance.getURI(apiPath);
	}

	public URI uriGetJobRunList() { 
		return this.getAPI("jobrunlist", agentName); 
	}	
	
	public URI uriGetAgent() { 
		return this.getAPI("agent", agentName); 
	}
	
	public URI uriGetJobRunConfig(RecordKey jobKey) {
		return this.getAPI("jobrunconfig", jobKey.toString());
	}
	
	public URI uriPutJobRunStatus(RecordKey jobKey) {
		return this.getAPI("jobrunstatus", jobKey.toString());
	}
	
	public URI uriGetTableSchema(String tablename) {
		return this.getAPI("tableschema", tablename);
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
