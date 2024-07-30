package sndml.agent;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigParseException;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.RecordKey;
import sndml.util.Log;

public class GetRunListRequest extends JsonRequest {

	final String agentName;
	final AppSession appSession;
	
	public GetRunListRequest(AppSession appSession, String agentName) {
		super(appSession, appSession.uriGetJobRunList(), HttpMethod.GET, null);
		this.appSession = appSession;
		this.agentName = agentName;		
	}
	
	public RecordKey getAgentKey() throws IOException {
		if (!this.executed) this.execute();
		if (logger.isDebugEnabled()) logger.debug(Log.RESPONSE, responseObj.toPrettyString());
		String agent_sysid = this.getResult().get("agent").asText();
		assert agent_sysid != null;
		assert agent_sysid.length() == 32;
		RecordKey result = new RecordKey(agent_sysid);
		return result;		
	}
		
	public ArrayNode getRunList() throws IOException, ConfigParseException {
		ArrayNode runlist = null;
		Log.setJobContext(agentName);
		if (!this.executed) this.execute();
		if (logger.isDebugEnabled()) logger.debug(Log.RESPONSE, responseObj.toPrettyString());
		ObjectNode objResult = this.getResult();
		if (objResult.has("runs")) {
			runlist = (ArrayNode) objResult.get("runs");
		}
		if (runlist == null || runlist.size() == 0) {
			logger.debug(Log.INIT, "Nothing ready");			
		}
		else {
			logger.debug(Log.INIT, "Runlist=" + getNumbers(runlist));			
		}
		return runlist;		
	}
	
	private static String getNumbers(ArrayNode runlist) {		
		ArrayList<String> numbers = new ArrayList<String>();
		for (JsonNode node : runlist) {
			assert node.isObject();
			ObjectNode obj = (ObjectNode) node;
			numbers.add(obj.get("number").asText());
		}
		return String.join(",", numbers);		
	}

}
