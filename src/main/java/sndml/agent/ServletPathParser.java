package sndml.agent;

import sndml.servicenow.RecordKey;

public class ServletPathParser {
	
	final String action;
	final String parm;
	final RecordKey key;

	public ServletPathParser(String path) throws AgentURLException {
		String[] parts = path.split("/");
		if (parts.length < 2) throw new AgentURLException(path);
		this.action = parts.length > 1 ? parts[1] : null;
		this.parm = parts.length > 2 ? parts[2] : null;
		if (action != "startjobrun") throw new AgentURLException(path);
		key = new RecordKey(parm);
		if (!key.isGUID()) throw new AgentURLException(path);		
	}
	
	public RecordKey getSysID() {
		return this.key;
	}	

}
