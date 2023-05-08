package sndml.daemon;

import sndml.datamart.ConnectionProfile;
import sndml.servicenow.RecordKey;

public class AgentJobRunner {

	final ConnectionProfile profile;
	final RecordKey jobKey;
	
	public AgentJobRunner(ConnectionProfile profile, RecordKey jobKey) {
		this.profile = profile;
		this.jobKey = jobKey;
		// TODO Auto-generated constructor stub
	}

}
